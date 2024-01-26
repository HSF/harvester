import json
import time
from concurrent.futures import ThreadPoolExecutor as Pool

import six

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.pilot_errors import PilotErrors
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.worker_errors import WorkerErrors
from pandaharvester.harvestermisc.htcondor_utils import (
    CondorJobManage,
    CondorJobQuery,
    condor_job_id_from_workspec,
    get_host_batchid_map,
)
from pandaharvester.harvestermonitor.monitor_common import get_payload_errstr_from_ec

# logger
baseLogger = core_utils.setup_logger("htcondor_monitor")


# Native HTCondor job status map
CONDOR_JOB_STATUS_MAP = {
    "1": "idle",
    "2": "running",
    "3": "removed",
    "4": "completed",
    "5": "held",
    "6": "transferring_output",
    "7": "suspended",
}


# Condor jobs held with these reasons should be killed
TO_KILL_HOLD_REASONS = [
    "Job not found",
    "Failed to start GAHP",
]


# pilot error object
PILOT_ERRORS = PilotErrors()


# Check one worker
def _check_one_worker(workspec, job_ads_all_dict, cancel_unknown=False, held_timeout=3600, payload_type=None):
    # Make logger for one single worker
    tmpLog = core_utils.make_logger(baseLogger, f"workerID={workspec.workerID}", method_name="_check_one_worker")
    # Initialize newStatus
    newStatus = workspec.status
    errStr = ""
    try:
        job_ads_dict = job_ads_all_dict[condor_job_id_from_workspec(workspec)]
    except KeyError:
        got_job_ads = False
    except Exception as e:
        got_job_ads = False
        tmpLog.error(f"With error {e}")
    else:
        got_job_ads = True
    # Parse job ads
    if got_job_ads:
        # Check JobStatus
        try:
            batchStatus = str(job_ads_dict["JobStatus"])
        except KeyError:
            # Propagate native condor job status as unknown
            workspec.nativeStatus = "unknown"
            if cancel_unknown:
                newStatus = WorkSpec.ST_cancelled
                errStr = f"cannot get JobStatus of job submissionHost={workspec.submissionHost} batchID={workspec.batchID}. Regard the worker as canceled"
                tmpLog.error(errStr)
            else:
                newStatus = None
                errStr = f"cannot get JobStatus of job submissionHost={workspec.submissionHost} batchID={workspec.batchID}. Skipped"
                tmpLog.warning(errStr)
        else:
            # Try to get LastJobStatus
            lastBatchStatus = str(job_ads_dict.get("LastJobStatus", ""))
            # Set batchStatus if lastBatchStatus is terminated status
            if (lastBatchStatus in ["3", "4"] and batchStatus not in ["3", "4"]) or (lastBatchStatus in ["4"] and batchStatus in ["3"]):
                batchStatus = lastBatchStatus
                tmpLog.warning(
                    "refer to LastJobStatus={0} as new status of job submissionHost={1} batchID={2} to avoid reversal in status (Jobstatus={3})".format(
                        lastBatchStatus, workspec.submissionHost, workspec.batchID, str(job_ads_dict["JobStatus"])
                    )
                )
            # Propagate native condor job status
            workspec.nativeStatus = CONDOR_JOB_STATUS_MAP.get(batchStatus, "unexpected")
            if batchStatus in ["2", "6"]:
                # 2 running, 6 transferring output
                newStatus = WorkSpec.ST_running
            elif batchStatus in ["1", "7"]:
                # 1 idle, 7 suspended
                if job_ads_dict.get("JobStartDate"):
                    newStatus = WorkSpec.ST_idle
                else:
                    newStatus = WorkSpec.ST_submitted
            elif batchStatus in ["3"]:
                # 3 removed
                if not errStr:
                    errStr = f"Condor HoldReason: {job_ads_dict.get('LastHoldReason')} ; Condor RemoveReason: {job_ads_dict.get('RemoveReason')} "
                newStatus = WorkSpec.ST_cancelled
            elif batchStatus in ["5"]:
                # 5 held
                hold_reason = job_ads_dict.get("HoldReason")
                errStr = f"Condor HoldReason: {hold_reason} "
                if hold_reason in TO_KILL_HOLD_REASONS or int(time.time()) - int(job_ads_dict.get("EnteredCurrentStatus", 0)) > held_timeout:
                    # Kill the job if held too long or other reasons
                    if hold_reason in TO_KILL_HOLD_REASONS:
                        tmpLog.debug(f"trying to kill job submissionHost={workspec.submissionHost} batchID={workspec.batchID} due to HoldReason: {hold_reason}")
                    else:
                        tmpLog.debug(f"trying to kill job submissionHost={workspec.submissionHost} batchID={workspec.batchID} due to held too long")
                    for submissionHost, batchIDs_list in six.iteritems(get_host_batchid_map([workspec])):
                        condor_job_manage = CondorJobManage(id=workspec.submissionHost)
                        try:
                            ret_map = condor_job_manage.remove(batchIDs_list)
                        except Exception as e:
                            ret_map = {}
                            ret_err_str = f"failed to kill job. Exception {e.__class__.__name__}: {e}"
                            tmpLog.error(ret_err_str)
                        else:
                            ret = ret_map.get(condor_job_id_from_workspec(workspec))
                            if ret and ret[0]:
                                tmpLog.info(f"killed held job submissionHost={workspec.submissionHost} batchID={workspec.batchID}")
                            else:
                                tmpLog.error(f"cannot kill held job submissionHost={workspec.submissionHost} batchID={workspec.batchID}")
                    newStatus = WorkSpec.ST_cancelled
                    errStr += " ; Worker canceled by harvester due to held too long or not found"
                    # Mark the PanDA job as closed instead of failed
                    workspec.set_pilot_closed()
                    tmpLog.debug("Called workspec set_pilot_closed")
                else:
                    if job_ads_dict.get("JobStartDate"):
                        newStatus = WorkSpec.ST_idle
                    else:
                        newStatus = WorkSpec.ST_submitted
            elif batchStatus in ["4"]:
                # 4 completed
                try:
                    payloadExitCode_str = str(job_ads_dict["ExitCode"])
                    payloadExitCode = int(payloadExitCode_str)
                except KeyError:
                    errStr = f"cannot get ExitCode of job submissionHost={workspec.submissionHost} batchID={workspec.batchID}. Regard the worker as failed"
                    tmpLog.warning(errStr)
                    newStatus = WorkSpec.ST_failed
                except ValueError:
                    errStr = "got invalid ExitCode {0} of job submissionHost={1} batchID={2}. Regard the worker as failed".format(
                        payloadExitCode_str, workspec.submissionHost, workspec.batchID
                    )
                    tmpLog.warning(errStr)
                    newStatus = WorkSpec.ST_failed
                else:
                    # Propagate condor return code
                    workspec.nativeExitCode = payloadExitCode
                    if payloadExitCode == 0:
                        # Payload should return 0 after successful run
                        newStatus = WorkSpec.ST_finished
                    else:
                        # Other return codes are considered failed
                        newStatus = WorkSpec.ST_failed
                        errStr = ""
                        if payload_type:
                            errStr = get_payload_errstr_from_ec(payload_type, payloadExitCode)
                        if not errStr:
                            errStr = f"Payload execution error: returned non-zero {payloadExitCode}"
                        tmpLog.debug(errStr)
                        # Map return code to Pilot error code
                        reduced_exit_code = payloadExitCode // 256 if (payloadExitCode % 256 == 0) else payloadExitCode
                        pilot_error_code, pilot_error_diag = PILOT_ERRORS.convertToPilotErrors(reduced_exit_code)
                        if pilot_error_code is not None:
                            workspec.set_pilot_error(pilot_error_code, pilot_error_diag)
                    tmpLog.info(f"Payload return code = {payloadExitCode}")
            else:
                errStr = "cannot get reasonable JobStatus of job submissionHost={0} batchID={1}. Regard the worker as failed by default".format(
                    workspec.submissionHost, workspec.batchID
                )
                tmpLog.error(errStr)
                newStatus = WorkSpec.ST_failed
            tmpLog.info(f"submissionHost={workspec.submissionHost} batchID={workspec.batchID} : batchStatus {batchStatus} -> workerStatus {newStatus}")
    else:
        # Propagate native condor job status as unknown
        workspec.nativeStatus = "unknown"
        if cancel_unknown:
            errStr = f"condor job submissionHost={workspec.submissionHost} batchID={workspec.batchID} not found. Regard the worker as canceled by default"
            tmpLog.error(errStr)
            newStatus = WorkSpec.ST_cancelled
            tmpLog.info(f"submissionHost={workspec.submissionHost} batchID={workspec.batchID} : batchStatus 3 -> workerStatus {newStatus}")
        else:
            errStr = f"condor job submissionHost={workspec.submissionHost} batchID={workspec.batchID} not found. Skipped"
            tmpLog.warning(errStr)
            newStatus = None
    # Set supplemental error message
    error_code = WorkerErrors.error_codes.get("GENERAL_ERROR") if errStr else WorkerErrors.error_codes.get("SUCCEEDED")
    workspec.set_supplemental_error(error_code=error_code, error_diag=errStr)
    # Return
    return (newStatus, errStr)


# monitor for HTCONDOR batch system
class HTCondorMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        try:
            self.nProcesses
        except AttributeError:
            self.nProcesses = 4
        try:
            self.cancelUnknown
        except AttributeError:
            self.cancelUnknown = False
        else:
            self.cancelUnknown = bool(self.cancelUnknown)
        try:
            self.heldTimeout
        except AttributeError:
            self.heldTimeout = 3600
        try:
            self.cacheEnable = harvester_config.monitor.pluginCacheEnable
        except AttributeError:
            self.cacheEnable = False
        try:
            self.cacheRefreshInterval = harvester_config.monitor.pluginCacheRefreshInterval
        except AttributeError:
            self.cacheRefreshInterval = harvester_config.monitor.checkInterval
        try:
            self.useCondorHistory
        except AttributeError:
            self.useCondorHistory = True
        try:
            self.submissionHost_list
        except AttributeError:
            self.submissionHost_list = []
        try:
            self.condorHostConfig_list
        except AttributeError:
            self.condorHostConfig_list = []
        try:
            self.payloadType
        except AttributeError:
            self.payloadType = None

    # check workers
    def check_workers(self, workspec_list):
        # Make logger for batch job query
        tmpLog = self.make_logger(baseLogger, "batch job query", method_name="check_workers")
        tmpLog.debug("start")
        # Loop over submissionHost
        job_ads_all_dict = {}
        for submissionHost, batchIDs_list in six.iteritems(get_host_batchid_map(workspec_list)):
            # Record batch job query result to this dict, with key = batchID
            try:
                job_query = CondorJobQuery(
                    cacheEnable=self.cacheEnable, cacheRefreshInterval=self.cacheRefreshInterval, useCondorHistory=self.useCondorHistory, id=submissionHost
                )
                host_job_ads_dict = job_query.get_all(batchIDs_list=batchIDs_list)
            except Exception as e:
                host_job_ads_dict = {}
                ret_err_str = f"Exception {e.__class__.__name__}: {e}"
                tmpLog.error(ret_err_str)
            job_ads_all_dict.update(host_job_ads_dict)
        # Check for all workers
        with Pool(self.nProcesses) as _pool:
            retIterator = _pool.map(
                lambda _x: _check_one_worker(
                    _x, job_ads_all_dict, cancel_unknown=self.cancelUnknown, held_timeout=self.heldTimeout, payload_type=self.payloadType
                ),
                workspec_list,
            )
        retList = list(retIterator)
        tmpLog.debug("done")
        return True, retList

    # report updated workers info to monitor to check
    def report_updated_workers(self, time_window):
        # Make logger for batch job query
        tmpLog = self.make_logger(baseLogger, method_name="report_updated_workers")
        tmpLog.debug("start")
        # Get now timestamp
        timeNow = time.time()
        # Set of submission hosts
        submission_host_set = set()
        for submissionHost in self.submissionHost_list:
            submission_host_set.add(submissionHost)
        for condorHostConfig in self.condorHostConfig_list:
            try:
                with open(condorHostConfig, "r") as f:
                    condor_host_config_map = json.load(f)
                for _schedd, _cm in condor_host_config_map.items():
                    _pool = _cm["pool"]
                    submissionHost = f"{_schedd},{_pool}"
                    submission_host_set.add(submissionHost)
            except Exception as e:
                err_str = f"failed to parse condorHostConfig {condorHostConfig}; {e.__class__.__name__}: {e}"
                tmpLog.error(err_str)
                continue
        # Loop over submissionHost and get all jobs
        job_ads_all_dict = {}
        for submissionHost in submission_host_set:
            try:
                job_query = CondorJobQuery(
                    cacheEnable=self.cacheEnable, cacheRefreshInterval=self.cacheRefreshInterval, useCondorHistory=self.useCondorHistory, id=submissionHost
                )
                job_ads_all_dict.update(job_query.get_all(allJobs=True))
                tmpLog.debug(f"got information of condor jobs on {submissionHost}")
            except Exception as e:
                ret_err_str = f"Exception {e.__class__.__name__}: {e}"
                tmpLog.error(ret_err_str)
        # Choose workers updated within a time window
        workers_to_check_list = []
        for condor_job_id, job_ads in six.iteritems(job_ads_all_dict):
            # put in worker cache fifo, with lock mechanism
            job_EnteredCurrentStatus = job_ads.get("EnteredCurrentStatus")
            if not (job_EnteredCurrentStatus > timeNow - time_window):
                continue
            workerid = job_ads.get("harvesterWorkerID")
            if workerid is None:
                continue
            else:
                workerid = int(workerid)
            workers_to_check_list.append((workerid, job_EnteredCurrentStatus))
        tmpLog.debug(f"got {len(workers_to_check_list)} workers")
        tmpLog.debug("done")
        return workers_to_check_list
