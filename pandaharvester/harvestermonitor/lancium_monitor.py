import time

from concurrent.futures import ThreadPoolExecutor as Pool

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.worker_errors import WorkerErrors
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.pilot_errors import PilotErrors
from pandaharvester.harvestermisc.lancium_utils import get_full_batch_id_from_workspec, get_workerid_from_job_name, get_host_batch_id_map, timestamp_to_datetime
from pandaharvester.harvestermisc.lancium_utils import LanciumJobQuery


# logger
base_logger = core_utils.setup_logger("lancium_monitor")

# pilot error object
PILOT_ERRORS = PilotErrors()


# Check one worker
def _check_one_worker(workspec, job_attr_all_dict, cancel_unknown=False, held_timeout=3600):
    # Make logger for one single worker
    tmp_log = core_utils.make_logger(base_logger, "workerID={0}".format(workspec.workerID), method_name="_check_one_worker")
    # Initialize newStatus
    newStatus = workspec.status
    errStr = ""
    try:
        job_attr_dict = job_attr_all_dict[get_full_batch_id_from_workspec(workspec)]
    except KeyError:
        got_job_attr = False
    except Exception as e:
        got_job_attr = False
        tmp_log.error("With error {0}".format(e))
    else:
        got_job_attr = True
    # Parse job ads
    if got_job_attr:
        # Check
        try:
            # FIXME
            new_batch_status = job_attr_dict["status"]
        except KeyError:
            # Propagate native job status as unknown
            workspec.nativeStatus = "unknown"
            if cancel_unknown:
                newStatus = WorkSpec.ST_cancelled
                errStr = "cannot get job status of submissionHost={0} batchID={1}. Regard the worker as canceled".format(
                    workspec.submissionHost, workspec.batchID
                )
                tmp_log.error(errStr)
            else:
                newStatus = None
                errStr = "cannot get job status of submissionHost={0} batchID={1}. Skipped".format(workspec.submissionHost, workspec.batchID)
                tmp_log.warning(errStr)
        else:
            # Possible native statuses: "created" "submitted" "queued" "ready" "running" "error" "finished" "delete pending"
            last_batch_status = workspec.nativeStatus
            batchStatus = new_batch_status
            # Set batchStatus if last_batch_status is terminated status
            if (last_batch_status in ["error", "finished", "delete pending"] and new_batch_status not in ["error", "finished", "delete pending"]) or (
                last_batch_status in ["error", "finished"] and new_batch_status in ["delete pending"]
            ):
                batchStatus = last_batch_status
                tmp_log.warning(
                    "refer to last_batch_status={0} as new status of job submissionHost={1} batchID={2} to avoid reversal in status (new_batch_status={3})".format(
                        last_batch_status, workspec.submissionHost, workspec.batchID, new_batch_status
                    )
                )
            # Propagate native job status
            workspec.nativeStatus = batchStatus
            if batchStatus in ["running"]:
                # running
                newStatus = WorkSpec.ST_running
            elif batchStatus in ["created", "submitted", "queued", "ready"]:
                # pre-running
                newStatus = WorkSpec.ST_submitted
            elif batchStatus in ["error"]:
                # failed
                errStr += "job error_string: {0} ".format(job_attr_dict.get("error_string"))
                newStatus = WorkSpec.ST_failed
            elif batchStatus in ["delete pending"]:
                # cancelled
                errStr = "job error_string: {0} ".format(job_attr_dict.get("error_string"))
                newStatus = WorkSpec.ST_cancelled
                # Mark the PanDA job as closed instead of failed
                workspec.set_pilot_closed()
                tmp_log.debug("Called workspec set_pilot_closed")
            elif batchStatus in ["finished"]:
                # finished
                # try:
                #     payloadExitCode_str = str(job_attr_dict['exit_code'])
                #     payloadExitCode = int(payloadExitCode_str)
                # except KeyError:
                #     errStr = 'cannot get exit_code of submissionHost={0} batchID={1}. Regard the worker as failed'.format(workspec.submissionHost, workspec.batchID)
                #     tmp_log.warning(errStr)
                #     newStatus = WorkSpec.ST_failed
                # except ValueError:
                #     errStr = 'got invalid exit_code {0} of submissionHost={1} batchID={2}. Regard the worker as failed'.format(payloadExitCode_str, workspec.submissionHost, workspec.batchID)
                #     tmp_log.warning(errStr)
                #     newStatus = WorkSpec.ST_failed
                # else:
                #     # Propagate exit_code code
                #     workspec.nativeExitCode = payloadExitCode
                #     if payloadExitCode == 0:
                #         # Payload should return 0 after successful run
                #         newStatus = WorkSpec.ST_finished
                #     else:
                #         # Other return codes are considered failed
                #         newStatus = WorkSpec.ST_failed
                #         errStr = 'Payload execution error: returned non-zero {0}'.format(payloadExitCode)
                #         tmp_log.debug(errStr)
                #         # Map return code to Pilot error code
                #         reduced_exit_code = payloadExitCode // 256 if (payloadExitCode % 256 == 0) else payloadExitCode
                #         pilot_error_code, pilot_error_diag = PILOT_ERRORS.convertToPilotErrors(reduced_exit_code)
                #         if pilot_error_code is not None:
                #             workspec.set_pilot_error(pilot_error_code, pilot_error_diag)
                #     tmp_log.info('Payload return code = {0}'.format(payloadExitCode))
                #
                # finished
                newStatus = WorkSpec.ST_finished
                try:
                    payloadExitCode_str = str(job_attr_dict["exit_code"])
                    payloadExitCode = int(payloadExitCode_str)
                except KeyError:
                    errStr = "cannot get exit_code of submissionHost={0} batchID={1}".format(workspec.submissionHost, workspec.batchID)
                    tmp_log.warning(errStr)
                except ValueError:
                    errStr = "got invalid exit_code {0} of submissionHost={1} batchID={2}".format(
                        payloadExitCode_str, workspec.submissionHost, workspec.batchID
                    )
                    tmp_log.warning(errStr)
                else:
                    # Propagate exit_code code
                    workspec.nativeExitCode = payloadExitCode
                    tmp_log.info("Payload return code = {0}".format(payloadExitCode))
            else:
                errStr = "cannot get reasonable job status of submissionHost={0} batchID={1}. Regard the worker as failed by default".format(
                    workspec.submissionHost, workspec.batchID
                )
                tmp_log.error(errStr)
                newStatus = WorkSpec.ST_failed
            tmp_log.info(
                "submissionHost={0} batchID={1} : batchStatus {2} -> workerStatus {3}".format(workspec.submissionHost, workspec.batchID, batchStatus, newStatus)
            )
    else:
        # Propagate native job status as unknown
        workspec.nativeStatus = "unknown"
        if cancel_unknown:
            errStr = "job submissionHost={0} batchID={1} not found. Regard the worker as canceled by default".format(workspec.submissionHost, workspec.batchID)
            tmp_log.error(errStr)
            newStatus = WorkSpec.ST_cancelled
            tmp_log.info(
                "submissionHost={0} batchID={1} : batchStatus {2} -> workerStatus {3}".format(workspec.submissionHost, workspec.batchID, "3", newStatus)
            )
        else:
            errStr = "job submissionHost={0} batchID={1} not found. Skipped".format(workspec.submissionHost, workspec.batchID)
            tmp_log.warning(errStr)
            newStatus = None
    # Set supplemental error message
    error_code = WorkerErrors.error_codes.get("GENERAL_ERROR") if errStr else WorkerErrors.error_codes.get("SUCCEEDED")
    workspec.set_supplemental_error(error_code=error_code, error_diag=errStr)
    # Return
    return (newStatus, errStr)


# monitor for Lancium
class LanciumMonitor(PluginBase):
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
            self.submissionHost_list
        except AttributeError:
            self.submissionHost_list = []

    # check workers
    def check_workers(self, workspec_list):
        # Make logger for batch job query
        tmp_log = self.make_logger(base_logger, "{0}".format("batch job query"), method_name="check_workers")
        tmp_log.debug("start")
        # Loop over submissionHost
        job_attr_all_dict = {}
        for submissionHost, batchIDs_list in get_host_batch_id_map(workspec_list).items():
            # Record batch job query result to this dict, with key = batchID
            try:
                job_query = LanciumJobQuery(cacheEnable=self.cacheEnable, cacheRefreshInterval=self.cacheRefreshInterval, id=submissionHost)
                host_job_attr_dict = job_query.query_jobs(batchIDs_list=batchIDs_list)
            except Exception as e:
                host_job_attr_dict = {}
                ret_err_str = "Exception {0}: {1}".format(e.__class__.__name__, e)
                tmp_log.error(ret_err_str)
            job_attr_all_dict.update(host_job_attr_dict)
        # Check for all workers
        with Pool(self.nProcesses) as _pool:
            retIterator = _pool.map(
                lambda _x: _check_one_worker(_x, job_attr_all_dict, cancel_unknown=self.cancelUnknown, held_timeout=self.heldTimeout), workspec_list
            )
        retList = list(retIterator)
        tmp_log.debug("done")
        return True, retList

    # report updated workers info to monitor to check
    def report_updated_workers(self, time_window):
        # Make logger for batch job query
        tmp_log = self.make_logger(base_logger, method_name="report_updated_workers")
        tmp_log.debug("start")
        # Get now timestamp
        timeNow = time.time()
        # Set of submission hosts
        submission_host_set = set()
        for submissionHost in self.submissionHost_list:
            submission_host_set.add(submissionHost)
        # Loop over submissionHost and get all jobs
        job_attr_all_dict = {}
        for submissionHost in submission_host_set:
            try:
                job_query = LanciumJobQuery(cacheEnable=self.cacheEnable, cacheRefreshInterval=self.cacheRefreshInterval, id=submissionHost)
                job_attr_all_dict.update(job_query.query_jobs(all_jobs=True))
                tmp_log.debug("got information of jobs on {0}".format(submissionHost))
            except Exception as e:
                ret_err_str = "Exception {0}: {1}".format(e.__class__.__name__, e)
                tmp_log.error(ret_err_str)
        # Choose workers updated within a time window
        workers_to_check_list = []
        for full_batch_id, job_attr_dict in job_attr_all_dict.items():
            # put in worker cache fifo, with lock mechanism
            job_update_at_str = job_attr_dict.get("updated_at")
            try:
                job_update_at = timestamp_to_datetime(job_update_at_str)
            except Exception as e:
                ret_err_str = "Exception {0}: {1}".format(e.__class__.__name__, e)
                tmp_log.error(ret_err_str)
                job_update_at = None
            if job_update_at is not None and not (job_update_at > timeNow - time_window):
                continue
            job_name = job_attr_dict.get("name")
            harvester_id, worker_id = get_workerid_from_job_name(job_name)
            if worker_id is None or harvester_id != harvester_config.master.harvester_id:
                continue
            workers_to_check_list.append((worker_id, job_update_at))
        tmp_log.debug("got {0} workers".format(len(workers_to_check_list)))
        tmp_log.debug("done")
        return workers_to_check_list
