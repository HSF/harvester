import re
import time
import datetime

try:
    from threading import get_ident
except ImportError:
    from thread import get_ident

import six

from concurrent.futures import ThreadPoolExecutor as Pool

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.htcondor_utils import condor_job_id_from_workspec, get_host_batchid_map, _runShell
from pandaharvester.harvestermisc.htcondor_utils import CondorJobQuery


# logger
baseLogger = core_utils.setup_logger('htcondor_monitor')


# Native HTCondor job status map
CONDOR_JOB_STATUS_MAP = {
    '1': 'idle',
    '2': 'running',
    '3': 'removed',
    '4': 'completed',
    '5': 'held',
    '6': 'transferring_output',
    '7': 'suspended',
    }


# Check one worker
def _check_one_worker(workspec, job_ads_all_dict, cancel_unknown=False, held_timeout=3600):
    # Make logger for one single worker
    tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID), method_name='_check_one_worker')
    # Initialize newStatus
    newStatus = workspec.status
    errStr = ''
    name_opt, pool_opt = '', ''
    if workspec.submissionHost is None or workspec.submissionHost == 'LOCAL':
        pass
    else:
        try:
            condor_schedd, condor_pool = workspec.submissionHost.split(',')[0:2]
        except ValueError:
            pass
        else:
            name_opt = '-name {0}'.format(condor_schedd) if condor_schedd else ''
            pool_opt = '-pool {0}'.format(condor_pool) if condor_pool else ''
    try:
        job_ads_dict = job_ads_all_dict[condor_job_id_from_workspec(workspec)]
    except KeyError:
        got_job_ads = False
    except Exception as e:
        got_job_ads = False
        tmpLog.error('With error {0}'.format(e))
    else:
        got_job_ads = True
    # Parse job ads
    if got_job_ads:
        # Check JobStatus
        try:
            batchStatus = str(job_ads_dict['JobStatus'])
        except KeyError:
            # Propagate native condor job status as unknown
            workspec.nativeStatus = 'unknown'
            if cancel_unknown:
                newStatus = WorkSpec.ST_cancelled
                errStr = 'cannot get JobStatus of job submissionHost={0} batchID={1}. Regard the worker as canceled'.format(workspec.submissionHost, workspec.batchID)
                tmpLog.error(errStr)
            else:
                newStatus = None
                errStr = 'cannot get JobStatus of job submissionHost={0} batchID={1}. Skipped'.format(workspec.submissionHost, workspec.batchID)
                tmpLog.warning(errStr)
        else:
            # Try to get LastJobStatus
            lastBatchStatus = str(job_ads_dict.get('LastJobStatus', ''))
            # Set batchStatus if lastBatchStatus is terminated status
            if (lastBatchStatus in ['3', '4'] and batchStatus not in ['3', '4']) \
                or (lastBatchStatus in ['4'] and batchStatus in ['3']):
                batchStatus = lastBatchStatus
                tmpLog.warning('refer to LastJobStatus={0} as new status of job submissionHost={1} batchID={2} to avoid reversal in status (Jobstatus={3})'.format(
                                lastBatchStatus, workspec.submissionHost, workspec.batchID, str(job_ads_dict['JobStatus'])))
            # Propagate native condor job status
            workspec.nativeStatus = CONDOR_JOB_STATUS_MAP.get(batchStatus, 'unexpected')
            if batchStatus in ['2', '6']:
                # 2 running, 6 transferring output
                newStatus = WorkSpec.ST_running
            elif batchStatus in ['1', '7']:
                # 1 idle, 7 suspended
                if job_ads_dict.get('JobStartDate'):
                    newStatus = WorkSpec.ST_idle
                else:
                    newStatus = WorkSpec.ST_submitted
            elif batchStatus in ['3']:
                # 3 removed
                if not errStr:
                    errStr = 'Condor HoldReason: {0} ; Condor RemoveReason: {1} '.format(
                                job_ads_dict.get('LastHoldReason'), job_ads_dict.get('RemoveReason'))
                newStatus = WorkSpec.ST_cancelled
            elif batchStatus in ['5']:
                # 5 held
                errStr = 'Condor HoldReason: {0} '.format(job_ads_dict.get('HoldReason'))
                if (
                    job_ads_dict.get('HoldReason') == 'Job not found'
                    or int(time.time()) - int(job_ads_dict.get('EnteredCurrentStatus', 0)) > held_timeout
                    ):
                    # Kill the job if held too long or other reasons
                    (retCode, stdOut, stdErr) = _runShell('condor_rm {name_opt} {pool_opt} {batchID}'.format(
                                                                                    batchID=workspec.batchID,
                                                                                    name_opt=name_opt,
                                                                                    pool_opt=pool_opt,
                                                                                    ))
                    if retCode == 0:
                        tmpLog.info('killed held job submissionHost={0} batchID={1}'.format(workspec.submissionHost, workspec.batchID))
                    else:
                        newStatus = WorkSpec.ST_cancelled
                        tmpLog.error('cannot kill held job submissionHost={0} batchID={1}. Force worker to be in cancelled status'.format(workspec.submissionHost, workspec.batchID))
                    errStr += 'Worker canceled by harvester '
                    # Mark the PanDA job as closed instead of failed
                    workspec.set_pilot_closed()
                    tmpLog.debug('Called workspec set_pilot_closed')
                else:
                    if job_ads_dict.get('JobStartDate'):
                        newStatus = WorkSpec.ST_idle
                    else:
                        newStatus = WorkSpec.ST_submitted
            elif batchStatus in ['4']:
                # 4 completed
                try:
                    payloadExitCode = str(job_ads_dict['ExitCode'])
                except KeyError:
                    errStr = 'cannot get ExitCode of job submissionHost={0} batchID={1}. Regard the worker as failed'.format(workspec.submissionHost, workspec.batchID)
                    tmpLog.warning(errStr)
                    newStatus = WorkSpec.ST_failed
                else:
                    # Propagate condor return code
                    workspec.nativeExitCode = payloadExitCode
                    if payloadExitCode in ['0']:
                        # Payload should return 0 after successful run
                        newStatus = WorkSpec.ST_finished
                    else:
                        # Other return codes are considered failed
                        newStatus = WorkSpec.ST_failed
                        errStr = 'Payload execution error: returned non-zero'
                        tmpLog.debug(errStr)
                    tmpLog.info('Payload return code = {0}'.format(payloadExitCode))
            else:
                errStr = 'cannot get reasonable JobStatus of job submissionHost={0} batchID={1}. Regard the worker as failed by default'.format(
                            workspec.submissionHost, workspec.batchID)
                tmpLog.error(errStr)
                newStatus = WorkSpec.ST_failed
            tmpLog.info('submissionHost={0} batchID={1} : batchStatus {2} -> workerStatus {3}'.format(
                            workspec.submissionHost, workspec.batchID, batchStatus, newStatus))
    else:
        # Propagate native condor job status as unknown
        workspec.nativeStatus = 'unknown'
        if cancel_unknown:
            errStr = 'condor job submissionHost={0} batchID={1} not found. Regard the worker as canceled by default'.format(
                            workspec.submissionHost, workspec.batchID)
            tmpLog.error(errStr)
            newStatus = WorkSpec.ST_cancelled
            tmpLog.info('submissionHost={0} batchID={1} : batchStatus {2} -> workerStatus {3}'.format(
                            workspec.submissionHost, workspec.batchID, '3', newStatus))
        else:
            errStr = 'condor job submissionHost={0} batchID={1} not found. Skipped'.format(
                            workspec.submissionHost, workspec.batchID)
            tmpLog.warning(errStr)
            newStatus = None
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

    # check workers
    def check_workers(self, workspec_list):
        # Make logger for batch job query
        tmpLog = self.make_logger(baseLogger, '{0}'.format('batch job query'),
                                  method_name='check_workers')
        tmpLog.debug('start')
        # Loop over submissionHost
        job_ads_all_dict = {}
        for submissionHost, batchIDs_list in six.iteritems(get_host_batchid_map(workspec_list)):
            # Record batch job query result to this dict, with key = batchID
            job_query = CondorJobQuery( cacheEnable=self.cacheEnable,
                                        cacheRefreshInterval=self.cacheRefreshInterval,
                                        useCondorHistory=self.useCondorHistory,
                                        id=submissionHost)
            job_ads_all_dict.update(job_query.get_all(batchIDs_list=batchIDs_list))
        # Check for all workers
        with Pool(self.nProcesses) as _pool:
            retIterator = _pool.map(lambda _x: _check_one_worker(
                                        _x, job_ads_all_dict,
                                        cancel_unknown=self.cancelUnknown,
                                        held_timeout=self.heldTimeout),
                                    workspec_list)
        retList = list(retIterator)
        tmpLog.debug('done')
        return True, retList
