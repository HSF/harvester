import os
import shutil

import six

try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

from pandaharvester.harvestersweeper.base_sweeper import BaseSweeper
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.htcondor_utils import condor_job_id_from_workspec, get_host_batchid_map, _runShell
from pandaharvester.harvestermisc.htcondor_utils import CondorJobManage


# Logger
baseLogger = core_utils.setup_logger("htcondor_sweeper")


# sweeper for HTCONDOR batch system
class HTCondorSweeper(BaseSweeper):
    # constructor
    def __init__(self, **kwarg):
        BaseSweeper.__init__(self, **kwarg)

    # # kill a worker
    # def kill_worker(self, workspec):
    #     # Make logger
    #     tmpLog = self.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
    #                               method_name='kill_worker')
    #
    #     # Skip batch operation for workers without batchID
    #     if workspec.batchID is None:
    #         tmpLog.info('Found workerID={0} has submissionHost={1} batchID={2} . Cannot kill. Skipped '.format(
    #                         workspec.workerID, workspec.submissionHost, workspec.batchID))
    #         return True, ''
    #
    #     # Parse condor remote options
    #     name_opt, pool_opt = '', ''
    #     if workspec.submissionHost is None or workspec.submissionHost == 'LOCAL':
    #         pass
    #     else:
    #         try:
    #             condor_schedd, condor_pool = workspec.submissionHost.split(',')[0:2]
    #         except ValueError:
    #             errStr = 'Invalid submissionHost: {0} . Skipped'.format(workspec.submissionHost)
    #             tmpLog.error(errStr)
    #             return False, errStr
    #         name_opt = '-name {0}'.format(condor_schedd) if condor_schedd else ''
    #         pool_opt = '-pool {0}'.format(condor_pool) if condor_pool else ''
    #
    #     # Kill command
    #     comStr = 'condor_rm {name_opt} {pool_opt} {batchID}'.format(name_opt=name_opt,
    #                                                                 pool_opt=pool_opt,
    #                                                                 batchID=workspec.batchID)
    #     (retCode, stdOut, stdErr) = _runShell(comStr)
    #     if retCode != 0:
    #         comStr = 'condor_q -l {name_opt} {pool_opt} {batchID}'.format(name_opt=name_opt,
    #                                                                     pool_opt=pool_opt,
    #                                                                     batchID=workspec.batchID)
    #         (retCode, stdOut, stdErr) = _runShell(comStr)
    #         if ('ClusterId = {0}'.format(workspec.batchID) in str(stdOut) \
    #             and 'JobStatus = 3' not in str(stdOut)) or retCode != 0:
    #             # Force to cancel if batch job not terminated first time
    #             comStr = 'condor_rm -forcex {name_opt} {pool_opt} {batchID}'.format(name_opt=name_opt,
    #                                                                         pool_opt=pool_opt,
    #                                                                         batchID=workspec.batchID)
    #             (retCode, stdOut, stdErr) = _runShell(comStr)
    #             if retCode != 0:
    #                 # Command failed to kill
    #                 errStr = 'command "{0}" failed, retCode={1}, error: {2} {3}'.format(comStr, retCode, stdOut, stdErr)
    #                 tmpLog.error(errStr)
    #                 return False, errStr
    #         # Found already killed
    #         tmpLog.info('Found workerID={0} submissionHost={1} batchID={2} already killed'.format(
    #                         workspec.workerID, workspec.submissionHost, workspec.batchID))
    #     else:
    #         tmpLog.info('Succeeded to kill workerID={0} submissionHost={1} batchID={2}'.format(
    #                         workspec.workerID, workspec.submissionHost, workspec.batchID))
    #     # Return
    #     return True, ''

    # kill workers

    def kill_workers(self, workspec_list):
        # Make logger
        tmpLog = self.make_logger(baseLogger, method_name="kill_workers")
        tmpLog.debug("start")
        # Initialization
        all_job_ret_map = {}
        retList = []
        # Kill
        for submissionHost, batchIDs_list in six.iteritems(get_host_batchid_map(workspec_list)):
            try:
                condor_job_manage = CondorJobManage(id=submissionHost)
                ret_map = condor_job_manage.remove(batchIDs_list)
            except Exception as e:
                ret_map = {}
                ret_err_str = "Exception {0}: {1}".format(e.__class__.__name__, e)
                tmpLog.error(ret_err_str)
            all_job_ret_map.update(ret_map)
        # Fill return list
        for workspec in workspec_list:
            if workspec.batchID is None:
                ret = (True, "worker without batchID; skipped")
            else:
                ret = all_job_ret_map.get(condor_job_id_from_workspec(workspec), (False, "batch job not found in return map"))
            retList.append(ret)
        tmpLog.debug("done")
        # Return
        return retList

    # cleanup for a worker
    def sweep_worker(self, workspec):
        # Make logger
        tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="sweep_worker")
        tmpLog.debug("start")
        # Clean up preparator base directory (staged-in files)
        try:
            preparatorBasePath = self.preparatorBasePath
        except AttributeError:
            tmpLog.debug("No preparator base directory is configured. Skipped cleaning up preparator directory")
        else:
            if os.path.isdir(preparatorBasePath):
                if not workspec.get_jobspec_list():
                    tmpLog.warning("No job PandaID found relate to workerID={0}. Skipped cleaning up preparator directory".format(workspec.workerID))
                else:
                    for jobspec in workspec.get_jobspec_list():
                        preparator_dir_for_cleanup = os.path.join(preparatorBasePath, str(jobspec.PandaID))
                        if os.path.isdir(preparator_dir_for_cleanup) and preparator_dir_for_cleanup != preparatorBasePath:
                            try:
                                shutil.rmtree(preparator_dir_for_cleanup)
                            except OSError as _err:
                                if "No such file or directory" in _err.strerror:
                                    tmpLog.debug("Found that {0} was already removed".format(_err.filename))
                                pass
                            tmpLog.info("Succeeded to clean up preparator directory: Removed {0}".format(preparator_dir_for_cleanup))
                        else:
                            errStr = "Failed to clean up preparator directory: {0} does not exist or invalid to be cleaned up".format(
                                preparator_dir_for_cleanup
                            )
                            tmpLog.error(errStr)
                            return False, errStr
            else:
                errStr = "Configuration error: Preparator base directory {0} does not exist".format(preparatorBasePath)
                tmpLog.error(errStr)
                return False, errStr
        tmpLog.info("Succeeded to clean up everything about workerID={0}".format(workspec.workerID))
        tmpLog.debug("done")
        # Return
        return True, ""
