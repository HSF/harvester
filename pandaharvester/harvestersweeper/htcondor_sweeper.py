import os
import shutil

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.htcondor_utils import (
    CondorJobManage,
    condor_job_id_from_workspec,
    get_host_batchid_map,
)
from pandaharvester.harvestersweeper.base_sweeper import BaseSweeper

# Logger
baseLogger = core_utils.setup_logger("htcondor_sweeper")


# sweeper for HTCONDOR batch system
class HTCondorSweeper(BaseSweeper):
    # constructor
    def __init__(self, **kwarg):
        BaseSweeper.__init__(self, **kwarg)

    # kill workers
    def kill_workers(self, workspec_list):
        # Make logger
        tmpLog = self.make_logger(baseLogger, method_name="kill_workers")
        tmpLog.debug("start")
        # Initialization
        all_job_ret_map = {}
        retList = []
        # Kill
        for submissionHost, batchIDs_dict in get_host_batchid_map(workspec_list).items():
            try:
                ret_map = CondorJobManage(id=submissionHost).remove(list(batchIDs_dict.keys()))
            except Exception as e:
                ret_err_str = f"failed to kill jobs on submissionHost={submissionHost} ; {e.__class__.__name__}: {e}"
                tmpLog.error(ret_err_str)
                ret_map = {condor_job_id_from_workspec(workspec): (False, ret_err_str) for workspec in batchIDs_dict.values()}
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
        tmpLog = self.make_logger(baseLogger, f"workerID={workspec.workerID}", method_name="sweep_worker")
        tmpLog.debug("start")
        # Clean up preparator base directory (staged-in files)
        try:
            preparatorBasePath = self.preparatorBasePath
        except AttributeError:
            tmpLog.debug("No preparator base directory is configured. Skipped cleaning up preparator directory")
        else:
            if os.path.isdir(preparatorBasePath):
                jobspec_list = workspec.get_jobspec_list()
                if not jobspec_list:
                    tmpLog.warning(f"No job PandaID found relate to workerID={workspec.workerID}. Skipped cleaning up preparator directory")
                else:
                    for jobspec in jobspec_list:
                        preparator_dir_for_cleanup = os.path.join(preparatorBasePath, str(jobspec.PandaID))
                        if not os.path.isdir(preparator_dir_for_cleanup):
                            # never created or already removed
                            tmpLog.debug(f"No preparator directory {preparator_dir_for_cleanup} to clean up")
                            continue
                        try:
                            shutil.rmtree(preparator_dir_for_cleanup)
                        except FileNotFoundError as _err:
                            tmpLog.debug(f"Found that {_err.filename} was already removed")
                        except OSError as _err:
                            errStr = f"Failed to clean up preparator directory {preparator_dir_for_cleanup}: {_err}"
                            tmpLog.error(errStr)
                            return False, errStr
                        tmpLog.info(f"Succeeded to clean up preparator directory: Removed {preparator_dir_for_cleanup}")
            else:
                errStr = f"Configuration error: Preparator base directory {preparatorBasePath} does not exist"
                tmpLog.error(errStr)
                return False, errStr
        tmpLog.info(f"Succeeded to clean up everything about workerID={workspec.workerID}")
        tmpLog.debug("done")
        # Return
        return True, ""
