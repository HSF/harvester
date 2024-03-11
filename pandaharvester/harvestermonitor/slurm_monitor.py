import re
import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec

# logger
baseLogger = core_utils.setup_logger("slurm_monitor")


# monitor for SLURM batch system
class SlurmMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check workers
    def check_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, f"workerID={workSpec.workerID}", method_name="check_workers")
            # command
            comStr = f"sacct --jobs={workSpec.batchID}"
            # check
            tmpLog.debug(f"check with {comStr}")
            p = subprocess.Popen(comStr.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            newStatus = workSpec.status
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug(f"retCode={retCode}")
            errStr = ""
            stdOut_str = stdOut if (isinstance(stdOut, str) or stdOut is None) else stdOut.decode()
            stdErr_str = stdErr if (isinstance(stdErr, str) or stdErr is None) else stdErr.decode()
            if retCode == 0:
                for tmpLine in stdOut_str.split("\n"):
                    tmpMatch = re.search(f"{workSpec.batchID} ", tmpLine)
                    if tmpMatch is not None:
                        errStr = tmpLine
                        batchStatus = tmpLine.split()[5]
                        if batchStatus in ["RUNNING", "COMPLETING", "STOPPED", "SUSPENDED"]:
                            newStatus = WorkSpec.ST_running
                        elif batchStatus in ["COMPLETED", "PREEMPTED", "TIMEOUT"]:
                            newStatus = WorkSpec.ST_finished
                        elif batchStatus in ["CANCELLED"]:
                            newStatus = WorkSpec.ST_cancelled
                        elif batchStatus in ["CONFIGURING", "PENDING"]:
                            newStatus = WorkSpec.ST_submitted
                        else:
                            newStatus = WorkSpec.ST_failed
                        tmpLog.debug(f"batchStatus {batchStatus} -> workerStatus {newStatus}")
                        break
                retList.append((newStatus, errStr))
            else:
                # failed
                errStr = f"{stdOut_str} {stdErr_str}"
                tmpLog.error(errStr)
                if "slurm_load_jobs error: Invalid job id specified" in errStr:
                    newStatus = WorkSpec.ST_failed
                retList.append((newStatus, errStr))
        return True, retList
