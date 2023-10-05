import re

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase

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
            tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workSpec.workerID), method_name="check_workers")
            # command
            comStr = "sacct --jobs={0}".format(workSpec.batchID)
            # check
            tmpLog.debug("check with {0}".format(comStr))
            p = subprocess.Popen(comStr.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            newStatus = workSpec.status
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug("retCode={0}".format(retCode))
            errStr = ""
            stdOut_str = stdOut if (isinstance(stdOut, str) or stdOut is None) else stdOut.decode()
            stdErr_str = stdErr if (isinstance(stdErr, str) or stdErr is None) else stdErr.decode()
            if retCode == 0:
                for tmpLine in stdOut_str.split("\n"):
                    tmpMatch = re.search("{0} ".format(workSpec.batchID), tmpLine)
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
                        tmpLog.debug("batchStatus {0} -> workerStatus {1}".format(batchStatus, newStatus))
                        break
                retList.append((newStatus, errStr))
            else:
                # failed
                errStr = "{0} {1}".format(stdOut_str, stdErr_str)
                tmpLog.error(errStr)
                if "slurm_load_jobs error: Invalid job id specified" in errStr:
                    newStatus = WorkSpec.ST_failed
                retList.append((newStatus, errStr))
        return True, retList
