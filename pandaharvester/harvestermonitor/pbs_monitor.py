import re

try:
    import subprocess32 as subprocess
except BaseException:
    import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger("pbs_monitor")


# monitor for PBS batch system
class PBSMonitor(PluginBase):
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
            comStr = "qstat {0}".format(workSpec.batchID)
            # check
            tmpLog.debug("check with {0}".format(comStr))
            p = subprocess.Popen(comStr.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            newStatus = workSpec.status
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug("retCode={0}".format(retCode))
            errStr = ""
            if retCode == 0:
                # parse
                for tmpLine in stdOut.split("\n"):
                    tmpMatch = re.search("{0} ".format(workSpec.batchID), tmpLine)
                    if tmpMatch is not None:
                        errStr = tmpLine
                        batchStatus = tmpLine.split()[-2]
                        if batchStatus in ["R", "E"]:
                            newStatus = WorkSpec.ST_running
                        elif batchStatus in ["C", "H"]:
                            newStatus = WorkSpec.ST_finished
                        elif batchStatus in ["CANCELLED"]:
                            newStatus = WorkSpec.ST_cancelled
                        elif batchStatus in ["Q", "W", "S"]:
                            newStatus = WorkSpec.ST_submitted
                        else:
                            newStatus = WorkSpec.ST_failed
                        tmpLog.debug("batchStatus {0} -> workerStatus {1}".format(batchStatus, newStatus))
                        break
                retList.append((newStatus, errStr))
            else:
                # failed
                errStr = stdOut + " " + stdErr
                tmpLog.error(errStr)
                if "Unknown Job Id Error" in errStr:
                    tmpLog.info("Mark job as finished.")
                    newStatus = WorkSpec.ST_finished
                retList.append((newStatus, errStr))
        return True, retList
