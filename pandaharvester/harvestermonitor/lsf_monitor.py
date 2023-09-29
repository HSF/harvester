import re
from shlex import quote
from shlex import split

try:
    import subprocess32 as subprocess
except BaseException:
    import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger("lsf_monitor")


# monitor for LSF batch system
class LSFMonitor(PluginBase):
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
            comStr = "bjobs -a -noheader -o {0} {1} ".format(quote("jobid:10 stat:10"), workSpec.batchID)
            comStr_split = split(comStr)
            # check
            p = subprocess.Popen(comStr_split, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            newStatus = workSpec.status
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug("len(stdOut) = {0} stdOut={1}".format(len(str(stdOut)), stdOut))
            tmpLog.debug("len(stdErr) = {0}  stdErr={1}".format(len(str(stdErr)), stdErr))
            tmpLog.debug("retCode={0}".format(retCode))
            errStr = ""
            if retCode == 0:
                # check if any came back on stdOut otherwise check stdErr
                tempresponse = ""
                if len(str(stdOut)) >= len(str(stdErr)):
                    tempresponse = str(stdOut)
                else:
                    tempresponse = str(stdErr)
                # tmpLog.debug('tempresponse = {0}'.format(tempresponse))
                # parse
                for tmpLine in tempresponse.split("\n"):
                    tmpMatch = re.search("{0}".format(workSpec.batchID), tmpLine)
                    tmpLog.debug("tmpLine = {0} tmpMatch = {1}".format(tmpLine, tmpMatch))
                    if tmpMatch is not None:
                        errStr = tmpLine
                        # search for phrase  is not found
                        tmpMatch = re.search("is not found", tmpLine)
                        if tmpMatch is not None:
                            batchStatus = "Job {0} is not found".format(workSpec.batchID)
                            newStatus = WorkSpec.ST_failed
                            tmpLog.debug("batchStatus {0} -> workerStatus {1}".format(batchStatus, retCode))
                        else:
                            batchStatus = tmpLine.split()[-2]
                            if batchStatus in ["RUN"]:
                                newStatus = WorkSpec.ST_running
                            elif batchStatus in ["DONE"]:
                                newStatus = WorkSpec.ST_finished
                            elif batchStatus in ["PEND", "PROV", "WAIT"]:
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
