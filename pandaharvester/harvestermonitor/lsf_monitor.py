import re
from shlex import quote, split

try:
    import subprocess32 as subprocess
except BaseException:
    import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec

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
            tmpLog = self.make_logger(baseLogger, f"workerID={workSpec.workerID}", method_name="check_workers")
            # command
            comStr = f"bjobs -a -noheader -o {quote('jobid:10 stat:10')} {workSpec.batchID} "
            comStr_split = split(comStr)
            # check
            p = subprocess.Popen(comStr_split, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            newStatus = workSpec.status
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug(f"len(stdOut) = {len(str(stdOut))} stdOut={stdOut}")
            tmpLog.debug(f"len(stdErr) = {len(str(stdErr))}  stdErr={stdErr}")
            tmpLog.debug(f"retCode={retCode}")
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
                    tmpMatch = re.search(f"{workSpec.batchID}", tmpLine)
                    tmpLog.debug(f"tmpLine = {tmpLine} tmpMatch = {tmpMatch}")
                    if tmpMatch is not None:
                        errStr = tmpLine
                        # search for phrase  is not found
                        tmpMatch = re.search("is not found", tmpLine)
                        if tmpMatch is not None:
                            batchStatus = f"Job {workSpec.batchID} is not found"
                            newStatus = WorkSpec.ST_failed
                            tmpLog.debug(f"batchStatus {batchStatus} -> workerStatus {retCode}")
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
                            tmpLog.debug(f"batchStatus {batchStatus} -> workerStatus {newStatus}")
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
