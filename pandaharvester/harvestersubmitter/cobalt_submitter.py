import tempfile

try:
    import subprocess32 as subprocess
except BaseException:
    import subprocess
import os
import stat

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger("cobalt_submitter")


# submitter for Cobalt batch system
class CobaltSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.uploadLog = False
        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)
        # template for batch script
        tmpFile = open(self.templateFile)
        self.template = tmpFile.read()
        tmpFile.close()

    # submit workers
    def submit_workers(self, workspec_list):
        retList = []
        retStrList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workSpec.workerID), method_name="submit_workers")
            # set nCore
            workSpec.nCore = self.nCore
            # make batch script
            batchFile = self.make_batch_script(workSpec)
            # command
            # DPBcomStr = "qsub --cwd {0} {1}".format(workSpec.get_access_point(), batchFile)
            comStr = "qsub {0}".format(batchFile)
            # submit
            tmpLog.debug("submit with {0}".format(batchFile))
            p = subprocess.Popen(comStr.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug("retCode={0}".format(retCode))
            if retCode == 0:
                # extract batchID
                workSpec.batchID = stdOut.split()[-1]
                tmpLog.debug("batchID={0}".format(workSpec.batchID))
                # set log files
                if self.uploadLog:
                    if self.logBaseURL is None:
                        baseDir = workSpec.get_access_point()
                    else:
                        baseDir = self.logBaseURL
                    batchLog, stdOut, stdErr = self.get_log_file_names(batchFile, workSpec.batchID)
                    if batchLog is not None:
                        workSpec.set_log_file("batch_log", "{0}/{0}".format(baseDir, batchLog))
                    if stdOut is not None:
                        workSpec.set_log_file("stdout", "{0}/{1}".format(baseDir, stdOut))
                    if stdErr is not None:
                        workSpec.set_log_file("stderr", "{0}/{1}".format(baseDir, stdErr))
                tmpRetVal = (True, "")
            else:
                # failed
                errStr = stdOut + " " + stdErr
                tmpLog.error(errStr)
                tmpRetVal = (False, errStr)
            retList.append(tmpRetVal)
        return retList

    # make batch script
    def make_batch_script(self, workspec):
        tmpFile = tempfile.NamedTemporaryFile(mode="w+t", delete=False, suffix="_submit.sh", dir=workspec.get_access_point())
        tmpFile.write(self.template.format(nNode=int(workspec.nCore / self.nCorePerNode), accessPoint=workspec.accessPoint, workerID=workspec.workerID))
        tmpFile.close()

        # set execution bit on the temp file
        st = os.stat(tmpFile.name)
        os.chmod(tmpFile.name, st.st_mode | stat.S_IEXEC | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)

        return tmpFile.name

    # get log file names
    def get_log_file_names(self, batch_script, batch_id):
        batchLog = None
        stdOut = None
        stdErr = None
        with open(batch_script) as f:
            for line in f:
                if not line.startswith("#COBALT"):
                    continue
                items = line.split()
                if "--debuglog" in items:
                    batchLog = items[-1].replace("$COBALT_JOBID", batch_id)
                elif "-o" in items:
                    stdOut = items[-1].replace("$COBALT_JOBID", batch_id)
                elif "-e" in items:
                    stdErr = items[-1].replace("$COBALT_JOBID", batch_id)
        return batchLog, stdOut, stdErr
