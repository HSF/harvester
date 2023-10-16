import datetime
import tempfile

try:
    import subprocess32 as subprocess
except BaseException:
    import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger("pbs_submitter")


# submitter for PBS batch system
class PBSSubmitter(PluginBase):
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
        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workSpec.workerID), method_name="submit_workers")
            # make batch script
            batchFile = self.make_batch_script(workSpec)
            # command
            comStr = "qsub {0}".format(batchFile)
            # submit
            tmpLog.debug("submit with {0}".format(comStr))
            p = subprocess.Popen(comStr.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
                    stdOut, stdErr = self.get_log_file_names(batchFile, workSpec.batchID)
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
        if hasattr(self, "dynamicSizing") and self.dynamicSizing is True:
            maxWalltime = str(datetime.timedelta(seconds=workspec.maxWalltime))
            yodaWallClockLimit = workspec.maxWalltime / 60
        else:
            workspec.nCore = self.nCore
            maxWalltime = str(datetime.timedelta(seconds=self.maxWalltime))
            yodaWallClockLimit = self.maxWalltime / 60
        tmpFile = tempfile.NamedTemporaryFile(delete=False, suffix="_submit.sh", dir=workspec.get_access_point())
        tmpFile.write(
            self.template.format(
                nCorePerNode=self.nCorePerNode,
                localQueue=self.localQueue,
                projectName=self.projectName,
                nNode=workspec.nCore / self.nCorePerNode,
                accessPoint=workspec.accessPoint,
                walltime=maxWalltime,
                yodaWallClockLimit=yodaWallClockLimit,
                workerID=workspec.workerID,
            )
        )
        tmpFile.close()
        return tmpFile.name

    # get log file names
    def get_log_file_names(self, batch_script, batch_id):
        stdOut = None
        stdErr = None
        with open(batch_script) as f:
            for line in f:
                if not line.startswith("#PBS"):
                    continue
                items = line.split()
                if "-o" in items:
                    stdOut = items[-1].replace("$SPBS_JOBID", batch_id)
                elif "-e" in items:
                    stdErr = items[-1].replace("$PBS_JOBID", batch_id)
        return stdOut, stdErr
