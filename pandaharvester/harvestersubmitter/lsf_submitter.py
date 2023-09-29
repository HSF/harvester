import datetime
import tempfile
import re

try:
    import subprocess32 as subprocess
except BaseException:
    import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger("lsf_submitter")


# submitter for LSF batch system
class LSFSubmitter(PluginBase):
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
            comStr = "bsub -L /bin/sh"
            # submit
            tmpLog.debug("submit with {0} and LSF options file {1}".format(comStr, batchFile))
            p = subprocess.Popen(comStr.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=open(batchFile, "r"))
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug("retCode={0}".format(retCode))
            tmpLog.debug("stdOut={0}".format(stdOut))
            tmpLog.debug("stdErr={0}".format(stdErr))
            if retCode == 0:
                # extract batchID
                batchID = str(stdOut.split()[1], "utf-8")
                result = re.sub("[^0-9]", "", batchID)
                tmpLog.debug("strip out non-numberic charactors from {0} - result {1}".format(batchID, result))
                workSpec.batchID = result
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
        # if hasattr(self, 'dynamicSizing') and self.dynamicSizing is True:
        #    maxWalltime = str(datetime.timedelta(seconds=workspec.maxWalltime))
        #    yodaWallClockLimit = workspec.maxWalltime / 60
        # else:
        #    workspec.nCore = self.nCore
        #    maxWalltime = str(datetime.timedelta(seconds=self.maxWalltime))
        #    yodaWallClockLimit = self.maxWalltime / 60

        # set number of nodes  - Note Ultimately will need to something more sophisticated
        if hasattr(self, "nGpuPerNode"):
            if int(self.nGpuPerNode) > 0:
                numnodes = int(workspec.nJobs / self.nGpuPerNode)
                if numnodes <= 0:
                    numnodes = 1
                else:
                    if (workspec.nJobs % self.nGpuPerNode) != 0:
                        numnodes += 1
        else:
            numnodes = workspec.nCore / self.nCorePerNode

        tmpFile = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix="_submit.sh", dir=workspec.get_access_point())
        tmpFile.write(
            self.template.format(
                nCorePerNode=self.nCorePerNode,
                # localQueue=self.localQueue,
                # projectName=self.projectName,
                nNode=numnodes,
                accessPoint=workspec.accessPoint,
                # walltime=maxWalltime,
                # yodaWallClockLimit=yodaWallClockLimit,
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
                if not line.startswith("#BSUB"):
                    continue
                items = line.split()
                if "-o" in items:
                    # stdOut = items[-1].replace('$LSB_BATCH_JID', batch_id)
                    stdOut = items[-1].replace("%J", batch_id)
                elif "-e" in items:
                    # stdErr = items[-1].replace('$LSB_BATCH_JID', batch_id)
                    stdErr = items[-1].replace("%J", batch_id)
        return stdOut, stdErr
