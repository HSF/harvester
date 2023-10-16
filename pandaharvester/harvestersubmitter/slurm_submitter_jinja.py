import tempfile
import re
import jinja2

import six

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger("slurm_submitter")


# submitter for SLURM batch system
class SlurmSubmitterJinja(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.uploadLog = False
        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)

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
            batchFile = self.make_batch_script_jinja(workSpec)
            # command
            comStr = "sbatch -D {0} {1}".format(workSpec.get_access_point(), batchFile)
            # submit
            tmpLog.debug("submit with {0}".format(batchFile))
            p = subprocess.Popen(comStr.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug("retCode={0}".format(retCode))
            stdOut_str = stdOut if (isinstance(stdOut, str) or stdOut is None) else stdOut.decode()
            stdErr_str = stdErr if (isinstance(stdErr, str) or stdErr is None) else stdErr.decode()
            if retCode == 0:
                # extract batchID
                workSpec.batchID = re.search("[^0-9]*([0-9]+)[^0-9]*", "{0}".format(stdOut_str)).group(1)
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
                errStr = "{0} {1}".format(stdOut_str, stdErr_str)
                tmpLog.error(errStr)
                tmpRetVal = (False, errStr)
            retList.append(tmpRetVal)
        return retList

    # make batch script
    def make_batch_script(self, workspec):
        # template for batch script
        tmpFile = open(self.templateFile)
        self.template = tmpFile.read()
        tmpFile.close()
        del tmpFile
        tmpFile = tempfile.NamedTemporaryFile(delete=False, suffix="_submit.sh", dir=workspec.get_access_point())
        tmpFile.write(
            six.b(
                self.template.format(
                    nCorePerNode=self.nCorePerNode, nNode=workspec.nCore // self.nCorePerNode, accessPoint=workspec.accessPoint, workerID=workspec.workerID
                )
            )
        )
        tmpFile.close()
        return tmpFile.name

    # make batch script

    def make_batch_script_jinja(self, workspec):
        # template for batch script
        tmpFile = open(self.templateFile)
        self.template = tmpFile.read()
        tmpFile.close()
        del tmpFile
        tmpFile = tempfile.NamedTemporaryFile(delete=False, suffix="_submit.sh", dir=workspec.get_access_point())
        tm = jinja2.Template(self.template)
        tmpFile.write(
            six.b(
                tm.render(
                    nCorePerNode=self.nCorePerNode,
                    nNode=workspec.nCore // self.nCorePerNode,
                    accessPoint=workspec.accessPoint,
                    workerID=workspec.workerID,
                    workspec=workspec,
                )
            )
        )

        # tmpFile.write(six.b(self.template.format(nCorePerNode=self.nCorePerNode,
        #                                   nNode=workspec.nCore // self.nCorePerNode,
        #                                   accessPoint=workspec.accessPoint,
        #                                   workerID=workspec.workerID))
        #              )
        # tmpFile.write(six.b(self.template.format(nCorePerNode=self.nCorePerNode,
        #                                   nNode=workspec.nCore // self.nCorePerNode,
        #                                   worker=workSpec,
        #                                   submitter=self))
        #              )
        tmpFile.close()
        return tmpFile.name

    # get log file names

    def get_log_file_names(self, batch_script, batch_id):
        stdOut = None
        stdErr = None
        with open(batch_script) as f:
            for line in f:
                if not line.startswith("#SBATCH"):
                    continue
                items = line.split()
                if "-o" in items:
                    stdOut = items[-1].replace("$SLURM_JOB_ID", batch_id)
                elif "-e" in items:
                    stdErr = items[-1].replace("$SLURM_JOB_ID", batch_id)
        return stdOut, stdErr
