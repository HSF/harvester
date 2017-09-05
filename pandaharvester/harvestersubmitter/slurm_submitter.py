import tempfile
import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger('slurm_submitter')


# submitter for SLURM batch system
class SlurmSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
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
            tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workSpec.workerID),
                                            method_name='submit_workers')
            # set nCore
            workSpec.nCore = self.nCore
            # make batch script
            batchFile = self.make_batch_script(workSpec)
            # command
            comStr = "sbatch -D {0} {1}".format(workSpec.get_access_point(), batchFile)
            # submit
            tmpLog.debug('submit with {0}'.format(batchFile))
            p = subprocess.Popen(comStr.split(),
                                 shell=False,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug('retCode={0}'.format(retCode))
            if retCode == 0:
                # extract batchID
                workSpec.batchID = stdOut.split()[-1]
                tmpLog.debug('batchID={0}'.format(workSpec.batchID))
                tmpRetVal = (True, '')
            else:
                # failed
                errStr = stdOut + ' ' + stdErr
                tmpLog.error(errStr)
                tmpRetVal = (False, errStr)
            retList.append(tmpRetVal)
        return retList

    # make batch script
    def make_batch_script(self, workspec):
        tmpFile = tempfile.NamedTemporaryFile(delete=False, suffix='_submit.sh', dir=workspec.get_access_point())
        tmpFile.write(self.template.format(nCorePerNode=self.nCorePerNode,
                                           nNode=workspec.nCore / self.nCorePerNode,
                                           accessPoint=workspec.accessPoint)
                      )
        tmpFile.close()
        return tmpFile.name
