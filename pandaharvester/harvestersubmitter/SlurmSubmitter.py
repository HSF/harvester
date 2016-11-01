import tempfile
import subprocess

from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.PluginBase import PluginBase

# logger
baseLogger = CoreUtils.setupLogger()



# submitter for SLURM batch system
class SlurmSubmitter (PluginBase):

    # constructor
    def __init__(self,**kwarg):
        PluginBase.__init__(self,**kwarg)
        # template for batch script
        tmpFile = open(self.templateFile)
        self.template = tmpFile.read()
        tmpFile.close()
        


    # submit workers
    def submitWorkers(self,workSpecs):
        retList = []
        retStrList = []
        for workSpec in workSpecs:
            # make logger
            tmpLog = CoreUtils.makeLogger(baseLogger,'workerID={0}'.format(workSpec.workerID))
            # make batch script
            batchFile = self.makeBatchScript(workSpec)
            # command
            comStr = "sbatch -D {0} {1}".format(workSpec.getAccessPoint(),batchFile)
            # submit
            tmpLog.debug('submit with {0}'.format(batchFile))
            p = subprocess.Popen(comStr.split(),
                                 shell=False,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            # check return code
            stdOut,stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug('retCode={0}'.format(retCode))
            if retCode == 0:
                # extract batchID
                workSpec.batchID = stdOut.split()[-1]
                tmpLog.debug('batchID={0}'.format(workSpec.batchID))
                tmpRetVal = (True,'')
            else:
                # failed
                errStr = stdOut+' '+stdErr
                tmpLog.error(errStr)
                tmpRetVal = (False,errStr)
            retList.append(tmpRetVal)
        return retList



    # make batch script
    def makeBatchScript(self,workSpec):
        tmpFile = tempfile.NamedTemporaryFile(delete=False)
        tmpFile.write(self.template.format(nCore=workSpec.nCore,
                                           accessPoint=workSpec.accessPoint)
                      )
        tmpFile.close()
        return tmpFile.name

    

