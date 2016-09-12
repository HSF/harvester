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
        self.template = """
#!/bin/bash -l
#SBATCH -p {partition}
#SBATCH -N {nodes}
#SBATCH -t {time}
#SBATCH -L {license}
#SBATCH -D {workDir}
srun {application} {accessPoint}
"""
        # remove the first newline
        self.template = self.template[1:]
        


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
            comStr = "sbatch {0}".format(batchFile)
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
        tmpFile.write(self.template.format(partition=self.partition,
                                           nodes=self.nodes,
                                           time=self.time,
                                           license=self.license,
                                           application=self.application,
                                           workDir=self.workDir,
                                           accessPoint=workSpec.accessPoint)
                      )
        tmpFile.close()
        return tmpFile.name

    

