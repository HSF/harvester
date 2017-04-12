import tempfile
import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger()


# submitter for HTCONDOR batch system
class HTCondorSubmitter(PluginBase):
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
            tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workSpec.workerID))

            ## Aggregate resources requirement from jobs
            # (agg_nCore, agg_RAM, agg_disk, max_walltime) = (0, 0, 0, 0)
            if workSpec.get_jobspec_list():
                for jobSpec in workSpec.get_jobspec_list():
                    tmpLog.debug('PandaID={0}, nCore={1}, RAM={2}, disk={3}, wallTime={4}'.format(
                                                                jobSpec.PandaID,
                                                                jobSpec.jobParams['coreCount'],
                                                                jobSpec.jobParams['minRamCount'],
                                                                jobSpec.jobParams['maxDiskCount'],
                                                                jobSpec.jobParams['maxWalltime'],
                                                                ) )
                    # agg_nCore += jobSpec.jobParams['coreCount']
                    # agg_RAM += jobSpec.jobParams['minRamCount']
                    # agg_disk += jobSpec.jobParams['maxDiskCount']
                    # max_walltime = max(max_walltime, jobSpec.jobParams['maxWalltime'])

            ## Set overall resource requirement on this worker. Set lower bound to be 1 for each requirement
            ## For unit test only. Now Commented.
            # workSpec.nCore = max(agg_nCore, 1)
            # workSpec.minRamCount = max(agg_RAM, 1)
            # workSpec.maxDiskCount = max(agg_disk, 1)
            # workSpec.maxWalltime = max(max_walltime, 1)

            # make batch script
            batchFile = self.make_batch_script(workSpec)
            # command
            comStr = 'condor_submit {0}'.format(batchFile)
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
        tmpFile = tempfile.NamedTemporaryFile(delete=False, suffix='_submit.sdf', dir=workspec.get_access_point())
        ## Note: In workspec, unit of minRamCount and of maxDiskCount are both MB.
        ##       In HTCondor SDF, unit of request_memory is MB, and request_disk is KB.
        tmpFile.write(self.template.format(
                                            nCorePerNode=self.nCorePerNode,
                                            nCoreTotal=workspec.nCore,
                                            nNode=( workspec.nCore // self.nCorePerNode + min(workspec.nCore % self.nCorePerNode, 1) ),
                                            requestRam=workspec.minRamCount,
                                            requestDisk=(workspec.maxDiskCount * 1024),
                                            requestWalltime=workspec.maxWalltime,
                                            accessPoint=self.accessPoint,
                                            logDir=self.logDir,
                                            )
                      )
        tmpFile.close()
        return tmpFile.name
