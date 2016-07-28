import datetime
import threading

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.JobSpec import JobSpec
from pandaharvester.harvestercore.DBProxy import DBProxy

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('Propagator')


# propagate important checkpoints to panda
class Propagator (threading.Thread):

    # constructor
    def __init__(self,communicator,singleMode=False):
        threading.Thread.__init__(self)
        self.dbProxy = DBProxy()
        self.communicator = communicator
        self.singleMode = singleMode



    # main loop
    def run (self):
        while True:
            mainLog = CoreUtils.makeLogger(_logger,'id={0}'.format(self.ident))
            mainLog.debug('getting jobs to propagate')
            jobSpecs = self.dbProxy.getJobsToPropagate(harvester_config.prop.maxJobs,
                                                       harvester_config.prop.lockInterval,
                                                       harvester_config.prop.updateInterval,
                                                       self.ident)
            mainLog.debug('got {0} jobs'.format(len(jobSpecs)))
            # update jobs in central database
            iJobs = 0
            nJobs = harvester_config.prop.nJobsInBulk
            while iJobs < len(jobSpecs):
                jobList = jobSpecs[iJobs:iJobs+nJobs]
                iJobs += nJobs
                retList = self.communicator.updateJobs(jobList)
                okPandaIDs = []
                # logging
                for tmpJobSpec,tmpRet in zip(jobList,retList):
                    if tmpRet['StatusCode'] == 0:
                        mainLog.debug('updated PandaID={0} status={1}'.format(tmpJobSpec.PandaID,
                                                                              tmpJobSpec.status))
                        # release job
                        tmpJobSpec.propagatorLock = None
                        if tmpJobSpec.status in ['finished','failed','cancelled']:
                            # unset to disable further updating
                            tmpJobSpec.propagatorTime = None
                        self.dbProxy.updateJob(tmpJobSpec,{'propagatorLock':self.ident})
                    else:
                        mainLog.error('failed to update PandaID={0} status={1}'.format(tmpJobSpec.PandaID,
                                                                                       tmpJobSpec.status))
            mainLog.debug('done')
            if self.singleMode:
                return
            # sleep
            CoreUtils.sleep(harvester_config.prop.sleepTime)
