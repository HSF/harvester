import datetime
import threading

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.JobSpec import JobSpec
from pandaharvester.harvestercore.DBProxy import DBProxy

# logger
_logger = CoreUtils.setupLogger()


# propagate important checkpoints to panda
class Propagator(threading.Thread):
    # constructor
    def __init__(self, communicator, single_mode=False):
        threading.Thread.__init__(self)
        self.dbProxy = DBProxy()
        self.communicator = communicator
        self.singleMode = single_mode

    # main loop
    def run(self):
        while True:
            mainLog = CoreUtils.makeLogger(_logger, 'id={0}'.format(self.ident))
            mainLog.debug('getting jobs to propagate')
            jobSpecs = self.dbProxy.getJobsToPropagate(harvester_config.propagator.maxJobs,
                                                       harvester_config.propagator.lockInterval,
                                                       harvester_config.propagator.updateInterval,
                                                       self.ident)
            mainLog.debug('got {0} jobs'.format(len(jobSpecs)))
            # update jobs in central database
            iJobs = 0
            nJobs = harvester_config.propagator.nJobsInBulk
            while iJobs < len(jobSpecs):
                jobList = jobSpecs[iJobs:iJobs + nJobs]
                iJobs += nJobs
                retList = self.communicator.updateJobs(jobList)
                okPandaIDs = []
                # logging
                for tmpJobSpec, tmpRet in zip(jobList, retList):
                    if tmpRet['StatusCode'] == 0:
                        mainLog.debug('updated PandaID={0} status={1}'.format(tmpJobSpec.PandaID,
                                                                              tmpJobSpec.status))
                        # release job
                        tmpJobSpec.propagatorLock = None
                        if tmpJobSpec.isFinalStatus() and tmpJobSpec.allEventsDone():
                            # unset to disable further updating
                            tmpJobSpec.propagatorTime = None
                            tmpJobSpec.subStatus = 'done'
                        self.dbProxy.updateJob(tmpJobSpec, {'propagatorLock': self.ident})
                    else:
                        mainLog.error('failed to update PandaID={0} status={1}'.format(tmpJobSpec.PandaID,
                                                                                       tmpJobSpec.status))
            mainLog.debug('done')
            if self.singleMode:
                return
            # sleep
            CoreUtils.sleep(harvester_config.propagator.sleepTime)
