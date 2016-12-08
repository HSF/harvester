from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy import DBProxy
from pandaharvester.harvesterbody.agent_base import AgentBase

# logger
_logger = core_utils.setup_logger()


# propagate important checkpoints to panda
class Propagator(AgentBase):
    # constructor
    def __init__(self, communicator, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.communicator = communicator


    # main loop
    def run(self):
        while True:
            mainLog = core_utils.make_logger(_logger, 'id={0}'.format(self.ident))
            mainLog.debug('getting jobs to propagate')
            jobSpecs = self.dbProxy.get_jobs_to_propagate(harvester_config.propagator.maxJobs,
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
                retList = self.communicator.update_jobs(jobList)
                okPandaIDs = []
                # logging
                for tmpJobSpec, tmpRet in zip(jobList, retList):
                    if tmpRet['StatusCode'] == 0:
                        mainLog.debug('updated PandaID={0} status={1}'.format(tmpJobSpec.PandaID,
                                                                              tmpJobSpec.status))
                        # release job
                        tmpJobSpec.propagatorLock = None
                        if tmpJobSpec.is_final_status() and tmpJobSpec.status == tmpJobSpec.get_status():
                            # unset to disable further updating
                            tmpJobSpec.propagatorTime = None
                            tmpJobSpec.subStatus = 'done'
                        self.dbProxy.update_job(tmpJobSpec, {'propagatorLock': self.ident})
                    else:
                        mainLog.error('failed to update PandaID={0} status={1}'.format(tmpJobSpec.PandaID,
                                                                                       tmpJobSpec.status))
            mainLog.debug('done')
            # check if being terminated
            if self.terminated(harvester_config.propagator.sleepTime):
                mainLog.debug('terminated')
                return
