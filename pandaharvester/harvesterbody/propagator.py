from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy import DBProxy
from pandaharvester.harvesterbody.agent_base import AgentBase

# logger
_logger = core_utils.setup_logger()


# propagate important checkpoints to panda
class Propagator(AgentBase):
    # constructor
    def __init__(self, communicator, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.communicator = communicator
        self.queueConfigMapper = queue_config_mapper

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
            hbSuppressMap = dict()
            while iJobs < len(jobSpecs):
                jobList = jobSpecs[iJobs:iJobs + nJobs]
                iJobs += nJobs
                # collect jobs to update or skip
                jobListToUpdate = []
                jobListToSkip = []
                retList = []
                for tmpJobSpec in jobList:
                    if tmpJobSpec.computingSite not in hbSuppressMap:
                        queueConfig = self.queueConfigMapper.get_queue(tmpJobSpec.computingSite)
                        hbSuppressMap[tmpJobSpec.computingSite] = queueConfig.noHeartbeat.split()
                    # heartbeat is suppressed
                    if tmpJobSpec.status in hbSuppressMap[tmpJobSpec.computingSite]:
                        jobListToSkip.append(tmpJobSpec)
                        retList.append({'StatusCode': 0})
                    else:
                        jobListToUpdate.append(tmpJobSpec)
                retList = self.communicator.update_jobs(jobListToUpdate)
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
            mainLog.debug('getting workers to propagate')
            workSpecs = self.dbProxy.get_workers_to_propagate(harvester_config.propagator.maxWorkers,
                                                              harvester_config.propagator.updateInterval)
            mainLog.debug('got {0} workers'.format(len(workSpecs)))
            # update workers in central database
            iWorkers = 0
            nWorkers = harvester_config.propagator.nWorkersInBulk
            while iWorkers < len(workSpecs):
                workList = workSpecs[iWorkers:iWorkers + nJobs]
                iWorkers += nWorkers
                retList, tmpErrStr = self.communicator.update_workers(workList)
                # logging
                if retList is None:
                    mainLog.error('failed to update workers with {0}'.format(tmpErrStr))
                else:
                    for tmpWorkSpec, tmpRet in zip(workList, retList):
                        if tmpRet:
                            mainLog.debug('updated workerID={0} status={1}'.format(tmpWorkSpec.workerID,
                                                                                   tmpWorkSpec.status))
                            # disable further update
                            if tmpWorkSpec.is_final_status():
                                tmpWorkSpec.disable_propagation()
                            self.dbProxy.update_worker(tmpWorkSpec, {'workerID': tmpWorkSpec.workerID})
                        else:
                            mainLog.error('failed to update workerID={0} status={1}'.format(tmpWorkSpec.workerID,
                                                                                            tmpWorkSpec.status))
            mainLog.debug('done')
            # check if being terminated
            if self.terminated(harvester_config.propagator.sleepTime):
                mainLog.debug('terminated')
                return
