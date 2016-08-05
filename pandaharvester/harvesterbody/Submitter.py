import datetime
import threading


from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.DBProxy import DBProxy
from pandaharvester.harvestercore.PluginFactory import PluginFactory
from WorkMaker import WorkMaker


# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('Submitter')


# class to submit workers
class Submitter (threading.Thread):

    # constructor
    def __init__(self,queueConfigMapper,singleMode=False):
        threading.Thread.__init__(self)
        self.queueConfigMapper = queueConfigMapper
        self.dbProxy = DBProxy()
        self.singleMode = singleMode
        self.workMaker = WorkMaker()
        self.pluginFactory = PluginFactory()


    # main loop
    def run (self):
        lockedBy = 'submitter-{0}'.format(self.ident)
        while True:
            mainLog = CoreUtils.makeLogger(_logger,'id={0}'.format(lockedBy))
            mainLog.debug('getting queues to submit workers')
            # get queues to submit workers
            nWorkersPerQueue = self.dbProxy.getQueuesToSubmit(harvester_config.submitter.nQueues,
                                                              harvester_config.submitter.lookupTime)
            mainLog.debug('got {0} queues'.format(len(nWorkersPerQueue)))
            # loop over all queues
            for queueName,nWorkers in nWorkersPerQueue.iteritems():
                tmpLog = CoreUtils.makeLogger(_logger,'queue={0}'.format(queueName))
                tmpLog.debug('start')
                # check queue
                if not self.queueConfigMapper.hasQueue(queueName):
                    tmpLog.error('config not found')
                    continue
                # get config
                jobChunks = []
                queueConfig = self.queueConfigMapper.getQueue(queueName)
                # actions based on mapping type
                if queueConfig.mapType == '0Jto1W':
                    # workers without jobs
                    jobChunks = []
                    for i in range(nWorkers):
                        jobChunks.append([])
                elif queueConfig.mapType == '1Jto1W':
                    # one woker per one job
                    jobChunks = self.dbProxy.getJobChunksForWorkers(queueName,
                                                                    nWorkers,1,None,
                                                                    queueConfig.useJobLateBinding,
                                                                    harvester_config.submitter.checkInterval,
                                                                    harvester_config.submitter.lockInterval,
                                                                    lockedBy)
                elif queueConfig.mapType == 'NJto1W':
                    # one worker for multiple jobs
                    # FIXME
                    pass
                elif queueConfig.mapType == '1JtoNW':
                    # multiple workers for one job
                    # FIXME
                    pass
                tmpLog.debug('got {0} job chunks'.format(len(jobChunks)))
                if len(jobChunks) == 0:
                    continue
                # make workers
                okChunks,ngChunks = self.workMaker.makeWorkers(jobChunks,queueConfig)
                tmpLog.debug('made {0} workers while {1} failed'.format(len(okChunks),len(ngChunks)))
                timeNow = datetime.datetime.utcnow()
                # NG
                for ngJobs in ngChunks:
                    for jobSpec in ngJobs:
                        jobSpec.status = 'failed'
                        jobSpec.subStatus = 'failedtosubmit'
                        jobSpec.stateChangeTime = timeNow
                        jobSpec.lockedBy = None
                        jobSpec.triggerPropagation()
                        self.dbProxy.updateJob(jobSpec,{'lockedBy':lockedBy,
                                                        'subStatus':'prepared'})
                # OK
                workerList = []
                for worker,okJobs in okChunks:
                    workerList.append(worker)
                if len(workerList) > 0:
                    # get plugin
                    submitterCore = self.pluginFactory.getPlugin(queueConfig.submitter)
                    if submitterCore == None:
                        # not found
                        tmpLog.error('plugin for {0} not found'.format(jobSpec.computingSite))
                        continue
                    # submit
                    tmpRetList,tmpStrList = submitterCore.submitWorkers(workerList)
                    for iWorker,(tmpRet,tmpStr) in enumerate(zip(tmpRetList,tmpStrList)):
                        worker,jobList = okChunks[iWorker]
                        # failed
                        if tmpRet == None:
                            # update jobs
                            for jobSpec in jobList:
                                jobSpec.status = 'failed'
                                jobSpec.subStatus = 'failedtosubmit'
                                jobSpec.stateChangeTime = timeNow
                                jobSpec.lockedBy = None
                                jobSpec.triggerPropagation()
                                self.dbProxy.updateJob(jobSpec,{'lockedBy':lockedBy,
                                                                'subStatus':'prepared'})
                                tmpLog.error('failed to submit a worker for PandaID={0}'.format(jobSpec.PandaID))
                            continue
                        # succeeded
                        worker.status = 'submitted'
                        worker.submitTime = timeNow
                        worker.stateChangeTime = timeNow
                        worker.modificationTime = timeNow
                        # register worker
                        tmpStat = self.dbProxy.registerWorker(worker,jobList,lockedBy)
                        for jobSpec in jobList:
                            if tmpStat:
                                tmpLog.debug('submtted a worker for PandaID={0} with batchID={1}'.format(jobSpec.PandaID,
                                                                                                         worker.batchID))
                            else:
                                tmpLog.error('failed to register a worker for PandaID={0} with batchID={1}'.format(jobSpec.PandaID,
                                                                                                                   worker.batchID))
            mainLog.debug('done')
            if self.singleMode:
                return
            # sleep
            CoreUtils.sleep(harvester_config.submitter.sleepTime)


    
