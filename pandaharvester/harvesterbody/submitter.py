import datetime

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterbody.worker_maker import WorkerMaker
from pandaharvester.harvesterbody.worker_adjuster import WorkerAdjuster

# logger
_logger = core_utils.setup_logger()


# class to submit workers
class Submitter(AgentBase):
    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.queueConfigMapper = queue_config_mapper
        self.dbProxy = DBProxy()
        self.workerMaker = WorkerMaker()
        self.workerAdjuster = WorkerAdjuster(queue_config_mapper)
        self.pluginFactory = PluginFactory()


    # main loop
    def run(self):
        lockedBy = 'submitter-{0}'.format(self.ident)
        while True:
            mainLog = core_utils.make_logger(_logger, 'id={0}'.format(lockedBy))
            mainLog.debug('getting queues to submit workers')
            # get queues associated to a site to submit workers
            curWorkersPerQueue, siteName = self.dbProxy.get_queues_to_submit(harvester_config.submitter.nQueues,
                                                                             harvester_config.submitter.lookupTime)
            mainLog.debug('got {0} queues for site {1}'.format(len(curWorkersPerQueue), siteName))
            # define number of new workers
            nWorkersPerQueue = self.workerAdjuster.define_num_workers(curWorkersPerQueue, siteName)
            # loop over all queues
            for queueName, tmpVal in nWorkersPerQueue.iteritems():
                tmpLog = core_utils.make_logger(_logger, 'queue={0}'.format(queueName))
                tmpLog.debug('start')
                nWorkers = tmpVal['nNewWorker'] + tmpVal['nReady']
                nReady = tmpVal['nReady']
                # check queue
                if not self.queueConfigMapper.has_queue(queueName):
                    tmpLog.error('config not found')
                    continue
                # get queue
                queueConfig = self.queueConfigMapper.get_queue(queueName)
                # actions based on mapping type
                if queueConfig.mapType == WorkSpec.MT_NoJob:
                    # workers without jobs
                    jobChunks = []
                    for i in range(nWorkers):
                        jobChunks.append([])
                elif queueConfig.mapType == WorkSpec.MT_OneToOne:
                    # one worker per one job
                    jobChunks = self.dbProxy.get_job_chunks_for_workers(queueName,
                                                                        nWorkers, nReady, 1, None,
                                                                        queueConfig.useJobLateBinding,
                                                                        harvester_config.submitter.checkInterval,
                                                                        harvester_config.submitter.lockInterval,
                                                                        lockedBy)
                elif queueConfig.mapType == WorkSpec.MT_MultiJobs:
                    # one worker for multiple jobs
                    nJobsPerWorker = self.workerMaker.get_num_jobs_per_worker(queueConfig)
                    jobChunks = self.dbProxy.get_job_chunks_for_workers(queueName,
                                                                        nWorkers, nReady, nJobsPerWorker, None,
                                                                        queueConfig.useJobLateBinding,
                                                                        harvester_config.submitter.checkInterval,
                                                                        harvester_config.submitter.lockInterval,
                                                                        lockedBy)
                elif queueConfig.mapType == WorkSpec.MT_MultiWorkers:
                    # multiple workers for one job
                    nWorkersPerJob = self.workerMaker.get_num_workers_per_job(queueConfig)
                    jobChunks = self.dbProxy.get_job_chunks_for_workers(queueName,
                                                                        nWorkers, nReady, None, nWorkersPerJob,
                                                                        queueConfig.useJobLateBinding,
                                                                        harvester_config.submitter.checkInterval,
                                                                        harvester_config.submitter.lockInterval,
                                                                        lockedBy)
                else:
                    tmpLog.error('unknown mapType={0}'.format(queueConfig.mapType))
                    continue
                tmpLog.debug('got {0} job chunks'.format(len(jobChunks)))
                if len(jobChunks) == 0:
                    continue
                # make workers
                okChunks, ngChunks = self.workerMaker.make_workers(jobChunks, queueConfig, nReady)
                tmpLog.debug('made {0} workers, while {1} workers failed'.format(len(okChunks), len(ngChunks)))
                timeNow = datetime.datetime.utcnow()
                # NG
                for ngJobs in ngChunks:
                    for jobSpec in ngJobs:
                        jobSpec.status = 'failed'
                        jobSpec.subStatus = 'failedtomake'
                        jobSpec.stateChangeTime = timeNow
                        jobSpec.lockedBy = None
                        jobSpec.trigger_propagation()
                        self.dbProxy.update_job(jobSpec, {'lockedBy': lockedBy,
                                                          'subStatus': 'prepared'})
                # OK
                workSpecList = []
                if len(okChunks) > 0:
                    for workSpec, okJobs in okChunks:
                        # has job
                        if (queueConfig.useJobLateBinding and workSpec.workerID is None) \
                                or queueConfig.mapType == WorkSpec.MT_NoJob:
                            workSpec.hasJob = 0
                        else:
                            workSpec.hasJob = 1
                            workSpec.set_jobspec_list(okJobs)
                        # map type
                        workSpec.mapType = queueConfig.mapType
                        # queue name
                        workSpec.computingSite = queueConfig.queueName
                        # set access point
                        workSpec.accessPoint = queueConfig.messenger['accessPoint']
                        # events
                        if len(okJobs) > 0 and (
                                'eventService' in okJobs[0].jobParams or 'cloneJob' in okJobs[0].jobParams):
                            workSpec.eventsRequest = WorkSpec.EV_useEvents
                        workSpecList.append(workSpec)
                if len(workSpecList) > 0:
                    # get plugin for submitter
                    submitterCore = self.pluginFactory.get_plugin(queueConfig.submitter)
                    if submitterCore is None:
                        # not found
                        tmpLog.error('submitter plugin for {0} not found'.format(jobSpec.computingSite))
                        continue
                    # get plugin for messenger
                    messenger = self.pluginFactory.get_plugin(queueConfig.messenger)
                    if messenger is None:
                        # not found
                        tmpLog.error('messenger plugin for {0} not found'.format(jobSpec.computingSite))
                        continue
                    # setup access points
                    messenger.setup_access_points(workSpecList)
                    # feed jobs
                    for workSpec in workSpecList:
                        if workSpec.hasJob == 1:
                            tmpStat = messenger.feed_jobs(workSpec, workSpec.get_jobspec_list())
                            tmpLog.debug('sent jobs to workerID={0} with {1}'.format(workSpec.workerID, tmpStat))
                    # submit
                    tmpLog.debug('submitting {0} workers'.format(len(workSpecList)))
                    workSpecList, tmpRetList, tmpStrList = self.submit_workers(submitterCore, workSpecList)
                    pandaIDs = set()
                    for iWorker, (tmpRet, tmpStr) in enumerate(zip(tmpRetList, tmpStrList)):
                        workSpec, jobList = okChunks[iWorker]
                        # failed
                        if tmpRet is None:
                            continue
                        # succeeded
                        if queueConfig.useJobLateBinding and workSpec.hasJob == 1:
                            workSpec.set_status(WorkSpec.ST_running)
                        else:
                            workSpec.set_status(WorkSpec.ST_submitted)
                        workSpec.submitTime = timeNow
                        workSpec.modificationTime = timeNow
                        # prefetch events
                        if workSpec.hasJob == 1 and workSpec.eventsRequest == WorkSpec.EV_useEvents:
                            workSpec.eventsRequest = WorkSpec.EV_requestEvents
                            eventsRequestParams = dict()
                            for jobSpec in jobList:
                                eventsRequestParams[jobSpec.PandaID] = {'pandaID': jobSpec.PandaID,
                                                                        'taskID': jobSpec.taskID,
                                                                        'jobsetID': jobSpec.jobParams['jobsetID'],
                                                                        'nRanges': jobSpec.jobParams['coreCount'],
                                                                        }
                            workSpec.eventsRequestParams = eventsRequestParams
                        # register worker
                        tmpStat = self.dbProxy.register_worker(workSpec, jobList, lockedBy)
                        for jobSpec in jobList:
                            pandaIDs.add(jobSpec.PandaID)
                            if tmpStat:
                                tmpLog.debug('submitted a workerID={0} for PandaID={1} with batchID={2}'.format(
                                    workSpec.workerID,
                                    jobSpec.PandaID,
                                    workSpec.batchID))
                            else:
                                tmpLog.error('failed to register a worker for PandaID={0} with batchID={1}'.format(
                                    jobSpec.PandaID,
                                    workSpec.batchID))
                    # release jobs
                    self.dbProxy.release_jobs(pandaIDs, lockedBy)
            mainLog.debug('done')
            # check if being terminated
            if self.terminated(harvester_config.submitter.sleepTime):
                mainLog.debug('terminated')
                return


    # wrapper for submitWorkers to skip ready workers
    def submit_workers(self, submitter_core, workspec_list):
        retList = []
        strList = []
        newSpecList = []
        workersToSubmit = []
        for workSpec in workspec_list:
            if workSpec.status == WorkSpec.ST_ready:
                newSpecList.append(workSpec)
                retList.append(True)
                strList.append('')
            else:
                workersToSubmit.append(workSpec)
        tmpRetList = submitter_core.submit_workers(workersToSubmit)
        for tmpRet, tmpStr in tmpRetList:
            retList.append(tmpRet)
            strList.append(tmpStr)
        newSpecList += workersToSubmit
        return newSpecList, retList, strList
