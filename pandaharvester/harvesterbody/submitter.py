import math
import datetime
import time
import socket
from future.utils import iteritems

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.command_spec import CommandSpec
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterbody.worker_maker import WorkerMaker
from pandaharvester.harvesterbody.worker_adjuster import WorkerAdjuster
from pandaharvester.harvestercore.pilot_errors import PilotErrors
from pandaharvester.harvestercore.fifos import MonitorFIFO
from pandaharvester.harvestermisc.apfmon import Apfmon

# logger
_logger = core_utils.setup_logger('submitter')


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
        self.monitor_fifo = MonitorFIFO()
        self.apfmon = Apfmon(self.queueConfigMapper)

    # main loop
    def run(self):
        lockedBy = 'submitter-{0}'.format(self.get_pid())
        monitor_fifo = self.monitor_fifo
        queueLockInterval = getattr(harvester_config.submitter, 'queueLockInterval',
                                    harvester_config.submitter.lockInterval)
        while True:
            sw_main = core_utils.get_stopwatch()
            mainLog = self.make_logger(_logger, 'id={0}'.format(lockedBy), method_name='run')
            mainLog.debug('getting queues to submit workers')

            # get queues associated to a site to submit workers
            curWorkers, siteName, resMap = self.dbProxy.get_queues_to_submit(harvester_config.submitter.nQueues,
                                                                             harvester_config.submitter.lookupTime,
                                                                             harvester_config.submitter.lockInterval,
                                                                             lockedBy, queueLockInterval)
            submitted = False
            if siteName is not None:
                mainLog.debug('got {0} queues for site {1}'.format(len(curWorkers), siteName))

                # get commands
                comStr = '{0}:{1}'.format(CommandSpec.COM_setNWorkers, siteName)
                commandSpecs = self.dbProxy.get_commands_for_receiver('submitter', comStr)
                mainLog.debug('got {0} {1} commands'.format(commandSpecs, comStr))
                for commandSpec in commandSpecs:
                    newLimits = self.dbProxy.set_queue_limit(siteName, commandSpec.params)
                    for tmpResource, tmpNewVal in iteritems(newLimits):
                        # if available, overwrite new worker value with the command from panda server
                        if tmpResource in resMap:
                            tmpQueueName = resMap[tmpResource]
                            if tmpQueueName in curWorkers:
                                curWorkers[tmpQueueName][tmpResource]['nNewWorkers'] = tmpNewVal

                # define number of new workers
                if len(curWorkers) == 0:
                    n_workers_per_queue_and_rt = dict()
                else:
                    n_workers_per_queue_and_rt = self.workerAdjuster.define_num_workers(curWorkers, siteName)

                if n_workers_per_queue_and_rt is None:
                    mainLog.error('WorkerAdjuster failed to define the number of workers')
                elif len(n_workers_per_queue_and_rt) == 0:
                    pass
                else:
                    # loop over all queues and resource types
                    for queueName in n_workers_per_queue_and_rt:
                        for resource_type, tmpVal in iteritems(n_workers_per_queue_and_rt[queueName]):

                            tmpLog = self.make_logger(_logger, 'id={0} queue={1} rtype={2}'.format(lockedBy,
                                                                                                   queueName,
                                                                                                   resource_type),
                                                      method_name='run')
                            try:
                                tmpLog.debug('start')
                                tmpLog.debug('workers status: %s' % tmpVal)
                                nWorkers = tmpVal['nNewWorkers'] + tmpVal['nReady']
                                nReady = tmpVal['nReady']

                                # check queue
                                if not self.queueConfigMapper.has_queue(queueName):
                                    tmpLog.error('config not found')
                                    continue

                                # no new workers
                                if nWorkers == 0:
                                    tmpLog.debug('skipped since no new worker is needed based on current stats')
                                    continue
                                # get queue
                                queueConfig = self.queueConfigMapper.get_queue(queueName)
                                workerMakerCore = self.workerMaker.get_plugin(queueConfig)
                                # check if resource is ready
                                if hasattr(workerMakerCore, 'dynamicSizing') and workerMakerCore.dynamicSizing is True:
                                    numReadyResources = self.workerMaker.num_ready_resources(queueConfig,
                                                                                             resource_type,
                                                                                             workerMakerCore)
                                    tmpLog.debug('numReadyResources: %s' % numReadyResources)
                                    if not numReadyResources:
                                        if hasattr(workerMakerCore, 'staticWorkers'):
                                            nQRWorkers = tmpVal['nQueue'] + tmpVal['nRunning']
                                            tmpLog.debug('staticWorkers: %s, nQRWorkers(Queue+Running): %s' %
                                                         (workerMakerCore.staticWorkers, nQRWorkers))
                                            if nQRWorkers >= workerMakerCore.staticWorkers:
                                                tmpLog.debug('No left static workers, skip')
                                                continue
                                            else:
                                                nWorkers = min(workerMakerCore.staticWorkers - nQRWorkers, nWorkers)
                                                tmpLog.debug('staticWorkers: %s, nWorkers: %s' %
                                                             (workerMakerCore.staticWorkers, nWorkers))
                                        else:
                                            tmpLog.debug('skip since no resources are ready')
                                            continue
                                    else:
                                        nWorkers = min(nWorkers, numReadyResources)
                                # post action of worker maker
                                if hasattr(workerMakerCore, 'skipOnFail') and workerMakerCore.skipOnFail is True:
                                    skipOnFail = True
                                else:
                                    skipOnFail = False
                                # actions based on mapping type
                                if queueConfig.mapType == WorkSpec.MT_NoJob:
                                    # workers without jobs
                                    jobChunks = []
                                    for i in range(nWorkers):
                                        jobChunks.append([])
                                elif queueConfig.mapType == WorkSpec.MT_OneToOne:
                                    # one worker per one job
                                    jobChunks = self.dbProxy.get_job_chunks_for_workers(
                                        queueName,
                                        nWorkers, nReady, 1, None,
                                        queueConfig.useJobLateBinding,
                                        harvester_config.submitter.checkInterval,
                                        harvester_config.submitter.lockInterval,
                                        lockedBy)
                                elif queueConfig.mapType == WorkSpec.MT_MultiJobs:
                                    # one worker for multiple jobs
                                    nJobsPerWorker = self.workerMaker.get_num_jobs_per_worker(queueConfig,
                                                                                              nWorkers,
                                                                                              resource_type,
                                                                                              maker=workerMakerCore)
                                    tmpLog.debug('nJobsPerWorker={0}'.format(nJobsPerWorker))
                                    jobChunks = self.dbProxy.get_job_chunks_for_workers(
                                        queueName,
                                        nWorkers, nReady, nJobsPerWorker, None,
                                        queueConfig.useJobLateBinding,
                                        harvester_config.submitter.checkInterval,
                                        harvester_config.submitter.lockInterval,
                                        lockedBy,
                                        queueConfig.allowJobMixture)
                                elif queueConfig.mapType == WorkSpec.MT_MultiWorkers:
                                    # multiple workers for one job
                                    nWorkersPerJob = self.workerMaker.get_num_workers_per_job(queueConfig,
                                                                                              nWorkers,
                                                                                              resource_type,
                                                                                              maker=workerMakerCore)
                                    maxWorkersPerJob = self.workerMaker.get_max_workers_per_job_in_total(
                                        queueConfig, resource_type, maker=workerMakerCore)
                                    maxWorkersPerJobPerCycle = self.workerMaker.get_max_workers_per_job_per_cycle(
                                        queueConfig, resource_type, maker=workerMakerCore)
                                    tmpLog.debug('nWorkersPerJob={0}'.format(nWorkersPerJob))
                                    jobChunks = self.dbProxy.get_job_chunks_for_workers(
                                        queueName,
                                        nWorkers, nReady, None, nWorkersPerJob,
                                        queueConfig.useJobLateBinding,
                                        harvester_config.submitter.checkInterval,
                                        harvester_config.submitter.lockInterval,
                                        lockedBy, max_workers_per_job_in_total=maxWorkersPerJob,
                                        max_workers_per_job_per_cycle=maxWorkersPerJobPerCycle)
                                else:
                                    tmpLog.error('unknown mapType={0}'.format(queueConfig.mapType))
                                    continue

                                tmpLog.debug('got {0} job chunks'.format(len(jobChunks)))
                                if len(jobChunks) == 0:
                                    continue
                                # make workers
                                okChunks, ngChunks = self.workerMaker.make_workers(jobChunks, queueConfig,
                                                                                   nReady, resource_type,
                                                                                   maker=workerMakerCore)
                                if len(ngChunks) == 0:
                                    tmpLog.debug('successfully made {0} workers'.format(len(okChunks)))
                                else:
                                    tmpLog.debug('made {0} workers, while {1} workers failed'.format(len(okChunks),
                                                                                                     len(ngChunks)))
                                timeNow = datetime.datetime.utcnow()
                                timeNow_timestamp = time.time()
                                pandaIDs = set()
                                # NG (=not good)
                                for ngJobs in ngChunks:
                                    for jobSpec in ngJobs:
                                        if skipOnFail:
                                            # release jobs when workers are not made
                                            pandaIDs.add(jobSpec.PandaID)
                                        else:
                                            jobSpec.status = 'failed'
                                            jobSpec.subStatus = 'failed_to_make'
                                            jobSpec.stateChangeTime = timeNow
                                            jobSpec.lockedBy = None
                                            errStr = 'failed to make a worker'
                                            jobSpec.set_pilot_error(PilotErrors.ERR_SETUPFAILURE, errStr)
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
                                            if workSpec.nJobsToReFill in [None, 0]:
                                                workSpec.set_jobspec_list(okJobs)
                                            else:
                                                # refill free slots during the worker is running
                                                workSpec.set_jobspec_list(okJobs[:workSpec.nJobsToReFill])
                                                workSpec.nJobsToReFill = None
                                                for jobSpec in okJobs[workSpec.nJobsToReFill:]:
                                                    pandaIDs.add(jobSpec.PandaID)
                                            workSpec.set_num_jobs_with_list()
                                        # map type
                                        workSpec.mapType = queueConfig.mapType
                                        # queue name
                                        workSpec.computingSite = queueConfig.queueName
                                        # set access point
                                        workSpec.accessPoint = queueConfig.messenger['accessPoint']
                                        # sync level
                                        workSpec.syncLevel = queueConfig.get_synchronization_level()
                                        # events
                                        if len(okJobs) > 0 and \
                                                ('eventService' in okJobs[0].jobParams or
                                                 'cloneJob' in okJobs[0].jobParams):
                                            workSpec.eventsRequest = WorkSpec.EV_useEvents
                                        workSpecList.append(workSpec)
                                if len(workSpecList) > 0:
                                    sw = core_utils.get_stopwatch()
                                    # get plugin for submitter
                                    submitterCore = self.pluginFactory.get_plugin(queueConfig.submitter)
                                    if submitterCore is None:
                                        # not found
                                        tmpLog.error(
                                            'submitter plugin for {0} not found'.format(jobSpec.computingSite))
                                        continue
                                    # get plugin for messenger
                                    messenger = self.pluginFactory.get_plugin(queueConfig.messenger)
                                    if messenger is None:
                                        # not found
                                        tmpLog.error(
                                            'messenger plugin for {0} not found'.format(jobSpec.computingSite))
                                        continue
                                    # setup access points
                                    messenger.setup_access_points(workSpecList)
                                    # feed jobs
                                    for workSpec in workSpecList:
                                        if workSpec.hasJob == 1:
                                            tmpStat = messenger.feed_jobs(workSpec, workSpec.get_jobspec_list())
                                            if tmpStat is False:
                                                tmpLog.error(
                                                    'failed to send jobs to workerID={0}'.format(workSpec.workerID))
                                            else:
                                                tmpLog.debug(
                                                    'sent jobs to workerID={0} with {1}'.format(workSpec.workerID,
                                                                                                tmpStat))
                                    # insert workers
                                    self.dbProxy.insert_workers(workSpecList, lockedBy)
                                    # submit
                                    sw.reset()
                                    tmpLog.info('submitting {0} workers'.format(len(workSpecList)))
                                    workSpecList, tmpRetList, tmpStrList = self.submit_workers(submitterCore,
                                                                                               workSpecList)
                                    tmpLog.debug('done submitting {0} workers'.format(len(workSpecList))
                                                    + sw.get_elapsed_time())
                                    # collect successful jobs
                                    okPandaIDs = set()
                                    for iWorker, (tmpRet, tmpStr) in enumerate(zip(tmpRetList, tmpStrList)):
                                        if tmpRet:
                                            workSpec, jobList = okChunks[iWorker]
                                            jobList = workSpec.get_jobspec_list()
                                            if jobList is not None:
                                                for jobSpec in jobList:
                                                    okPandaIDs.add(jobSpec.PandaID)
                                    # loop over all workers
                                    for iWorker, (tmpRet, tmpStr) in enumerate(zip(tmpRetList, tmpStrList)):
                                        workSpec, jobList = okChunks[iWorker]
                                        # set harvesterHost
                                        workSpec.harvesterHost = socket.gethostname()
                                        # use associated job list since it can be truncated for re-filling
                                        jobList = workSpec.get_jobspec_list()
                                        # set status
                                        if not tmpRet:
                                            # failed submission
                                            errStr = 'failed to submit a workerID={0} with {1}'.format(
                                                workSpec.workerID,
                                                tmpStr)
                                            tmpLog.error(errStr)
                                            workSpec.set_status(WorkSpec.ST_missed)
                                            workSpec.set_dialog_message(tmpStr)
                                            workSpec.set_pilot_error(PilotErrors.ERR_SETUPFAILURE, errStr)
                                            if jobList is not None:
                                                # increment attempt number
                                                newJobList = []
                                                for jobSpec in jobList:
                                                    # skip if successful with another worker
                                                    if jobSpec.PandaID in okPandaIDs:
                                                        continue
                                                    if jobSpec.submissionAttempts is None:
                                                        jobSpec.submissionAttempts = 0
                                                    jobSpec.submissionAttempts += 1
                                                    # max attempt or permanent error
                                                    if tmpRet is False or \
                                                            jobSpec.submissionAttempts >= \
                                                            queueConfig.maxSubmissionAttempts:
                                                        newJobList.append(jobSpec)
                                                    else:
                                                        self.dbProxy.increment_submission_attempt(
                                                            jobSpec.PandaID,
                                                            jobSpec.submissionAttempts)
                                                jobList = newJobList
                                        elif queueConfig.useJobLateBinding and workSpec.hasJob == 1:
                                            # directly go to running after feeding jobs for late biding
                                            workSpec.set_status(WorkSpec.ST_running)
                                        else:
                                            # normal successful submission
                                            workSpec.set_status(WorkSpec.ST_submitted)
                                        workSpec.submitTime = timeNow
                                        workSpec.modificationTime = timeNow
                                        workSpec.checkTime = timeNow
                                        if self.monitor_fifo.enabled:
                                            workSpec.set_work_params({'lastCheckAt': timeNow_timestamp})
                                        # prefetch events
                                        if tmpRet and workSpec.hasJob == 1 and \
                                                workSpec.eventsRequest == WorkSpec.EV_useEvents and \
                                                queueConfig.prefetchEvents:
                                            workSpec.eventsRequest = WorkSpec.EV_requestEvents
                                            eventsRequestParams = dict()
                                            for jobSpec in jobList:
                                                eventsRequestParams[jobSpec.PandaID] = \
                                                    {'pandaID': jobSpec.PandaID,
                                                     'taskID': jobSpec.taskID,
                                                     'jobsetID': jobSpec.jobParams['jobsetID'],
                                                     'nRanges': max(int(math.ceil(workSpec.nCore / len(jobList))),
                                                                    jobSpec.jobParams['coreCount']),
                                                     }
                                            workSpec.eventsRequestParams = eventsRequestParams
                                        # register worker
                                        tmpStat = self.dbProxy.register_worker(workSpec, jobList, lockedBy)
                                        if jobList is not None:
                                            for jobSpec in jobList:
                                                pandaIDs.add(jobSpec.PandaID)
                                                if tmpStat:
                                                    if tmpRet:
                                                        tmpStr = \
                                                            'submitted a workerID={0} for PandaID={1} with batchID={2}'
                                                        tmpLog.info(tmpStr.format(workSpec.workerID,
                                                                                  jobSpec.PandaID,
                                                                                  workSpec.batchID))
                                                    else:
                                                        tmpStr = 'failed to submit a workerID={0} for PandaID={1}'
                                                        tmpLog.error(tmpStr.format(workSpec.workerID,
                                                                                   jobSpec.PandaID))
                                                else:
                                                    tmpStr = \
                                                        'failed to register a worker for PandaID={0} with batchID={1}'
                                                    tmpLog.error(tmpStr.format(jobSpec.PandaID, workSpec.batchID))
                                    # enqueue to monitor fifo
                                    if self.monitor_fifo.enabled \
                                            and queueConfig.mapType != WorkSpec.MT_MultiWorkers:
                                        workSpecsToEnqueue = \
                                            [[w] for w in workSpecList if w.status
                                             in (WorkSpec.ST_submitted, WorkSpec.ST_running)]
                                        monitor_fifo.put((queueName, workSpecsToEnqueue),
                                                            time.time() + harvester_config.monitor.fifoCheckInterval)
                                        mainLog.debug('put workers to monitor FIFO')
                                    submitted = True
                                # release jobs
                                self.dbProxy.release_jobs(pandaIDs, lockedBy)
                                tmpLog.info('done')
                            except Exception:
                                core_utils.dump_error_message(tmpLog)
                # release the site
                self.dbProxy.release_site(siteName, lockedBy)
            mainLog.debug('done')
            # define sleep interval
            if siteName is None:
                sleepTime = harvester_config.submitter.sleepTime
            else:
                sleepTime = 0
                if submitted and hasattr(harvester_config.submitter, 'minSubmissionInterval'):
                    interval = harvester_config.submitter.minSubmissionInterval
                    if interval > 0:
                        newTime = datetime.datetime.utcnow() + datetime.timedelta(seconds=interval)
                        self.dbProxy.update_panda_queue_attribute('submitTime', newTime, site_name=siteName)

            # time the cycle
            mainLog.debug('done a submitter cycle' + sw_main.get_elapsed_time())
            # check if being terminated
            if self.terminated(sleepTime):
                mainLog.debug('terminated')
                return

    # wrapper for submitWorkers to skip ready workers
    def submit_workers(self, submitter_core, workspec_list):
        retList = []
        strList = []
        newSpecList = []
        workersToSubmit = []
        for workSpec in workspec_list:
            if workSpec.status in [WorkSpec.ST_ready, WorkSpec.ST_running]:
                newSpecList.append(workSpec)
                retList.append(True)
                strList.append('')
            else:
                workersToSubmit.append(workSpec)
        tmpRetList = submitter_core.submit_workers(workersToSubmit)

        # submit the workers to the monitoring
        self.apfmon.create_workers(workersToSubmit)

        for tmpRet, tmpStr in tmpRetList:
            retList.append(tmpRet)
            strList.append(tmpStr)
        newSpecList += workersToSubmit
        return newSpecList, retList, strList
