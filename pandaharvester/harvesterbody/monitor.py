from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvesterbody.agent_base import AgentBase

# logger
_logger = core_utils.setup_logger()


# propagate important checkpoints to panda
class Monitor(AgentBase):
    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.queueConfigMapper = queue_config_mapper
        self.dbProxy = DBProxy()
        self.pluginFactory = PluginFactory()

    # main loop
    def run(self):
        lockedBy = 'monitor-{0}'.format(self.ident)
        # init messengers
        for queueConfig in self.queueConfigMapper.get_all_queues().values():
            # just import for module initialization
            self.pluginFactory.get_plugin(queueConfig.messenger)
        # main
        while True:
            mainLog = core_utils.make_logger(_logger, 'id={0}'.format(lockedBy))
            mainLog.debug('getting workers to monitor')
            workSpecsPerQueue = self.dbProxy.get_workers_to_update(harvester_config.monitor.maxWorkers,
                                                                   harvester_config.monitor.checkInterval,
                                                                   harvester_config.monitor.lockInterval,
                                                                   lockedBy)
            mainLog.debug('got {0} queues'.format(len(workSpecsPerQueue)))
            # loop over all workers
            for queueName, workSpecsList in workSpecsPerQueue.iteritems():
                tmpQueLog = core_utils.make_logger(_logger, 'queue={0}'.format(queueName))
                # check queue
                if not self.queueConfigMapper.has_queue(queueName):
                    tmpQueLog.error('config not found')
                    continue
                # get queue
                queueConfig = self.queueConfigMapper.get_queue(queueName)
                # get plugins
                monCore = self.pluginFactory.get_plugin(queueConfig.monitor)
                messenger = self.pluginFactory.get_plugin(queueConfig.messenger)
                # check workers
                allWorkers = [item for sublist in workSpecsList for item in sublist]
                tmpQueLog.debug('checking {0} workers'.format(len(allWorkers)))
                tmpRetMap = self.check_workers(monCore, messenger, allWorkers, queueConfig, tmpQueLog)
                # loop over all worker chunks
                iWorker = 0
                for workSpecs in workSpecsList:
                    jobSpecs = None
                    filesToStageOut = dict()
                    pandaIDsList = []
                    eventsToUpdateList = []
                    filesToStageOutList = []
                    for workSpec in workSpecs:
                        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workSpec.workerID))
                        tmpOut = tmpRetMap[workSpec.workerID]
                        newStatus = tmpOut['newStatus']
                        monStatus = tmpOut['monStatus']
                        diagMessage = tmpOut['diagMessage']
                        workAttributes = tmpOut['workAttributes']
                        eventsToUpdate = tmpOut['eventsToUpdate']
                        filesToStageOut = tmpOut['filesToStageOut']
                        eventsRequestParams = tmpOut['eventsRequestParams']
                        pandaIDs = tmpOut['pandaIDs']
                        tmpLog.debug('newStatus={0} monitoredStatus={1} diag={2}'.format(newStatus,
                                                                                         monStatus,
                                                                                         diagMessage))
                        iWorker += 1
                        # check status
                        if newStatus not in WorkSpec.ST_LIST:
                            tmpLog.error('unknown status={0}'.format(newStatus))
                            continue
                        # update worker
                        workSpec.set_status(newStatus)
                        workSpec.workAttributes = workAttributes
                        # request events
                        if eventsRequestParams != {}:
                            workSpec.eventsRequest = WorkSpec.EV_requestEvents
                            workSpec.eventsRequestParams = eventsRequestParams
                        # get associated jobs for the worker chunk
                        if workSpec.hasJob == 1 and jobSpecs is None:
                            jobSpecs = self.dbProxy.get_jobs_with_worker_id(workSpec.workerID,
                                                                            lockedBy,
                                                                            only_running=True)
                        # pandaIDs for push
                        pandaIDsList.append(pandaIDs)
                        if len(eventsToUpdate) > 0:
                            eventsToUpdateList.append(eventsToUpdate)
                        if len(filesToStageOut) > 0:
                            filesToStageOutList.append(filesToStageOut)
                    # update jobs and workers
                    if jobSpecs is not None:
                        tmpQueLog.debug('updating {0} jobs with {1} workers'.format(len(jobSpecs), len(workSpecs)))
                        core_utils.update_job_attributes_with_workers(queueConfig.mapType, jobSpecs, workSpecs,
                                                                      filesToStageOutList, eventsToUpdateList)
                        for jobSpec in jobSpecs:
                            tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(jobSpec.PandaID))
                            tmpLog.debug('new status={0} subStatus={1}'.format(jobSpec.status, jobSpec.subStatus))
                    # update local database
                    self.dbProxy.update_jobs_workers(jobSpecs, workSpecs, lockedBy, pandaIDsList)
                    # send ACK to workers for events and files
                    if len(eventsToUpdateList) > 0 or len(filesToStageOutList) > 0:
                        for workSpec in workSpecs:
                            messenger.acknowledge_events_files(workSpec)
                tmpQueLog.debug('done')
            mainLog.debug('done')
            # check if being terminated
            if self.terminated(harvester_config.monitor.sleepTime):
                mainLog.debug('terminated')
                return


    # wrapper for checkWorkers
    def check_workers(self, mon_core, messenger, all_workers, queue_config, tmp_log):
        workersToCheck = []
        retMap = dict()
        for workSpec in all_workers:
            eventsRequestParams = {}
            eventsToUpdate = []
            pandaIDs = []
            workAttributes = None
            filesToStageOut = None
            # job-level late binding
            if workSpec.hasJob == 0 and queue_config.mapType != WorkSpec.MT_NoJob:
                # check if job is requested
                jobRequested = messenger.job_requested(workSpec)
                if jobRequested:
                    # set ready when job is requested 
                    workStatus = WorkSpec.ST_ready
                else:
                    workStatus = workSpec.status
            else:
                workStatus = None
                workersToCheck.append(workSpec)
            # add
            retMap[workSpec.workerID] = {'newStatus': workStatus,
                                         'monStatus': workStatus,
                                         'workAttributes': workAttributes,
                                         'filesToStageOut': filesToStageOut,
                                         'eventsRequestParams': eventsRequestParams,
                                         'eventsToUpdate': eventsToUpdate,
                                         'diagMessage': '',
                                         'pandaIDs': pandaIDs}
        # check workers
        tmpStat, tmpOut = mon_core.check_workers(workersToCheck)
        if not tmpStat:
            tmp_log.error('failed to check workers with {0}'.format(tmpOut))
        else:
            for workSpec, (newStatus, diagMessage) in zip(workersToCheck, tmpOut):
                workerID = workSpec.workerID
                if workerID in retMap:
                    # get work attributes and output files
                    workAttributes = messenger.get_work_attributes(workSpec)
                    retMap[workerID]['workAttributes'] = workAttributes
                    filesToStageOut = messenger.get_files_to_stage_out(workSpec)
                    retMap[workerID]['filesToStageOut'] = filesToStageOut
                    # get events to update
                    if workSpec.eventsRequest in [WorkSpec.EV_useEvents, WorkSpec.EV_requestEvents]:
                        eventsToUpdate = messenger.events_to_update(workSpec)
                        retMap[workerID]['eventsToUpdate'] = eventsToUpdate
                    # request events
                    if workSpec.eventsRequest == WorkSpec.EV_useEvents:
                        eventsRequestParams = messenger.events_requested(workSpec)
                        retMap[workerID]['eventsRequestParams'] = eventsRequestParams
                    # get PandaIDs for pull model
                    if queue_config.mapType == WorkSpec.MT_NoJob:
                        pandaIDs = messenger.get_panda_ids(workSpec)
                    retMap[workerID]['pandaIDs'] = pandaIDs
                    # keep original new status
                    retMap[workerID]['monStatus'] = newStatus
                    # set running while there are events to update or files to stage out
                    if newStatus in [WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled]:
                        if len(retMap[workerID]['filesToStageOut']) > 0 or \
                                        len(retMap[workerID]['eventsToUpdate']) > 0:
                            newStatus = WorkSpec.ST_running
                        elif not workSpec.is_post_processed():
                            if not queue_config.is_no_heartbeat_status(newStatus):
                                # post processing unless heartbeat is suppressed
                                jobSpecs = self.dbProxy.get_jobs_with_worker_id(workSpec.workerID,
                                                                                None, True,
                                                                                only_running=True)
                                # post processing
                                messenger.post_processing(workSpec, jobSpecs, queue_config.mapType)
                            workSpec.post_processed()
                            newStatus = WorkSpec.ST_running
                        # reset modification time to immediately trigger subsequent lookup
                        workSpec.trigger_next_lookup()
                    retMap[workerID]['newStatus'] = newStatus
                    retMap[workerID]['diagMessage'] = diagMessage
        return retMap
