import threading

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy import DBProxy
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_factory import PluginFactory

# logger
_logger = core_utils.setup_logger()


# propagate important checkpoints to panda
class Monitor(threading.Thread):
    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        threading.Thread.__init__(self)
        self.queueConfigMapper = queue_config_mapper
        self.dbProxy = DBProxy()
        self.singleMode = single_mode
        self.pluginFactory = PluginFactory()

    # main loop
    def run(self):
        lockedBy = 'monitor-{0}'.format(self.ident)
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
                tmpRetMap = self.check_workers(monCore, messenger, allWorkers, tmpQueLog)
                # loop over all worker chunks
                iWorker = 0
                for workSpecs in workSpecsList:
                    jobSpecs = None
                    filesToStageOut = dict()
                    for workSpec in workSpecs:
                        tmpLog = core_utils.make_logger(_logger, 'workID={0}'.format(workSpec.workerID))
                        tmpOut = tmpRetMap[workSpec.workerID]
                        newStatus = tmpOut['newStatus']
                        diagMessage = tmpOut['diagMessage']
                        workAttributes = tmpOut['workAttributes']
                        eventsToUpdate = tmpOut['eventsToUpdate']
                        filesToStageOut = tmpOut['filesToStageOut']
                        eventsRequestParams = tmpOut['eventsRequestParams']
                        tmpLog.debug('newStatus={0} diag={1}'.format(newStatus, diagMessage))
                        iWorker += 1
                        # check status
                        if newStatus not in WorkSpec.ST_LIST:
                            tmpLog.error('unknown status={0}'.format(newStatus))
                            continue
                        # update worker
                        workSpec.status = newStatus
                        workSpec.workAttributes = workAttributes
                        # request events
                        if eventsRequestParams != {}:
                            workSpec.eventsRequest = WorkSpec.EV_requestEvents
                            workSpec.eventsRequestParams = eventsRequestParams
                        # get associated jobs for the worker chunk
                        if workSpec.hasJob == 1 and jobSpecs is None:
                            jobSpecs = self.dbProxy.get_jobs_with_worker_id(workSpec.workerID,
                                                                            lockedBy)
                    # update jobs and workers
                    if jobSpecs is not None:
                        tmpQueLog.debug('update {0} jobs with {1} workers'.format(len(jobSpecs), len(workSpecs)))
                        messenger.update_job_attributes_with_workers(queueConfig.mapType, jobSpecs, workSpecs,
                                                                     filesToStageOut, eventsToUpdate)
                    # update local database
                    self.dbProxy.update_jobs_workers(jobSpecs, workSpecs, lockedBy)
                    # send ACK to workers for events and files
                    if eventsToUpdate != [] or filesToStageOut != {}:
                        for workSpec in workSpecs:
                            messenger.acknowledge_events_files(workSpec)
                tmpQueLog.debug('done')
            mainLog.debug('done')
            if self.singleMode:
                return
            # sleep
            core_utils.sleep(harvester_config.monitor.sleepTime)

    # wrapper for checkWorkers
    def check_workers(self, mon_core, messenger, all_workers, tmp_log):
        workersToCheck = []
        retMap = {}
        for workSpec in all_workers:
            eventsRequestParams = {}
            eventsToUpdate = []
            # job-level late binding
            if workSpec.hasJob == 0:
                # check if job is requested
                jobRequested = messenger.job_requested(workSpec)
                if jobRequested:
                    # set ready when job is requested 
                    workStatus = WorkSpec.ST_ready
                else:
                    workStatus = workSpec.status
                workAttributes = None
                filesToStageOut = None
            else:
                workStatus = None
                workersToCheck.append(workSpec)
                # get events to update
                if workSpec.eventsRequest in [WorkSpec.EV_useEvents, WorkSpec.EV_requestEvents]:
                    eventsToUpdate = messenger.events_to_update(workSpec)
                # request events
                if workSpec.eventsRequest == WorkSpec.EV_useEvents:
                    eventsRequestParams = messenger.events_requested(workSpec)
                # get work attributes and output files
                workAttributes = messenger.get_work_attributes(workSpec)
                filesToStageOut = messenger.get_files_to_stage_out(workSpec)
            # add
            retMap[workSpec.workerID] = {'newStatus': workStatus,
                                         'workAttributes': workAttributes,
                                         'filesToStageOut': filesToStageOut,
                                         'eventsRequestParams': eventsRequestParams,
                                         'eventsToUpdate': eventsToUpdate,
                                         'diagMessage': ''}
        # check workers
        tmpStat, tmpOut = mon_core.check_workers(workersToCheck)
        if not tmpStat:
            tmp_log.error('failed to check workers with {0}'.format(tmpOut))
        else:
            for workSpec, (newStatus, diagMessage) in zip(workersToCheck, tmpOut):
                workerID = workSpec.workerID
                if workerID in retMap:
                    # set running while there are events to update or files to stage out
                    if len(retMap[workerID]['filesToStageOut']) > 0 or \
                                    len(retMap[workerID]['eventsToUpdate']) > 0:
                        newStatus = WorkSpec.ST_running
                    retMap[workerID]['newStatus'] = newStatus
                    retMap[workerID]['diagMessage'] = diagMessage
        return retMap
