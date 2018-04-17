import time
from future.utils import iteritems

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvestercore.pilot_errors import PilotErrors
from pandaharvester.harvestercore.fifos import MonitorFIFO

# logger
_logger = core_utils.setup_logger('monitor')


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
        last_time_run_with_DB = 0
        while True:
            sw = core_utils.get_stopwatch()
            mainLog = self.make_logger(_logger, 'id={0}'.format(lockedBy), method_name='run')

            # run with workers from FIFO
            mainLog.debug('starting run with FIFO')
            mq = MonitorFIFO()
            mainLog.debug('getting workers to monitor')
            try:
                queueName, workSpecsList = mq.get()
            except Exception as errStr:
                mainLog.error('failed to get object from FIFO: {0}'.format(errStr))
            else:
                mainLog.debug('got {0} workers of {1}'.format(len(workSpecsList), queueName)
                self.monitor_agent_core(lockedBy, queueName, workSpecsList)
            mainLog.debug('ended run with FIFO')

            # run with workers from DB
            if time.time() >= last_time_run_with_DB + harvester_config.monitor.sleepTime:
                mainLog.debug('starting run with DB')
                mainLog.debug('getting workers to monitor')
                workSpecsPerQueue = self.dbProxy.get_workers_to_update(harvester_config.monitor.maxWorkers,
                                                                       harvester_config.monitor.checkInterval,
                                                                       harvester_config.monitor.lockInterval,
                                                                       lockedBy)
                mainLog.debug('got {0} queues'.format(len(workSpecsPerQueue)))
                # loop over all workers
                for queueName, workSpecsList in iteritems(workSpecsPerQueue):
                    self.monitor_agent_core(lockedBy, queueName, workSpecsList)
                last_time_run_with_DB = time.time()
                mainLog.debug('ended run with DB')

            if sw.get_elapsed_time_in_sec() > harvester_config.monitor.lockInterval:
                mainLog.warning('a single cycle was longer than lockInterval ' + sw.get_elapsed_time())
            else:
                mainLog.debug('done' + sw.get_elapsed_time())

            # check if being terminated
            if self.terminated(harvester_config.monitor_fifo.sleepTime):
                mainLog.debug('terminated')
                return


    # core of monitor agent to check workers in workSpecsList of queueName
    def monitor_agent_core(self, lockedBy, queueName, workSpecsList):
        tmpQueLog = self.make_logger(_logger, 'id={0} queue={1}'.format(lockedBy, queueName),
                                     method_name='run')
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
        tmpStat, tmpRetMap = self.check_workers(monCore, messenger, allWorkers, queueConfig, tmpQueLog)
        if tmpStat:
            # loop over all worker chunks
            tmpQueLog.debug('update jobs and workers')
            iWorker = 0
            for workSpecs in workSpecsList:
                jobSpecs = None
                filesToStageOut = dict()
                pandaIDsList = []
                eventsToUpdateList = []
                filesToStageOutList = []
                mapType = workSpecs[0].mapType
                for workSpec in workSpecs:
                    tmpLog = self.make_logger(_logger,
                                              'id={0} workerID={1}'.format(lockedBy, workSpec.workerID),
                                              method_name='run')
                    tmpOut = tmpRetMap[workSpec.workerID]
                    newStatus = tmpOut['newStatus']
                    monStatus = tmpOut['monStatus']
                    diagMessage = tmpOut['diagMessage']
                    workAttributes = tmpOut['workAttributes']
                    eventsToUpdate = tmpOut['eventsToUpdate']
                    filesToStageOut = tmpOut['filesToStageOut']
                    eventsRequestParams = tmpOut['eventsRequestParams']
                    nJobsToReFill = tmpOut['nJobsToReFill']
                    pandaIDs = tmpOut['pandaIDs']
                    tmpStr = 'newStatus={0} monitoredStatus={1} diag={2} '
                    tmpStr += 'postProcessed={3} files={4}'
                    tmpLog.debug(tmpStr.format(newStatus, monStatus, diagMessage,
                                               workSpec.is_post_processed(),
                                               str(filesToStageOut)))
                    iWorker += 1
                    # check status
                    if newStatus not in WorkSpec.ST_LIST:
                        tmpLog.error('unknown status={0}'.format(newStatus))
                        continue
                    # update worker
                    workSpec.set_status(newStatus)
                    workSpec.set_work_attributes(workAttributes)
                    workSpec.set_dialog_message(diagMessage)
                    if monStatus == WorkSpec.ST_failed:
                        if not workSpec.has_pilot_error():
                            workSpec.set_pilot_error(PilotErrors.ERR_GENERALERROR, diagMessage)
                    elif monStatus == WorkSpec.ST_cancelled:
                        if not workSpec.has_pilot_error():
                            workSpec.set_pilot_error(PilotErrors.ERR_PANDAKILL, diagMessage)
                    # request events
                    if eventsRequestParams != {}:
                        workSpec.eventsRequest = WorkSpec.EV_requestEvents
                        workSpec.eventsRequestParams = eventsRequestParams
                    # jobs to refill
                    if nJobsToReFill is not None:
                        workSpec.nJobsToReFill = nJobsToReFill
                    # get associated jobs for the worker chunk
                    if workSpec.hasJob == 1 and jobSpecs is None:
                        jobSpecs = self.dbProxy.get_jobs_with_worker_id(workSpec.workerID,
                                                                        None,
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
                    core_utils.update_job_attributes_with_workers(mapType, jobSpecs, workSpecs,
                                                                  filesToStageOutList, eventsToUpdateList)
                    for jobSpec in jobSpecs:
                        tmpLog = self.make_logger(_logger,
                                                  'id={0} PandaID={1}'.format(lockedBy, jobSpec.PandaID),
                                                  method_name='run')
                        tmpLog.debug('new status={0} subStatus={1} status_in_metadata={2}'.format(
                            jobSpec.status,
                            jobSpec.subStatus,
                            jobSpec.get_job_status_from_attributes()))
                # update local database
                tmpRet = self.dbProxy.update_jobs_workers(jobSpecs, workSpecs, lockedBy, pandaIDsList)
                if not tmpRet:
                    for workSpec in workSpecs:
                        tmpLog = self.make_logger(_logger,
                                                  'id={0} workerID={1}'.format(lockedBy, workSpec.workerID),
                                                  method_name='run')
                        tmpLog.error('failed to update the DB. lockInterval may be too short')
                        sendWarning = True
                # send ACK to workers for events and files
                if len(eventsToUpdateList) > 0 or len(filesToStageOutList) > 0:
                    for workSpec in workSpecs:
                        messenger.acknowledge_events_files(workSpec)
        else:
            tmpQueLog.error('failed to check workers')
        tmpQueLog.debug('done')


    # wrapper for checkWorkers
    def check_workers(self, mon_core, messenger, all_workers, queue_config, tmp_log):
        workersToCheck = []
        retMap = dict()
        for workSpec in all_workers:
            eventsRequestParams = {}
            eventsToUpdate = []
            pandaIDs = []
            workStatus = None
            workAttributes = None
            filesToStageOut = []
            nJobsToReFill = None
            # job-level late binding
            if workSpec.hasJob == 0 and workSpec.mapType != WorkSpec.MT_NoJob:
                # check if job is requested
                jobRequested = messenger.job_requested(workSpec)
                if jobRequested:
                    # set ready when job is requested
                    workStatus = WorkSpec.ST_ready
                else:
                    workStatus = workSpec.status
            elif workSpec.nJobsToReFill in [0, None]:
                # check if job is requested to refill free slots
                jobRequested = messenger.job_requested(workSpec)
                if jobRequested:
                    nJobsToReFill = jobRequested
                workersToCheck.append(workSpec)
            else:
                workersToCheck.append(workSpec)
            # add
            retMap[workSpec.workerID] = {'newStatus': workStatus,
                                         'monStatus': workStatus,
                                         'workAttributes': workAttributes,
                                         'filesToStageOut': filesToStageOut,
                                         'eventsRequestParams': eventsRequestParams,
                                         'eventsToUpdate': eventsToUpdate,
                                         'diagMessage': '',
                                         'pandaIDs': pandaIDs,
                                         'nJobsToReFill': nJobsToReFill}
        # check workers
        tmp_log.debug('checking workers with plugin')
        try:
            tmpStat, tmpOut = mon_core.check_workers(workersToCheck)
            if not tmpStat:
                tmp_log.error('failed to check workers with: {0}'.format(tmpOut))
            else:
                tmp_log.debug('checked')
                for workSpec, (newStatus, diagMessage) in zip(workersToCheck, tmpOut):
                    workerID = workSpec.workerID
                    tmp_log.debug('Going to check workerID={0}'.format(workerID))
                    pandaIDs = []
                    if workerID in retMap:
                        # request kill
                        if messenger.kill_requested(workSpec):
                            self.dbProxy.kill_worker(workSpec.workerID)

                        # expired heartbeat - only when requested in the configuration
                        try:
                            # check if the queue configuration requires checking for worker heartbeat
                            worker_heartbeat_limit = int(queue_config.messenger['worker_heartbeat'])
                        except (AttributeError, KeyError):
                            worker_heartbeat_limit = None
                        tmp_log.debug(
                            'workerID={0} heartbeat limit is configured to {1}'.format(workerID,
                                                                                       worker_heartbeat_limit))
                        if worker_heartbeat_limit:
                            if messenger.is_alive(workSpec, worker_heartbeat_limit):
                                tmp_log.debug('heartbeat for workerID={0} is valid'.format(workerID))
                            else:
                                tmp_log.debug('heartbeat for workerID={0} expired: sending kill request'.format(
                                    workerID))
                                self.dbProxy.kill_worker(workSpec.workerID)

                        # get work attributes
                        workAttributes = messenger.get_work_attributes(workSpec)
                        retMap[workerID]['workAttributes'] = workAttributes
                        # get output files
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
                        if workSpec.mapType == WorkSpec.MT_NoJob:
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
                                    messenger.post_processing(workSpec, jobSpecs, workSpec.mapType)
                                workSpec.post_processed()
                                newStatus = WorkSpec.ST_running
                            # reset modification time to immediately trigger subsequent lookup
                            workSpec.trigger_next_lookup()
                        retMap[workerID]['newStatus'] = newStatus
                        retMap[workerID]['diagMessage'] = diagMessage
                    else:
                        tmp_log.debug('workerID={0} not in retMap'.format(workerID))
            return True, retMap
        except:
            core_utils.dump_error_message(tmp_log)
            return False, None
