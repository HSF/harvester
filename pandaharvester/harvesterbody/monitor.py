import time
import datetime
import collections
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
    # fifos
    monitor_fifo = MonitorFIFO()

    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.queueConfigMapper = queue_config_mapper
        self.dbProxy = DBProxy()
        self.pluginFactory = PluginFactory()
        self.startTimestamp = time.time()

    # main loop
    def run(self):
        lockedBy = 'monitor-{0}'.format(self.get_pid())
        # init messengers
        for queueConfig in self.queueConfigMapper.get_all_queues().values():
            # just import for module initialization
            self.pluginFactory.get_plugin(queueConfig.messenger)
        # main
        last_DB_cycle_timestamp = 0
        monitor_fifo = self.monitor_fifo
        sleepTime = (harvester_config.monitor.fifoSleepTimeMilli / 1000.0) \
                        if monitor_fifo.enabled else harvester_config.monitor.sleepTime
        try:
            fifoCheckDuration = harvester_config.monitor.fifoCheckDuration
        except AttributeError:
            fifoCheckDuration = 0
        try:
            fifoMaxWorkersPerChunk = harvester_config.monitor.fifoMaxWorkersPerChunk
        except AttributeError:
            fifoMaxWorkersPerChunk = 500
        try:
            fifoProtectiveDequeue = harvester_config.monitor.fifoProtectiveDequeue
        except AttributeError:
            fifoProtectiveDequeue = True
        if monitor_fifo.enabled:
            monitor_fifo.restore()
        while True:
            sw = core_utils.get_stopwatch()
            mainLog = self.make_logger(_logger, 'id={0}'.format(lockedBy), method_name='run')

            if time.time() >= last_DB_cycle_timestamp + harvester_config.monitor.sleepTime:
                # run with workers from DB
                mainLog.debug('starting run with DB')
                mainLog.debug('getting workers to monitor')
                workSpecsPerQueue = self.dbProxy.get_workers_to_update(harvester_config.monitor.maxWorkers,
                                                                       harvester_config.monitor.checkInterval,
                                                                       harvester_config.monitor.lockInterval,
                                                                       lockedBy)
                mainLog.debug('got {0} queues'.format(len(workSpecsPerQueue)))
                # loop over all workers
                for queueName, configIdWorkSpecs in iteritems(workSpecsPerQueue):
                    for configID, workSpecsList in iteritems(configIdWorkSpecs):
                        retVal = self.monitor_agent_core(lockedBy, queueName, workSpecsList, config_id=configID)
                        if monitor_fifo.enabled and retVal is not None:
                            workSpecsToEnqueue, workSpecsToEnqueueToHead, timeNow_timestamp, fifoCheckInterval = retVal
                            if workSpecsToEnqueue:
                                mainLog.debug('putting workers to FIFO')
                                try:
                                    score = fifoCheckInterval + timeNow_timestamp
                                    monitor_fifo.put((queueName, workSpecsToEnqueue), score)
                                    mainLog.info('put workers of {0} to FIFO with score {1}'.format(queueName, score))
                                except Exception as errStr:
                                    mainLog.error('failed to put object from FIFO: {0}'.format(errStr))
                            if workSpecsToEnqueueToHead:
                                mainLog.debug('putting workers to FIFO head')
                                try:
                                    score = fifoCheckInterval - timeNow_timestamp
                                    monitor_fifo.put((queueName, workSpecsToEnqueueToHead), score)
                                    mainLog.info('put workers of {0} to FIFO with score {1}'.format(queueName, score))
                                except Exception as errStr:
                                    mainLog.error('failed to put object from FIFO head: {0}'.format(errStr))
                last_DB_cycle_timestamp = time.time()
                mainLog.debug('ended run with DB')
            elif monitor_fifo.enabled:
                # run with workers from FIFO
                n_loops = 0
                last_fifo_cycle_timestamp = time.time()
                to_break = False
                obj_dequeued_id_list = []
                obj_to_enqueue_dict = collections.defaultdict(lambda: [[], 0, 0])
                obj_to_enqueue_to_head_dict = collections.defaultdict(lambda: [[], 0, 0])
                remaining_obj_to_enqueue_dict = {}
                remaining_obj_to_enqueue_to_head_dict = {}
                while n_loops == 0 or time.time() < last_fifo_cycle_timestamp + fifoCheckDuration:
                    if monitor_fifo.to_check_workers():
                        # check fifo size
                        fifo_size = monitor_fifo.size()
                        mainLog.debug('FIFO size is {0}'.format(fifo_size))
                        mainLog.debug('starting run with FIFO')
                        try:
                            obj_gotten = monitor_fifo.get(timeout=1, protective=fifoProtectiveDequeue)
                        except Exception as errStr:
                            mainLog.error('failed to get object from FIFO: {0}'.format(errStr))
                        else:
                            if obj_gotten is not None:
                                if fifoProtectiveDequeue:
                                    obj_dequeued_id_list.append(obj_gotten.id)
                                queueName, workSpecsList = obj_gotten.item
                                mainLog.debug('got {0} workers of {1}'.format(len(workSpecsList), queueName))
                                configID = workSpecsList[0][0].configID
                                for workSpecs in workSpecsList:
                                    for workSpec in workSpecs:
                                        if workSpec.pandaid_list is None:
                                            _jobspec_list = workSpec.get_jobspec_list()
                                            if _jobspec_list is not None:
                                                workSpec.pandaid_list = [j.PandaID for j in workSpec.get_jobspec_list()]
                                            else:
                                                workSpec.pandaid_list = []
                                            workSpec.force_update('pandaid_list')
                                retVal = self.monitor_agent_core(lockedBy, queueName, workSpecsList, from_fifo=True,
                                                                 config_id=configID)
                                if retVal is not None:
                                    workSpecsToEnqueue, workSpecsToEnqueueToHead, timeNow_timestamp, fifoCheckInterval = retVal
                                    try:
                                        if len(obj_to_enqueue_dict[queueName][0]) + len(workSpecsToEnqueue) <= fifoMaxWorkersPerChunk:
                                            obj_to_enqueue_dict[queueName][0].extend(workSpecsToEnqueue)
                                            obj_to_enqueue_dict[queueName][1] = max(obj_to_enqueue_dict[queueName][1], timeNow_timestamp)
                                            obj_to_enqueue_dict[queueName][2] = max(obj_to_enqueue_dict[queueName][2], fifoCheckInterval)
                                        else:
                                            to_break = True
                                            remaining_obj_to_enqueue_dict[queueName] = [workSpecsToEnqueue, timeNow_timestamp, fifoCheckInterval]
                                    except Exception as errStr:
                                        mainLog.error('failed to gather workers for FIFO: {0}'.format(errStr))
                                        to_break = True
                                    try:
                                        if len(obj_to_enqueue_to_head_dict[queueName][0]) + len(workSpecsToEnqueueToHead) <= fifoMaxWorkersPerChunk:
                                            obj_to_enqueue_to_head_dict[queueName][0].extend(workSpecsToEnqueueToHead)
                                            obj_to_enqueue_to_head_dict[queueName][1] = max(obj_to_enqueue_to_head_dict[queueName][1], timeNow_timestamp)
                                            obj_to_enqueue_to_head_dict[queueName][2] = max(obj_to_enqueue_to_head_dict[queueName][2], fifoCheckInterval)
                                        else:
                                            to_break = True
                                            remaining_obj_to_enqueue_to_head_dict[queueName] = [workSpecsToEnqueueToHead, timeNow_timestamp, fifoCheckInterval]
                                    except Exception as errStr:
                                        mainLog.error('failed to gather workers for FIFO head: {0}'.format(errStr))
                                        to_break = True
                                else:
                                    mainLog.debug('monitor_agent_core returned None. Skipped putting to FIFO')
                                if to_break:
                                    break
                            else:
                                mainLog.debug('got nothing in FIFO')
                    else:
                        mainLog.debug('workers in FIFO too young to check. Skipped')
                        time.sleep(sleepTime)
                    n_loops += 1
                # enqueue to fifo
                mainLog.debug('putting worker chunks to FIFO')
                for _dct in (obj_to_enqueue_dict, remaining_obj_to_enqueue_dict):
                    for queueName, obj_to_enqueue in iteritems(_dct):
                        try:
                            workSpecsToEnqueue, timeNow_timestamp, fifoCheckInterval = obj_to_enqueue
                            if workSpecsToEnqueue:
                                score = fifoCheckInterval + timeNow_timestamp
                                monitor_fifo.put((queueName, workSpecsToEnqueue), score)
                                mainLog.info('put a chunk of {0} workers of {1} to FIFO with score {2}'.format(
                                                len(workSpecsToEnqueue), queueName, score))
                        except Exception as errStr:
                            mainLog.error('failed to put object from FIFO: {0}'.format(errStr))
                mainLog.debug('putting worker chunks to FIFO head')
                for _dct in (obj_to_enqueue_to_head_dict, remaining_obj_to_enqueue_to_head_dict):
                    for queueName, obj_to_enqueue_to_head in iteritems(_dct):
                        try:
                            workSpecsToEnqueueToHead, timeNow_timestamp, fifoCheckInterval = obj_to_enqueue_to_head
                            if workSpecsToEnqueueToHead:
                                score = fifoCheckInterval - timeNow_timestamp
                                monitor_fifo.put((queueName, workSpecsToEnqueueToHead), score)
                                mainLog.info('put a chunk of {0} workers of {1} to FIFO with score {2}'.format(
                                                len(workSpecsToEnqueueToHead), queueName, score))
                        except Exception as errStr:
                            mainLog.error('failed to put object from FIFO head: {0}'.format(errStr))
                # release protective dequeued objects
                if fifoProtectiveDequeue and len(obj_dequeued_id_list) > 0:
                    monitor_fifo.release(ids=obj_dequeued_id_list)
                mainLog.debug('ended run with FIFO')

            if sw.get_elapsed_time_in_sec() > harvester_config.monitor.lockInterval:
                mainLog.warning('a single cycle was longer than lockInterval ' + sw.get_elapsed_time())
            else:
                mainLog.debug('done' + sw.get_elapsed_time())

            # check if being terminated
            if self.terminated(sleepTime):
                mainLog.debug('terminated')
                return


    # core of monitor agent to check workers in workSpecsList of queueName
    def monitor_agent_core(self, lockedBy, queueName, workSpecsList, from_fifo=False, config_id=None):
        tmpQueLog = self.make_logger(_logger, 'id={0} queue={1}'.format(lockedBy, queueName),
                                     method_name='run')
        # check queue
        if not self.queueConfigMapper.has_queue(queueName, config_id):
            tmpQueLog.error('config not found')
            return
        # get queue
        queueConfig = self.queueConfigMapper.get_queue(queueName, config_id)
        # get plugins
        monCore = self.pluginFactory.get_plugin(queueConfig.monitor)
        messenger = self.pluginFactory.get_plugin(queueConfig.messenger)
        # workspec chunk of active workers
        workSpecsToEnqueue = []
        workSpecsToEnqueueToHead = []
        timeNow_timestamp = time.time()
        # get fifoCheckInterval for PQ
        try:
            fifoCheckInterval = monCore.fifoCheckInterval
        except Exception:
            if hasattr(harvester_config.monitor, 'fifoCheckInterval'):
                fifoCheckInterval = harvester_config.monitor.fifoCheckInterval
            else:
                fifoCheckInterval = harvester_config.monitor.checkInterval
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
                # lock workers for fifo
                temRetLockWorker = None
                if from_fifo:
                    # lock workers
                    worker_id_list = [w.workerID for w in workSpecs]
                    temRetLockWorker = self.dbProxy.lock_workers(worker_id_list,
                                            harvester_config.monitor.lockInterval, lockedBy)
                    # skip if not locked
                    if not temRetLockWorker:
                        continue
                # loop over workSpecs
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
                    isChecked = tmpOut['isChecked']
                    tmpStr = 'newStatus={0} monitoredStatus={1} diag={2} '
                    tmpStr += 'postProcessed={3} files={4}'
                    tmpLog.debug(tmpStr.format(newStatus, monStatus, diagMessage,
                                               workSpec.is_post_processed(),
                                               str(filesToStageOut)))
                    iWorker += 1
                    if from_fifo:
                        workSpec.lockedBy = lockedBy
                        workSpec.force_update('lockedBy')
                    # check status
                    if newStatus not in WorkSpec.ST_LIST:
                        tmpLog.error('unknown status={0}'.format(newStatus))
                        return
                    # update worker
                    workSpec.set_status(newStatus)
                    workSpec.set_work_attributes(workAttributes)
                    workSpec.set_dialog_message(diagMessage)
                    if isChecked:
                        workSpec.checkTime = datetime.datetime.utcnow()
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
                        if from_fifo:
                            tmpLog.info('failed to update the DB. Maybe locked by other thread running with DB')
                        else:
                            tmpLog.error('failed to update the DB. lockInterval may be too short')
                        sendWarning = True
                # send ACK to workers for events and files
                if len(eventsToUpdateList) > 0 or len(filesToStageOutList) > 0:
                    for workSpec in workSpecs:
                        try:
                            messenger.acknowledge_events_files(workSpec)
                        except Exception:
                            core_utils.dump_error_message(tmpQueLog)
                            tmpQueLog.error('failed to send ACK to workerID={0}'.format(workSpec.workerID))
                # active workers for fifo
                if self.monitor_fifo.enabled and workSpecs:
                    workSpec = workSpecs[0]
                    tmpOut = tmpRetMap[workSpec.workerID]
                    newStatus = tmpOut['newStatus']
                    monStatus = tmpOut['monStatus']
                    if newStatus in [WorkSpec.ST_submitted, WorkSpec.ST_running] \
                        and workSpec.mapType != WorkSpec.MT_MultiWorkers \
                        and workSpec.workAttributes is not None:
                        forceEnqueueInterval = harvester_config.monitor.fifoForceEnqueueInterval
                        timeNow = datetime.datetime.utcnow()
                        timeNow_timestamp = time.time()
                        _bool, lastCheckAt = workSpec.get_work_params('lastCheckAt')
                        try:
                            last_check_period = timeNow_timestamp - lastCheckAt
                        except TypeError:
                            last_check_period = forceEnqueueInterval + 1.0
                        if _bool and lastCheckAt is not None and last_check_period > harvester_config.monitor.checkInterval \
                            and timeNow_timestamp - harvester_config.monitor.checkInterval > self.startTimestamp:
                            tmpQueLog.warning('last check period of workerID={0} is {1} sec, longer than monitor checkInterval'.format(
                                                workSpec.workerID, last_check_period))
                        if (from_fifo) \
                            or (not from_fifo
                                and timeNow_timestamp - harvester_config.monitor.sleepTime > self.startTimestamp
                                and last_check_period > forceEnqueueInterval):
                            if not from_fifo:
                                tmpQueLog.warning('last check period of workerID={0} is {1} sec, longer than monitor forceEnqueueInterval. Enqueue the worker by force'.format(
                                                    workSpec.workerID, last_check_period))
                            workSpec.set_work_params({'lastCheckAt': timeNow_timestamp})
                            workSpec.lockedBy = None
                            workSpec.force_update('lockedBy')
                            if monStatus in [WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled]:
                                _bool, startFifoPreemptAt = workSpec.get_work_params('startFifoPreemptAt')
                                if not _bool or startFifoPreemptAt is None:
                                    startFifoPreemptAt = timeNow_timestamp
                                    workSpec.set_work_params({'startFifoPreemptAt': startFifoPreemptAt})
                                tmpQueLog.debug('workerID={0} , startFifoPreemptAt: {1}'.format(workSpec.workerID, startFifoPreemptAt))
                                if timeNow_timestamp - startFifoPreemptAt < harvester_config.monitor.fifoMaxPreemptInterval:
                                    workSpecsToEnqueueToHead.append(workSpecs)
                                else:
                                    workSpec.set_work_params({'startFifoPreemptAt': timeNow_timestamp})
                                    workSpec.modificationTime = timeNow
                                    workSpec.force_update('modificationTime')
                                    workSpecsToEnqueue.append(workSpecs)
                            else:
                                workSpec.modificationTime = timeNow
                                workSpec.force_update('modificationTime')
                                workSpecsToEnqueue.append(workSpecs)
        else:
            tmpQueLog.error('failed to check workers')
        retVal = workSpecsToEnqueue, workSpecsToEnqueueToHead, timeNow_timestamp, fifoCheckInterval
        tmpQueLog.debug('done')
        return retVal


    # wrapper for checkWorkers
    def check_workers(self, mon_core, messenger, all_workers, queue_config, tmp_log):
        # check timeout value
        try:
            checkTimeout = mon_core.checkTimeout
        except Exception:
            try:
                checkTimeout = harvester_config.monitor.checkTimeout
            except Exception:
                checkTimeout = None
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
                                         'nJobsToReFill': nJobsToReFill,
                                         'isChecked': True}
        # check workers
        tmp_log.debug('checking workers with plugin')
        try:
            tmpStat, tmpOut = mon_core.check_workers(workersToCheck)
            if not tmpStat:
                tmp_log.error('failed to check workers with: {0}'.format(tmpOut))
            else:
                tmp_log.debug('checked')
                timeNow = datetime.datetime.utcnow()
                for workSpec, (newStatus, diagMessage) in zip(workersToCheck, tmpOut):
                    workerID = workSpec.workerID
                    tmp_log.debug('Going to check workerID={0}'.format(workerID))
                    pandaIDs = []
                    if workerID in retMap:
                        # failed to check status
                        if newStatus is None:
                            tmp_log.error('Failed to check workerID={0} with {1}'.format(workerID, diagMessage))
                            retMap[workerID]['isChecked'] = False
                            # set status
                            if workSpec.checkTime is not None and checkTimeout is not None and \
                                    timeNow - workSpec.checkTime > datetime.timedelta(seconds=checkTimeout):
                                # kill due to timeout
                                tmp_log.debug('kill workerID={0} due to continuous check failure'.format(workerID))
                                self.dbProxy.kill_worker(workSpec.workerID)
                                newStatus = WorkSpec.ST_cancelled
                                diagMessage = 'killed due to continuous check failure. ' + diagMessage
                                workSpec.set_pilot_error(PilotErrors.ERR_FAILEDBYSERVER, diagMessage)
                            else:
                                # use original status
                                newStatus = workSpec.status
                        # request kill
                        if messenger.kill_requested(workSpec):
                            tmp_log.debug('kill workerID={0} as requested'.format(workerID))
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
        except Exception:
            core_utils.dump_error_message(tmp_log)
            return False, None
