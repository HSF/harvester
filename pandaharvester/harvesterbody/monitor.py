import time
import datetime
import collections
import random
import itertools
from future.utils import iteritems

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvestercore.pilot_errors import PilotErrors
from pandaharvester.harvestercore.fifos import MonitorFIFO, MonitorEventFIFO
from pandaharvester.harvestermisc.apfmon import Apfmon

# logger
_logger = core_utils.setup_logger("monitor")


# propagate important checkpoints to panda
class Monitor(AgentBase):
    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        tmp_log = self.make_logger(_logger, method_name="__init__")
        AgentBase.__init__(self, single_mode)
        self.queueConfigMapper = queue_config_mapper
        self.dbProxy = DBProxy()
        self.pluginFactory = PluginFactory()
        self.startTimestamp = time.time()
        try:
            self.monitor_fifo = MonitorFIFO()
        except Exception:
            tmp_log.error("failed to launch monitor-fifo")
            core_utils.dump_error_message(tmp_log)
        if self.monitor_fifo.enabled:
            try:
                self.monitor_event_fifo = MonitorEventFIFO()
            except Exception:
                tmp_log.error("failed to launch monitor-event-fifo")
                core_utils.dump_error_message(tmp_log)
        else:
            self.monitor_event_fifo = None
        self.apfmon = Apfmon(self.queueConfigMapper)
        self.eventBasedMonCoreList = []
        if getattr(harvester_config.monitor, "eventBasedEnable", False):
            for pluginConf in harvester_config.monitor.eventBasedPlugins:
                pluginFactory = PluginFactory()
                plugin_key = pluginFactory.get_plugin_key(pluginConf)
                try:
                    self.eventBasedMonCoreList.append(pluginFactory.get_plugin(pluginConf))
                except Exception:
                    tmp_log.error("failed to launch event-based-monitor plugin of {0}".format(plugin_key))
                    core_utils.dump_error_message(tmp_log)

    # main loop
    def run(self):
        lockedBy = "monitor-{0}".format(self.get_pid())
        mainLog = self.make_logger(_logger, "id={0}".format(lockedBy), method_name="run")
        # init messengers
        for queueName, queueConfig in self.queueConfigMapper.get_all_queues().items():
            # just import for module initialization
            try:
                self.pluginFactory.get_plugin(queueConfig.messenger)
            except Exception:
                mainLog.error("failed to launch messenger plugin for {0}".format(queueName))
                core_utils.dump_error_message(mainLog)
        # main
        fifoSleepTimeMilli = getattr(harvester_config.monitor, "fifoSleepTimeMilli", 5000)
        fifoCheckDuration = getattr(harvester_config.monitor, "fifoCheckDuration", 30)
        fifoMaxWorkersPerChunk = getattr(harvester_config.monitor, "fifoMaxWorkersPerChunk", 500)
        fifoProtectiveDequeue = getattr(harvester_config.monitor, "fifoProtectiveDequeue", True)
        eventBasedCheckInterval = getattr(harvester_config.monitor, "eventBasedCheckInterval", 300)
        eventBasedTimeWindow = getattr(harvester_config.monitor, "eventBasedTimeWindow", 450)
        eventBasedCheckMaxEvents = getattr(harvester_config.monitor, "eventBasedCheckMaxEvents", 500)
        eventBasedEventLifetime = getattr(harvester_config.monitor, "eventBasedEventLifetime", 1800)
        eventBasedRemoveMaxEvents = getattr(harvester_config.monitor, "eventBasedRemoveMaxEvents", 2000)
        last_DB_cycle_timestamp = 0
        last_event_delivery_timestamp = 0
        last_event_digest_timestamp = 0
        last_event_dispose_timestamp = 0
        monitor_fifo = self.monitor_fifo
        sleepTime = (fifoSleepTimeMilli / 1000.0) if monitor_fifo.enabled else harvester_config.monitor.sleepTime
        adjusted_sleepTime = sleepTime
        if monitor_fifo.enabled:
            monitor_fifo.restore()
        while True:
            sw_main = core_utils.get_stopwatch()
            mainLog.debug("start a monitor cycle")
            if time.time() >= last_DB_cycle_timestamp + harvester_config.monitor.sleepTime and not (monitor_fifo.enabled and self.singleMode):
                # run with workers from DB
                sw_db = core_utils.get_stopwatch()
                mainLog.debug("starting run with DB")
                mainLog.debug("getting workers to monitor")
                workSpecsPerQueue = self.dbProxy.get_workers_to_update(
                    harvester_config.monitor.maxWorkers, harvester_config.monitor.checkInterval, harvester_config.monitor.lockInterval, lockedBy
                )
                mainLog.debug("got {0} queues".format(len(workSpecsPerQueue)))
                # loop over all workers
                for queueName, configIdWorkSpecs in iteritems(workSpecsPerQueue):
                    for configID, workSpecsList in iteritems(configIdWorkSpecs):
                        try:
                            retVal = self.monitor_agent_core(lockedBy, queueName, workSpecsList, config_id=configID, check_source="DB")
                        except Exception as e:
                            mainLog.error("monitor_agent_core excepted with {0}".format(e))
                            retVal = None  # skip the loop

                        if monitor_fifo.enabled and retVal is not None:
                            workSpecsToEnqueue, workSpecsToEnqueueToHead, timeNow_timestamp, fifoCheckInterval = retVal
                            if workSpecsToEnqueue:
                                mainLog.debug("putting workers to FIFO")
                                try:
                                    score = fifoCheckInterval + timeNow_timestamp
                                    monitor_fifo.put((queueName, workSpecsToEnqueue), score)
                                    mainLog.info("put workers of {0} to FIFO with score {1}".format(queueName, score))
                                except Exception as errStr:
                                    mainLog.error("failed to put object from FIFO: {0}".format(errStr))
                            if workSpecsToEnqueueToHead:
                                mainLog.debug("putting workers to FIFO head")
                                try:
                                    score = fifoCheckInterval - timeNow_timestamp
                                    monitor_fifo.put((queueName, workSpecsToEnqueueToHead), score)
                                    mainLog.info("put workers of {0} to FIFO with score {1}".format(queueName, score))
                                except Exception as errStr:
                                    mainLog.error("failed to put object from FIFO head: {0}".format(errStr))
                last_DB_cycle_timestamp = time.time()
                if sw_db.get_elapsed_time_in_sec() > harvester_config.monitor.lockInterval:
                    mainLog.warning("a single DB cycle was longer than lockInterval " + sw_db.get_elapsed_time())
                else:
                    mainLog.debug("done a DB cycle" + sw_db.get_elapsed_time())
                mainLog.debug("ended run with DB")
            elif monitor_fifo.enabled:
                # with FIFO
                sw = core_utils.get_stopwatch()
                to_run_fifo_check = True
                n_loops = 0
                n_loops_hit = 0
                last_fifo_cycle_timestamp = time.time()
                to_break = False
                obj_dequeued_id_list = []
                obj_to_enqueue_dict = collections.defaultdict(lambda: [[], 0, 0])
                obj_to_enqueue_to_head_dict = collections.defaultdict(lambda: [[], 0, 0])
                remaining_obj_to_enqueue_dict = {}
                remaining_obj_to_enqueue_to_head_dict = {}
                n_chunk_peeked_stat, sum_overhead_time_stat = 0, 0.0
                # go get workers
                if self.monitor_event_fifo.enabled:
                    # run with workers reported from plugin (event-based check)
                    to_deliver = time.time() >= last_event_delivery_timestamp + eventBasedCheckInterval
                    to_digest = time.time() >= last_event_digest_timestamp + eventBasedCheckInterval / 4
                    to_dispose = time.time() >= last_event_dispose_timestamp + eventBasedCheckInterval / 2
                    if to_deliver:
                        # deliver events of worker update
                        got_lock = self.dbProxy.get_process_lock("monitor_event_deliverer", lockedBy, eventBasedCheckInterval)
                        if got_lock:
                            self.monitor_event_deliverer(time_window=eventBasedTimeWindow)
                        else:
                            mainLog.debug("did not get lock. Skip monitor_event_deliverer")
                        last_event_delivery_timestamp = time.time()
                    if to_digest:
                        # digest events of worker update
                        to_run_fifo_check = False
                        retMap = self.monitor_event_digester(locked_by=lockedBy, max_events=eventBasedCheckMaxEvents)
                        for qc_key, retVal in iteritems(retMap):
                            workSpecsToEnqueue, workSpecsToEnqueueToHead, timeNow_timestamp, fifoCheckInterval = retVal
                            # only enqueue postprocessing workers to FIFO
                            obj_to_enqueue_to_head_dict[qc_key][0].extend(workSpecsToEnqueueToHead)
                            obj_to_enqueue_to_head_dict[qc_key][1] = max(obj_to_enqueue_to_head_dict[qc_key][1], timeNow_timestamp)
                            obj_to_enqueue_to_head_dict[qc_key][2] = max(obj_to_enqueue_to_head_dict[qc_key][2], fifoCheckInterval)
                        last_event_digest_timestamp = time.time()
                    if to_dispose:
                        # dispose of outdated events of worker update
                        self.monitor_event_disposer(event_lifetime=eventBasedEventLifetime, max_events=eventBasedRemoveMaxEvents)
                        last_event_dispose_timestamp = time.time()
                if to_run_fifo_check:
                    # run with workers from FIFO
                    while time.time() < last_fifo_cycle_timestamp + fifoCheckDuration:
                        sw.reset()
                        n_loops += 1
                        try:
                            retVal, overhead_time = monitor_fifo.to_check_workers()
                        except Exception as e:
                            mainLog.error("failed to check workers from FIFO: {0}".format(e))
                        if overhead_time is not None:
                            n_chunk_peeked_stat += 1
                            sum_overhead_time_stat += overhead_time
                        if retVal:
                            # check fifo size
                            try:
                                fifo_size = monitor_fifo.size()
                                mainLog.debug("FIFO size is {0}".format(fifo_size))
                            except Exception as e:
                                mainLog.error("failed to get size of FIFO: {0}".format(e))
                                time.sleep(2)
                                continue
                            mainLog.debug("starting run with FIFO")
                            try:
                                obj_gotten = monitor_fifo.get(timeout=1, protective=fifoProtectiveDequeue)
                            except Exception as errStr:
                                mainLog.error("failed to get object from FIFO: {0}".format(errStr))
                                time.sleep(2)
                                continue
                            else:
                                if obj_gotten is not None:
                                    sw_fifo = core_utils.get_stopwatch()
                                    if fifoProtectiveDequeue:
                                        obj_dequeued_id_list.append(obj_gotten.id)
                                    queueName, workSpecsList = obj_gotten.item
                                    mainLog.debug("got a chunk of {0} workers of {1} from FIFO".format(len(workSpecsList), queueName) + sw.get_elapsed_time())
                                    sw.reset()
                                    configID = None
                                    for workSpecs in workSpecsList:
                                        if configID is None and len(workSpecs) > 0:
                                            configID = workSpecs[0].configID
                                        for workSpec in workSpecs:
                                            if workSpec.pandaid_list is None:
                                                _jobspec_list = workSpec.get_jobspec_list()
                                                if _jobspec_list is not None:
                                                    workSpec.pandaid_list = [j.PandaID for j in workSpec.get_jobspec_list()]
                                                else:
                                                    workSpec.pandaid_list = []
                                                workSpec.force_update("pandaid_list")
                                    try:
                                        retVal = self.monitor_agent_core(
                                            lockedBy, queueName, workSpecsList, from_fifo=True, config_id=configID, check_source="FIFO"
                                        )
                                    except Exception as e:
                                        mainLog.error("monitor_agent_core excepted with {0}".format(e))
                                        retVal = None  # skip the loop

                                    if retVal is not None:
                                        workSpecsToEnqueue, workSpecsToEnqueueToHead, timeNow_timestamp, fifoCheckInterval = retVal
                                        qc_key = (queueName, configID)
                                        try:
                                            if len(obj_to_enqueue_dict[qc_key][0]) + len(workSpecsToEnqueue) <= fifoMaxWorkersPerChunk:
                                                obj_to_enqueue_dict[qc_key][0].extend(workSpecsToEnqueue)
                                                obj_to_enqueue_dict[qc_key][1] = max(obj_to_enqueue_dict[qc_key][1], timeNow_timestamp)
                                                obj_to_enqueue_dict[qc_key][2] = max(obj_to_enqueue_dict[qc_key][2], fifoCheckInterval)
                                            else:
                                                to_break = True
                                                remaining_obj_to_enqueue_dict[qc_key] = [workSpecsToEnqueue, timeNow_timestamp, fifoCheckInterval]
                                        except Exception as errStr:
                                            mainLog.error("failed to gather workers for FIFO: {0}".format(errStr))
                                            to_break = True
                                        try:
                                            if len(obj_to_enqueue_to_head_dict[qc_key][0]) + len(workSpecsToEnqueueToHead) <= fifoMaxWorkersPerChunk:
                                                obj_to_enqueue_to_head_dict[qc_key][0].extend(workSpecsToEnqueueToHead)
                                                obj_to_enqueue_to_head_dict[qc_key][1] = max(obj_to_enqueue_to_head_dict[qc_key][1], timeNow_timestamp)
                                                obj_to_enqueue_to_head_dict[qc_key][2] = max(obj_to_enqueue_to_head_dict[qc_key][2], fifoCheckInterval)
                                            else:
                                                to_break = True
                                                remaining_obj_to_enqueue_to_head_dict[qc_key] = [workSpecsToEnqueueToHead, timeNow_timestamp, fifoCheckInterval]
                                        except Exception as errStr:
                                            mainLog.error("failed to gather workers for FIFO head: {0}".format(errStr))
                                            to_break = True
                                        mainLog.debug("checked {0} workers from FIFO".format(len(workSpecsList)) + sw.get_elapsed_time())
                                    else:
                                        mainLog.debug("monitor_agent_core returned None. Skipped putting to FIFO")
                                    if sw_fifo.get_elapsed_time_in_sec() > harvester_config.monitor.lockInterval:
                                        mainLog.warning("a single FIFO cycle was longer than lockInterval " + sw_fifo.get_elapsed_time())
                                    else:
                                        mainLog.debug("done a FIFO cycle" + sw_fifo.get_elapsed_time())
                                        n_loops_hit += 1
                                    if to_break:
                                        break
                                else:
                                    mainLog.debug("got nothing in FIFO")
                        else:
                            mainLog.debug("workers in FIFO too young to check. Skipped")
                            if self.singleMode:
                                break
                            if overhead_time is not None:
                                time.sleep(max(-overhead_time * random.uniform(0.1, 1), adjusted_sleepTime))
                            else:
                                time.sleep(max(fifoCheckDuration * random.uniform(0.1, 1), adjusted_sleepTime))
                    mainLog.debug("run {0} loops, including {1} FIFO cycles".format(n_loops, n_loops_hit))
                # enqueue to fifo
                sw.reset()
                n_chunk_put = 0
                mainLog.debug("putting worker chunks to FIFO")
                for _dct in (obj_to_enqueue_dict, remaining_obj_to_enqueue_dict):
                    for (queueName, configID), obj_to_enqueue in iteritems(_dct):
                        try:
                            workSpecsToEnqueue, timeNow_timestamp, fifoCheckInterval = obj_to_enqueue
                            if workSpecsToEnqueue:
                                score = fifoCheckInterval + timeNow_timestamp
                                monitor_fifo.put((queueName, workSpecsToEnqueue), score)
                                n_chunk_put += 1
                                mainLog.info("put a chunk of {0} workers of {1} to FIFO with score {2}".format(len(workSpecsToEnqueue), queueName, score))
                        except Exception as errStr:
                            mainLog.error("failed to put object from FIFO: {0}".format(errStr))
                mainLog.debug("putting worker chunks to FIFO head")
                for _dct in (obj_to_enqueue_to_head_dict, remaining_obj_to_enqueue_to_head_dict):
                    for (queueName, configID), obj_to_enqueue_to_head in iteritems(_dct):
                        try:
                            workSpecsToEnqueueToHead, timeNow_timestamp, fifoCheckInterval = obj_to_enqueue_to_head
                            if workSpecsToEnqueueToHead:
                                score = fifoCheckInterval + timeNow_timestamp - 2**32
                                monitor_fifo.put((queueName, workSpecsToEnqueueToHead), score)
                                n_chunk_put += 1
                                mainLog.info("put a chunk of {0} workers of {1} to FIFO with score {2}".format(len(workSpecsToEnqueueToHead), queueName, score))
                        except Exception as errStr:
                            mainLog.error("failed to put object from FIFO head: {0}".format(errStr))
                # delete protective dequeued objects
                if fifoProtectiveDequeue and len(obj_dequeued_id_list) > 0:
                    try:
                        monitor_fifo.delete(ids=obj_dequeued_id_list)
                    except Exception as e:
                        mainLog.error("failed to delete object from FIFO: {0}".format(e))
                mainLog.debug("put {0} worker chunks into FIFO".format(n_chunk_put) + sw.get_elapsed_time())
                # adjust adjusted_sleepTime
                if n_chunk_peeked_stat > 0 and sum_overhead_time_stat > sleepTime:
                    speedup_factor = (sum_overhead_time_stat - sleepTime) / (n_chunk_peeked_stat * harvester_config.monitor.checkInterval)
                    speedup_factor = max(speedup_factor, 0)
                    adjusted_sleepTime = adjusted_sleepTime / (1.0 + speedup_factor)
                elif n_chunk_peeked_stat == 0 or sum_overhead_time_stat < 0:
                    adjusted_sleepTime = (sleepTime + adjusted_sleepTime) / 2
                mainLog.debug("adjusted_sleepTime becomes {0:.3f} sec".format(adjusted_sleepTime))
                # end run with fifo
                mainLog.debug("ended run with FIFO")
            # time the cycle
            mainLog.debug("done a monitor cycle" + sw_main.get_elapsed_time())
            # check if being terminated
            if self.terminated(adjusted_sleepTime):
                mainLog.debug("terminated")
                return

    # core of monitor agent to check workers in workSpecsList of queueName
    def monitor_agent_core(self, lockedBy, queueName, workSpecsList, from_fifo=False, config_id=None, check_source=None):
        tmpQueLog = self.make_logger(_logger, "id={0} queue={1}".format(lockedBy, queueName), method_name="run")
        # check queue
        if not self.queueConfigMapper.has_queue(queueName, config_id):
            tmpQueLog.error("config not found")
            return None
        # get queue
        queueConfig = self.queueConfigMapper.get_queue(queueName, config_id)
        # get plugins
        monCore = self.pluginFactory.get_plugin(queueConfig.monitor)
        messenger = self.pluginFactory.get_plugin(queueConfig.messenger)
        # workspec chunk of active workers
        workSpecsToEnqueue_dict = {}
        workSpecsToEnqueueToHead_dict = {}
        timeNow_timestamp = time.time()
        # get fifoCheckInterval for PQ and other fifo attributes
        try:
            fifoCheckInterval = monCore.fifoCheckInterval
        except Exception:
            if hasattr(harvester_config.monitor, "fifoCheckInterval"):
                fifoCheckInterval = harvester_config.monitor.fifoCheckInterval
            else:
                fifoCheckInterval = harvester_config.monitor.checkInterval
        try:
            forceEnqueueInterval = harvester_config.monitor.fifoForceEnqueueInterval
        except AttributeError:
            forceEnqueueInterval = 3600
        try:
            fifoMaxPreemptInterval = harvester_config.monitor.fifoMaxPreemptInterval
        except AttributeError:
            fifoMaxPreemptInterval = 60
        # check workers
        allWorkers = [item for sublist in workSpecsList for item in sublist]
        tmpQueLog.debug("checking {0} workers".format(len(allWorkers)))
        tmpStat, tmpRetMap = self.check_workers(monCore, messenger, allWorkers, queueConfig, tmpQueLog, from_fifo)
        if tmpStat:
            # loop over all worker chunks
            tmpQueLog.debug("update jobs and workers")
            iWorker = 0
            for workSpecs in workSpecsList:
                jobSpecs = None
                pandaIDsList = []
                eventsToUpdateList = []
                filesToStageOutList = dict()
                isCheckedList = []
                mapType = workSpecs[0].mapType
                # loop over workSpecs
                for workSpec in workSpecs:
                    tmpLog = self.make_logger(_logger, "id={0} workerID={1} from={2}".format(lockedBy, workSpec.workerID, check_source), method_name="run")
                    tmpOut = tmpRetMap[workSpec.workerID]
                    oldStatus = tmpOut["oldStatus"]
                    newStatus = tmpOut["newStatus"]
                    monStatus = tmpOut["monStatus"]
                    diagMessage = tmpOut["diagMessage"]
                    workAttributes = tmpOut["workAttributes"]
                    eventsToUpdate = tmpOut["eventsToUpdate"]
                    filesToStageOut = tmpOut["filesToStageOut"]
                    eventsRequestParams = tmpOut["eventsRequestParams"]
                    nJobsToReFill = tmpOut["nJobsToReFill"]
                    pandaIDs = tmpOut["pandaIDs"]
                    isChecked = tmpOut["isChecked"]
                    tmpStr = "newStatus={0} monitoredStatus={1} diag={2} "
                    tmpStr += "postProcessed={3} files={4}"
                    tmpLog.debug(tmpStr.format(newStatus, monStatus, diagMessage, workSpec.is_post_processed(), str(filesToStageOut)))
                    iWorker += 1
                    # check status
                    if newStatus not in WorkSpec.ST_LIST:
                        tmpLog.error("unknown status={0}".format(newStatus))
                        return
                    # update worker
                    workSpec.set_status(newStatus)
                    workSpec.set_work_attributes(workAttributes)
                    workSpec.set_dialog_message(diagMessage)
                    if isChecked:
                        workSpec.checkTime = datetime.datetime.utcnow()
                    isCheckedList.append(isChecked)
                    if monStatus == WorkSpec.ST_failed:
                        if not workSpec.has_pilot_error() and workSpec.errorCode is None:
                            workSpec.set_pilot_error(PilotErrors.GENERALERROR, diagMessage)
                    elif monStatus == WorkSpec.ST_cancelled:
                        if not workSpec.has_pilot_error() and workSpec.errorCode is None:
                            workSpec.set_pilot_error(PilotErrors.PANDAKILL, diagMessage)
                    if monStatus in [WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled]:
                        workSpec.set_work_params({"finalMonStatus": monStatus})
                    # request events
                    if eventsRequestParams != {}:
                        workSpec.eventsRequest = WorkSpec.EV_requestEvents
                        workSpec.eventsRequestParams = eventsRequestParams
                    # jobs to refill
                    if nJobsToReFill is not None:
                        workSpec.nJobsToReFill = nJobsToReFill
                    # get associated jobs for the worker chunk
                    if workSpec.hasJob == 1 and jobSpecs is None:
                        jobSpecs = self.dbProxy.get_jobs_with_worker_id(workSpec.workerID, None, only_running=True, slim=True)
                    # pandaIDs for push
                    pandaIDsList.append(pandaIDs)
                    if len(eventsToUpdate) > 0:
                        eventsToUpdateList.append(eventsToUpdate)
                    if len(filesToStageOut) > 0:
                        filesToStageOutList[workSpec.workerID] = filesToStageOut
                    # apfmon status update
                    if newStatus != oldStatus:
                        tmpQueLog.debug(
                            "newStatus: {0} monStatus: {1} oldStatus: {2} workSpecStatus: {3}".format(newStatus, monStatus, oldStatus, workSpec.status)
                        )
                        self.apfmon.update_worker(workSpec, monStatus)

                # lock workers for fifo
                if from_fifo:
                    # collect some attributes to be updated when workers are locked
                    worker_id_list = dict()
                    for workSpec, isChecked in zip(workSpecs, isCheckedList):
                        attrs = dict()
                        if isChecked:
                            attrs["checkTime"] = workSpec.checkTime
                            workSpec.force_not_update("checkTime")
                        if workSpec.has_updated_attributes():
                            attrs["lockedBy"] = lockedBy
                            workSpec.lockedBy = lockedBy
                            workSpec.force_not_update("lockedBy")
                        else:
                            attrs["lockedBy"] = None
                        worker_id_list[workSpec.workerID] = attrs
                    temRetLockWorker = self.dbProxy.lock_workers(worker_id_list, harvester_config.monitor.lockInterval)
                    # skip if not locked
                    if not temRetLockWorker:
                        continue
                # update jobs and workers
                if jobSpecs is not None and len(jobSpecs) > 0:
                    tmpQueLog.debug("updating {0} jobs with {1} workers".format(len(jobSpecs), len(workSpecs)))
                    core_utils.update_job_attributes_with_workers(mapType, jobSpecs, workSpecs, filesToStageOutList, eventsToUpdateList)
                # update local database
                tmpRet = self.dbProxy.update_jobs_workers(jobSpecs, workSpecs, lockedBy, pandaIDsList)
                if not tmpRet:
                    for workSpec in workSpecs:
                        tmpLog = self.make_logger(_logger, "id={0} workerID={1}".format(lockedBy, workSpec.workerID), method_name="run")
                        if from_fifo:
                            tmpLog.info("failed to update the DB. Maybe locked by other thread running with DB")
                        else:
                            if workSpec.status in [WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled, WorkSpec.ST_missed]:
                                tmpLog.info("worker already in final status. Skipped")
                            else:
                                tmpLog.error("failed to update the DB. lockInterval may be too short")
                else:
                    if jobSpecs is not None:
                        for jobSpec in jobSpecs:
                            tmpLog = self.make_logger(_logger, "id={0} PandaID={1}".format(lockedBy, jobSpec.PandaID), method_name="run")
                            tmpLog.debug(
                                "new status={0} subStatus={1} status_in_metadata={2}".format(
                                    jobSpec.status, jobSpec.subStatus, jobSpec.get_job_status_from_attributes()
                                )
                            )
                # send ACK to workers for events and files
                if len(eventsToUpdateList) > 0 or len(filesToStageOutList) > 0:
                    for workSpec in workSpecs:
                        try:
                            messenger.acknowledge_events_files(workSpec)
                        except Exception:
                            core_utils.dump_error_message(tmpQueLog)
                            tmpQueLog.error("failed to send ACK to workerID={0}".format(workSpec.workerID))
                # active workers for fifo
                if self.monitor_fifo.enabled and workSpecs:
                    workSpec = workSpecs[0]
                    tmpOut = tmpRetMap[workSpec.workerID]
                    newStatus = tmpOut["newStatus"]
                    monStatus = tmpOut["monStatus"]
                    if (
                        newStatus in [WorkSpec.ST_submitted, WorkSpec.ST_running, WorkSpec.ST_idle]
                        and workSpec.mapType != WorkSpec.MT_MultiWorkers
                        and workSpec.workAttributes is not None
                    ):
                        timeNow = datetime.datetime.utcnow()
                        timeNow_timestamp = time.time()
                        # get lastCheckAt
                        _bool, lastCheckAt = workSpec.get_work_params("lastCheckAt")
                        try:
                            last_check_period = timeNow_timestamp - lastCheckAt
                        except TypeError:
                            last_check_period = forceEnqueueInterval + 1.0
                        # get lastForceEnqueueAt
                        _bool, lastForceEnqueueAt = workSpec.get_work_params("lastForceEnqueueAt")
                        if not (_bool and lastForceEnqueueAt is not None):
                            lastForceEnqueueAt = 0
                        # notification
                        intolerable_delay = max(forceEnqueueInterval * 2, harvester_config.monitor.checkInterval * 4)
                        if (
                            _bool
                            and lastCheckAt is not None
                            and last_check_period > harvester_config.monitor.checkInterval
                            and timeNow_timestamp - harvester_config.monitor.checkInterval > self.startTimestamp
                        ):
                            if last_check_period > intolerable_delay:
                                tmpQueLog.error(
                                    "last check period of workerID={0} is {1} sec, intolerably longer than monitor checkInterval. Will NOT enquque worker by force. Please check why monitor checks worker slowly".format(
                                        workSpec.workerID, last_check_period
                                    )
                                )
                            else:
                                tmpQueLog.warning(
                                    "last check period of workerID={0} is {1} sec, longer than monitor checkInterval".format(
                                        workSpec.workerID, last_check_period
                                    )
                                )
                        # prepartion to enqueue fifo
                        if (from_fifo) or (
                            not from_fifo
                            and timeNow_timestamp - harvester_config.monitor.sleepTime > self.startTimestamp
                            and last_check_period > forceEnqueueInterval
                            and last_check_period < intolerable_delay
                            and timeNow_timestamp - lastForceEnqueueAt > 86400 + forceEnqueueInterval
                        ):
                            if not from_fifo:
                                # in DB cycle
                                tmpQueLog.warning(
                                    "last check period of workerID={0} is {1} sec, longer than monitor forceEnqueueInterval. Enqueue the worker by force".format(
                                        workSpec.workerID, last_check_period
                                    )
                                )
                                workSpec.set_work_params({"lastForceEnqueueAt": timeNow_timestamp})
                            workSpec.set_work_params({"lastCheckAt": timeNow_timestamp})
                            workSpec.lockedBy = None
                            workSpec.force_update("lockedBy")
                            if monStatus in [WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled]:
                                # for post-processing
                                _bool, startFifoPreemptAt = workSpec.get_work_params("startFifoPreemptAt")
                                if not _bool or startFifoPreemptAt is None:
                                    startFifoPreemptAt = timeNow_timestamp
                                    workSpec.set_work_params({"startFifoPreemptAt": startFifoPreemptAt})
                                tmpQueLog.debug("workerID={0} , startFifoPreemptAt: {1}".format(workSpec.workerID, startFifoPreemptAt))
                                if timeNow_timestamp - startFifoPreemptAt < fifoMaxPreemptInterval:
                                    workSpecsToEnqueueToHead_dict[workSpec.workerID] = workSpecs
                                else:
                                    workSpec.set_work_params({"startFifoPreemptAt": timeNow_timestamp})
                                    workSpec.modificationTime = timeNow
                                    workSpec.force_update("modificationTime")
                                    workSpecsToEnqueue_dict[workSpec.workerID] = workSpecs
                            else:
                                workSpec.modificationTime = timeNow
                                workSpec.force_update("modificationTime")
                                workSpecsToEnqueue_dict[workSpec.workerID] = workSpecs
        else:
            tmpQueLog.error("failed to check workers")
        workSpecsToEnqueue = list(workSpecsToEnqueue_dict.values())
        workSpecsToEnqueueToHead = list(workSpecsToEnqueueToHead_dict.values())
        retVal = workSpecsToEnqueue, workSpecsToEnqueueToHead, timeNow_timestamp, fifoCheckInterval
        tmpQueLog.debug("done")
        return retVal

    # wrapper for checkWorkers
    def check_workers(self, mon_core, messenger, all_workers, queue_config, tmp_log, from_fifo):
        # check timeout value
        try:
            checkTimeout = mon_core.checkTimeout
        except Exception:
            try:
                checkTimeout = harvester_config.monitor.checkTimeout
            except Exception:
                checkTimeout = None
        try:
            workerQueueTimeLimit = harvester_config.monitor.workerQueueTimeLimit
        except AttributeError:
            workerQueueTimeLimit = 172800
        workersToCheck = []
        thingsToPostProcess = []
        retMap = dict()
        for workSpec in all_workers:
            eventsRequestParams = {}
            eventsToUpdate = []
            pandaIDs = []
            workStatus = None
            workAttributes = None
            filesToStageOut = []
            nJobsToReFill = None
            if workSpec.has_work_params("finalMonStatus"):
                # to post-process
                _bool, finalMonStatus = workSpec.get_work_params("finalMonStatus")
                _thing = (workSpec, (finalMonStatus, ""))
                thingsToPostProcess.append(_thing)
            else:
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
            retMap[workSpec.workerID] = {
                "oldStatus": workSpec.status,
                "newStatus": workStatus,
                "monStatus": workStatus,
                "workAttributes": workAttributes,
                "filesToStageOut": filesToStageOut,
                "eventsRequestParams": eventsRequestParams,
                "eventsToUpdate": eventsToUpdate,
                "diagMessage": "",
                "pandaIDs": pandaIDs,
                "nJobsToReFill": nJobsToReFill,
                "isChecked": True,
            }
        # check workers
        tmp_log.debug("checking workers with plugin")
        try:
            if workersToCheck:
                tmpStat, tmpOut = mon_core.check_workers(workersToCheck)
                if not tmpStat:
                    tmp_log.error("failed to check workers with: {0}".format(tmpOut))
                    workersToCheck = []
                    tmpOut = []
                else:
                    tmp_log.debug("checked")
            else:
                tmp_log.debug("Nothing to be checked with plugin")
                tmpOut = []
            timeNow = datetime.datetime.utcnow()
            for workSpec, (newStatus, diagMessage) in itertools.chain(zip(workersToCheck, tmpOut), thingsToPostProcess):
                workerID = workSpec.workerID
                tmp_log.debug("Going to check workerID={0}".format(workerID))
                pandaIDs = []
                if workerID in retMap:
                    # failed to check status
                    if newStatus is None:
                        tmp_log.warning("Failed to check workerID={0} with {1}".format(workerID, diagMessage))
                        retMap[workerID]["isChecked"] = False
                        # set status
                        if (
                            workSpec.checkTime is not None
                            and checkTimeout is not None
                            and timeNow - workSpec.checkTime > datetime.timedelta(seconds=checkTimeout)
                        ):
                            # kill due to timeout
                            tmp_log.debug("kill workerID={0} due to consecutive check failures".format(workerID))
                            self.dbProxy.mark_workers_to_kill_by_workerids([workSpec.workerID])
                            newStatus = WorkSpec.ST_cancelled
                            diagMessage = "Killed by Harvester due to consecutive worker check failures. " + diagMessage
                            workSpec.set_pilot_error(PilotErrors.FAILEDBYSERVER, diagMessage)
                        else:
                            # use original status
                            newStatus = workSpec.status
                    # request kill
                    if messenger.kill_requested(workSpec):
                        tmp_log.debug("kill workerID={0} as requested".format(workerID))
                        self.dbProxy.mark_workers_to_kill_by_workerids([workSpec.workerID])
                    # stuck queuing for too long
                    if workSpec.status == WorkSpec.ST_submitted and timeNow > workSpec.submitTime + datetime.timedelta(seconds=workerQueueTimeLimit):
                        tmp_log.debug("kill workerID={0} due to queuing longer than {1} seconds".format(workerID, workerQueueTimeLimit))
                        self.dbProxy.mark_workers_to_kill_by_workerids([workSpec.workerID])
                        diagMessage = "Killed by Harvester due to worker queuing too long. " + diagMessage
                        workSpec.set_pilot_error(PilotErrors.FAILEDBYSERVER, diagMessage)
                        # set closed
                        workSpec.set_pilot_closed()
                    # expired heartbeat - only when requested in the configuration
                    try:
                        # check if the queue configuration requires checking for worker heartbeat
                        worker_heartbeat_limit = int(queue_config.messenger["worker_heartbeat"])
                    except (AttributeError, KeyError):
                        worker_heartbeat_limit = None
                    tmp_log.debug("workerID={0} heartbeat limit is configured to {1}".format(workerID, worker_heartbeat_limit))
                    if worker_heartbeat_limit:
                        if messenger.is_alive(workSpec, worker_heartbeat_limit):
                            tmp_log.debug("heartbeat for workerID={0} is valid".format(workerID))
                        else:
                            tmp_log.debug("heartbeat for workerID={0} expired: sending kill request".format(workerID))
                            self.dbProxy.mark_workers_to_kill_by_workerids([workSpec.workerID])
                            diagMessage = "Killed by Harvester due to worker heartbeat expired. " + diagMessage
                            workSpec.set_pilot_error(PilotErrors.FAILEDBYSERVER, diagMessage)
                    # get work attributes
                    workAttributes = messenger.get_work_attributes(workSpec)
                    retMap[workerID]["workAttributes"] = workAttributes
                    # get output files
                    filesToStageOut = messenger.get_files_to_stage_out(workSpec)
                    retMap[workerID]["filesToStageOut"] = filesToStageOut
                    # get events to update
                    if workSpec.eventsRequest in [WorkSpec.EV_useEvents, WorkSpec.EV_requestEvents]:
                        eventsToUpdate = messenger.events_to_update(workSpec)
                        retMap[workerID]["eventsToUpdate"] = eventsToUpdate
                    # request events
                    if workSpec.eventsRequest == WorkSpec.EV_useEvents:
                        eventsRequestParams = messenger.events_requested(workSpec)
                        retMap[workerID]["eventsRequestParams"] = eventsRequestParams
                    # get PandaIDs for pull model
                    if workSpec.mapType == WorkSpec.MT_NoJob:
                        pandaIDs = messenger.get_panda_ids(workSpec)
                    retMap[workerID]["pandaIDs"] = pandaIDs
                    # keep original new status
                    retMap[workerID]["monStatus"] = newStatus
                    # set running or idle while there are events to update or files to stage out
                    if newStatus in [WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled]:
                        isOK = True
                        if len(retMap[workerID]["filesToStageOut"]) > 0 or len(retMap[workerID]["eventsToUpdate"]) > 0:
                            if workSpec.status == WorkSpec.ST_running:
                                newStatus = WorkSpec.ST_running
                            else:
                                newStatus = WorkSpec.ST_idle
                        elif not workSpec.is_post_processed():
                            if (not queue_config.is_no_heartbeat_status(newStatus) and not queue_config.truePilot) or (
                                hasattr(messenger, "forcePostProcessing") and messenger.forcePostProcessing
                            ):
                                # post processing unless heartbeat is suppressed
                                jobSpecs = self.dbProxy.get_jobs_with_worker_id(workSpec.workerID, None, True, only_running=True, slim=True)
                                # post processing
                                tmpStat = messenger.post_processing(workSpec, jobSpecs, workSpec.mapType)
                                if tmpStat is None:
                                    # retry
                                    ppTimeOut = getattr(harvester_config.monitor, "postProcessTimeout", 0)
                                    if ppTimeOut > 0:
                                        timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=ppTimeOut)
                                        if workSpec.endTime is None or workSpec.endTime > timeLimit:
                                            isOK = False
                                            # set end time just in case for timeout
                                            workSpec.set_end_time()
                            if isOK:
                                workSpec.post_processed()
                            if workSpec.status == WorkSpec.ST_running:
                                newStatus = WorkSpec.ST_running
                            else:
                                newStatus = WorkSpec.ST_idle
                        # reset modification time to immediately trigger subsequent lookup
                        if isOK and not self.monitor_fifo.enabled:
                            workSpec.trigger_next_lookup()
                    retMap[workerID]["newStatus"] = newStatus
                    retMap[workerID]["diagMessage"] = diagMessage
                else:
                    tmp_log.debug("workerID={0} not in retMap".format(workerID))
            return True, retMap
        except Exception:
            core_utils.dump_error_message(tmp_log)
            return False, None

    # ask plugin for workers to update, get workspecs, and queue the event
    def monitor_event_deliverer(self, time_window):
        tmpLog = self.make_logger(_logger, "id=monitor-{0}".format(self.get_pid()), method_name="monitor_event_deliverer")
        tmpLog.debug("start")
        for mon_core in self.eventBasedMonCoreList:
            tmpLog.debug("run with {0}".format(mon_core.__class__.__name__))
            worker_update_list = mon_core.report_updated_workers(time_window=time_window)
            for workerID, updateTimestamp in worker_update_list:
                retVal = self.monitor_event_fifo.putbyid(id=workerID, item=True, score=updateTimestamp)
                if not retVal:
                    retVal = self.monitor_event_fifo.update(id=workerID, score=updateTimestamp, temporary=0, cond_score="gt")
                    if retVal:
                        tmpLog.debug("updated event with workerID={0}".format(workerID))
                    else:
                        tmpLog.debug("event with workerID={0} is updated. Skipped".format(workerID))
                else:
                    tmpLog.debug("put event with workerID={0}".format(workerID))
        tmpLog.debug("done")

    # get events and check workers
    def monitor_event_digester(self, locked_by, max_events):
        tmpLog = self.make_logger(_logger, "id=monitor-{0}".format(self.get_pid()), method_name="monitor_event_digester")
        tmpLog.debug("start")
        retMap = {}
        try:
            obj_gotten_list = self.monitor_event_fifo.getmany(mode="first", count=max_events, protective=True)
        except Exception as e:
            obj_gotten_list = []
            tmpLog.error("monitor_event_fifo excepted with {0}".format(e))
        workerID_list = [obj_gotten.id for obj_gotten in obj_gotten_list]
        tmpLog.debug("got {0} worker events".format(len(workerID_list)))
        if len(workerID_list) > 0:
            updated_workers_dict = self.dbProxy.get_workers_from_ids(workerID_list)
            tmpLog.debug("got workspecs for worker events")
            for queueName, _val in iteritems(updated_workers_dict):
                for configID, workSpecsList in iteritems(_val):
                    qc_key = (queueName, configID)
                    tmpLog.debug("checking workers of queueName={0} configID={1}".format(*qc_key))
                    try:
                        retVal = self.monitor_agent_core(locked_by, queueName, workSpecsList, from_fifo=True, config_id=configID, check_source="Event")
                    except Exception as e:
                        tmpLog.error("monitor_agent_core excepted with {0}".format(e))
                        retVal = None  # skip the loop

                    if retVal:
                        retMap[qc_key] = retVal
        tmpLog.debug("done")
        return retMap

    # remove outdated events
    def monitor_event_disposer(self, event_lifetime, max_events):
        tmpLog = self.make_logger(_logger, "id=monitor-{0}".format(self.get_pid()), method_name="monitor_event_disposer")
        tmpLog.debug("start")
        timeNow_timestamp = time.time()
        try:
            obj_gotten_list = self.monitor_event_fifo.getmany(mode="first", maxscore=(timeNow_timestamp - event_lifetime), count=max_events, temporary=True)
        except Exception as e:
            obj_gotten_list = []
            tmpLog.error("monitor_event_fifo excepted with {0}".format(e))
        tmpLog.debug("removed {0} events".format(len(obj_gotten_list)))
        try:
            n_events = self.monitor_event_fifo.size()
            tmpLog.debug("now {0} events in monitor-event fifo".format(n_events))
        except Exception as e:
            tmpLog.error("failed to get size of monitor-event fifo: {0}".format(e))
        tmpLog.debug("done")
