import datetime
import itertools
import random
import time
import types

from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.pilot_errors import PilotErrors
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.worker_check_spec import WorkerCheckSpec
from pandaharvester.harvestermisc.apfmon import Apfmon
from pandaharvester.harvestermonitor.monitor_common import PartialReportError

# logger
_logger = core_utils.setup_logger("monitor")

# worker statuses to check
_active_statuses = (WorkSpec.ST_submitted, WorkSpec.ST_running, WorkSpec.ST_idle)

# monitored statuses of workers ended on the resource
_final_mon_statuses = (WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled)

# fraction of the interval to randomize next check time, to spread the load
_jitter_fraction = 0.1

# number of workers sharing the same random jitter, so that they are released with one statement
_jitter_group_size = 50

# process lock names
_event_lock_name = "monitor_event_deliverer"
_reconcile_lock_name = "monitor_reconciler"

# max age in sec of the worker target cache of a target host since its last refresh, before deleting it
_target_cache_max_age = 86400

# cache keys of the time of the last successful event delivery
_event_delivery_cache_keys = ("_monitor_event_last_delivery", "_universal")

# obsolete monitor parameters, mostly removed with the fifo, to warn about
_removed_params = (
    "sleepTime",
    "fifoEnable",
    "fifoSleepTimeMilli",
    "fifoCheckInterval",
    "fifoCheckDuration",
    "fifoForceEnqueueInterval",
    "fifoMaxWorkersToPopulate",
    "fifoMaxWorkersPerChunk",
    "fifoProtectiveDequeue",
    "fifoModule",
    "fifoClass",
    "eventBasedCheckMaxEvents",
    "eventBasedEventLifetime",
    "eventBasedRemoveMaxEvents",
)

# monitor parameters renamed, as (old, new)
_renamed_params = (
    ("fifoMaxCheckInterval", "maxCheckInterval"),
    ("fifoMaxPreemptInterval", "postProcessPriorityInterval"),
)


def get_schedule_config():
    """
    Get parameters of the worker check schedule from the monitor section of harvester configuration.
    Return (config, deprecation_messages); config is a namespace.
    """
    mc = harvester_config.monitor
    messages = []
    # renamed parameters
    for old_name, new_name in _renamed_params:
        if hasattr(mc, old_name):
            if hasattr(mc, new_name):
                messages.append(f"monitor.{old_name} is deprecated and ignored since monitor.{new_name} is set")
            else:
                messages.append(f"monitor.{old_name} is deprecated; used as monitor.{new_name}. Please rename it")
    # check interval; the fifo check interval was the effective one when fifo was enabled
    check_interval = mc.checkInterval
    used_params = set()
    if getattr(mc, "fifoEnable", False) and hasattr(mc, "fifoCheckInterval"):
        check_interval = mc.fifoCheckInterval
        used_params.add("fifoCheckInterval")
        messages.append(
            f"monitor.fifoCheckInterval is deprecated; used as monitor.checkInterval={check_interval} since monitor.fifoEnable=True. "
            "Please set monitor.checkInterval instead"
        )
    # removed parameters
    for name in _removed_params:
        if hasattr(mc, name) and name not in used_params:
            messages.append(f"monitor.{name} is obsolete and ignored")
    if hasattr(harvester_config, "fifo"):
        messages.append("[fifo] section is obsolete and ignored")
    config = types.SimpleNamespace()
    config.jitter_fraction = _jitter_fraction
    config.check_interval = check_interval
    config.max_check_interval = getattr(mc, "maxCheckInterval", getattr(mc, "fifoMaxCheckInterval", 0))
    # check interval of queues without their own, capped by maxCheckInterval
    if config.max_check_interval > 0:
        config.default_check_interval = min(check_interval, config.max_check_interval)
    else:
        config.default_check_interval = check_interval
    config.check_retry_interval = getattr(mc, "checkRetryInterval", min(300, check_interval))
    config.lock_interval = mc.lockInterval
    config.max_workers = mc.maxWorkers
    config.check_timeout = getattr(mc, "checkTimeout", None)
    config.worker_queue_time_limit = getattr(mc, "workerQueueTimeLimit", 172800)
    config.post_process_timeout = getattr(mc, "postProcessTimeout", 0)
    config.worker_max_lifetime_hours = getattr(mc, "workerMaxLifetimeHours", 720)
    config.idle_sleep_time = getattr(mc, "idleSleepTime", 10)
    config.idle_log_interval = getattr(mc, "idleLogInterval", 300)
    config.reconcile_interval = getattr(mc, "reconcileInterval", 600)
    config.stale_check_age = getattr(mc, "staleCheckAge", 3 * check_interval)
    config.postproc_priority_interval = getattr(mc, "postProcessPriorityInterval", getattr(mc, "fifoMaxPreemptInterval", 60))
    config.event_based_enable = getattr(mc, "eventBasedEnable", False)
    config.event_check_interval = getattr(mc, "eventBasedCheckInterval", 300)
    config.event_time_window = getattr(mc, "eventBasedTimeWindow", 450)
    config.event_max_time_window = max(getattr(mc, "eventBasedMaxTimeWindow", 3600), config.event_time_window)
    # delay of the first check of a new worker
    if config.event_based_enable:
        config.first_check_delay = min(config.event_check_interval, check_interval)
    else:
        config.first_check_delay = check_interval
    # how far in the future a schedule can be; beyond is regarded as wrong and made due now by reconcile
    if config.max_check_interval > 0:
        config.max_schedule_ahead = (1 + _jitter_fraction) * max(config.max_check_interval, config.check_retry_interval)
    else:
        config.max_schedule_ahead = 86400
    return config, messages


def make_check_result(work_spec, status, diag_message="", n_jobs_to_refill=None):
    """
    Make the check result of a worker, in the format of check_workers
    """
    return {
        "oldStatus": work_spec.status,
        "newStatus": status,
        "monStatus": status,
        "workAttributes": None,
        "filesToStageOut": [],
        "eventsRequestParams": {},
        "eventsToUpdate": [],
        "diagMessage": diag_message,
        "pandaIDs": [],
        "nJobsToReFill": n_jobs_to_refill,
        "isChecked": True,
    }


# check workers and update their status
class Monitor(AgentBase):
    # whether reconcile ran in this process, shared by all monitor agents
    reconciledInProcess = False
    # time of the last cleanup of the worker target cache in this process
    lastTargetCacheCleanupTimestamp = 0

    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        tmp_log = self.make_logger(_logger, method_name="__init__")
        AgentBase.__init__(self, single_mode)
        self.queueConfigMapper = queue_config_mapper
        self.dbProxy = DBProxy()
        self.pluginFactory = PluginFactory()
        self.apfmon = Apfmon(self.queueConfigMapper)
        # schedule parameters
        self.config, deprecation_messages = get_schedule_config()
        for message in deprecation_messages:
            tmp_log.warning(message)
        # queues already warned about deprecated plugin parameters
        self.deprecationWarnedQueues = set()
        # plugins for event-based check
        self.eventBasedMonCoreList = []
        if self.config.event_based_enable:
            for pluginConf in harvester_config.monitor.eventBasedPlugins:
                plugin_key = self.pluginFactory.get_plugin_key(pluginConf)
                try:
                    self.eventBasedMonCoreList.append(self.pluginFactory.get_plugin(pluginConf))
                except Exception:
                    tmp_log.error(f"failed to launch event-based-monitor plugin of {plugin_key}")
                    core_utils.dump_error_message(tmp_log)

    # main loop
    def run(self):
        lockedBy = f"monitor-{self.get_pid()}"
        mainLog = self.make_logger(_logger, f"id={lockedBy}", method_name="run")
        # init messengers
        for queueName, queueConfig in self.queueConfigMapper.get_all_queues().items():
            # just import for module initialization
            try:
                self.pluginFactory.get_plugin(queueConfig.messenger)
            except Exception:
                mainLog.error(f"failed to launch messenger plugin for {queueName}")
                core_utils.dump_error_message(mainLog)
        # main
        last_reconcile_timestamp = 0
        last_event_delivery_timestamp = 0
        # consecutive cycles without worker to check, logged only once in a while
        n_idle_cycles = 0
        last_idle_log_timestamp = 0
        while True:
            sw_main = core_utils.get_stopwatch()
            # make the schedule consistent with workers; one agent at a time
            if time.time() >= last_reconcile_timestamp + self.config.reconcile_interval:
                last_reconcile_timestamp = time.time()
                if self.dbProxy.get_process_lock(_reconcile_lock_name, lockedBy, self.config.reconcile_interval):
                    self.reconcile_schedule()
                else:
                    mainLog.debug("did not get lock. Skip reconcile_schedule")
            # make workers updated on the resource due now; one agent at a time
            if self.eventBasedMonCoreList and time.time() >= last_event_delivery_timestamp + self.config.event_check_interval:
                last_event_delivery_timestamp = time.time()
                if self.dbProxy.get_process_lock(_event_lock_name, lockedBy, self.config.event_check_interval):
                    self.monitor_event_deliverer()
                else:
                    mainLog.debug("did not get lock. Skip monitor_event_deliverer")
            # check due workers
            n_claimed = self.check_due_workers(lockedBy)
            if n_claimed > 0:
                n_idle_cycles = 0
                mainLog.debug("done a monitor cycle" + sw_main.get_elapsed_time())
            else:
                n_idle_cycles += 1
                if n_idle_cycles == 1 or time.time() >= last_idle_log_timestamp + self.config.idle_log_interval:
                    last_idle_log_timestamp = time.time()
                    mainLog.debug(
                        f"no worker to check in {n_idle_cycles} consecutive cycles; logged at most every {self.config.idle_log_interval} sec while idle"
                    )
            # go on at once while there can be more due workers
            if n_claimed >= self.config.max_workers:
                sleep_time = 0
            else:
                sleep_time = self.config.idle_sleep_time
            # check if being terminated
            if self.terminated(sleep_time):
                mainLog.debug("terminated")
                return

    # get check interval of workers of a queue
    def get_check_interval(self, mon_core, queue_name, tmp_log):
        check_interval = getattr(mon_core, "checkInterval", None)
        if check_interval is None:
            check_interval = getattr(mon_core, "fifoCheckInterval", None)
            if check_interval is not None and queue_name not in self.deprecationWarnedQueues:
                self.deprecationWarnedQueues.add(queue_name)
                tmp_log.warning("monitor plugin parameter fifoCheckInterval is deprecated; used as checkInterval. Please rename it")
        if check_interval is None:
            return self.config.default_check_interval
        # maxCheckInterval caps the interval, also for workers whose queue configuration predates a change
        if self.config.max_check_interval > 0:
            check_interval = min(check_interval, self.config.max_check_interval)
        return check_interval

    # make an item to release a worker with its next schedule
    def make_release_item(self, check_spec, check_start, outcome, interval, jitter, postproc=False):
        """
        jitter is the random fraction of the interval added to next check time
        outcome is one of:
        - "final": worker in final status; delete its schedule
        - "checked": checked successfully
        - "failed": failed to check its status on the resource
        - "skipped": not to check in its status
        - "retry": not checked due to a problem in harvester
        postproc is True when the worker ended on the resource or was killed, with post-processing or stage-out pending
        """
        if outcome == "final":
            return {"workerID": check_spec.workerID, "delete": True}
        item = {
            "workerID": check_spec.workerID,
            "delete": False,
            "checkedOK": outcome == "checked",
            "firstFailTime": check_spec.firstFailTime,
            "lastEventTime": check_spec.lastEventTime,
            "priority": WorkerCheckSpec.PRIO_ROUTINE,
            "priorityTime": None,
        }
        if outcome == "failed" and item["firstFailTime"] is None:
            item["firstFailTime"] = check_start
        # post-processing goes first, for a limited time
        if postproc:
            if check_spec.priority == WorkerCheckSpec.PRIO_POSTPROC and check_spec.priorityTime is not None:
                postproc_start = check_spec.priorityTime
            else:
                postproc_start = check_start
            if (check_start - postproc_start).total_seconds() < self.config.postproc_priority_interval:
                item.update(
                    nextCheckTime=check_start,
                    priority=WorkerCheckSpec.PRIO_POSTPROC,
                    priorityTime=postproc_start,
                    lastTrigger=WorkerCheckSpec.TRIG_postproc,
                )
                return item
        # next check later
        if outcome in ("failed", "retry"):
            interval = self.config.check_retry_interval
            item["lastTrigger"] = WorkerCheckSpec.TRIG_retry
        else:
            item["lastTrigger"] = WorkerCheckSpec.TRIG_poll
        item["nextCheckTime"] = check_start + datetime.timedelta(seconds=interval * (1 + jitter))
        return item

    # claim due workers, check them, and release them with next schedule. Return the number of workers claimed
    def check_due_workers(self, locked_by):
        mainLog = self.make_logger(_logger, f"id={locked_by}", method_name="check_due_workers")
        lock_token, workspecs_per_queue, check_specs = self.dbProxy.claim_workers_to_check(locked_by, self.config.max_workers, self.config.lock_interval)
        if lock_token is None:
            mainLog.error("failed to claim workers to check")
            return 0
        if not check_specs:
            return 0
        sw = core_utils.get_stopwatch()
        check_start = core_utils.naive_utcnow()
        max_lag = max((check_start - check_spec.nextCheckTime).total_seconds() for check_spec in check_specs.values())
        mainLog.debug(f"claimed {len(check_specs)} workers; max lag {max_lag:.0f} sec")
        release_list = []
        jitter = 0
        n_outcome = {}
        for queueName, per_config in workspecs_per_queue.items():
            for configID, workSpecsList in per_config.items():
                tmpQueLog = self.make_logger(_logger, f"id={locked_by} queue={queueName}", method_name="check_due_workers")
                # queue configuration and monitor plugin; None if the queue configuration is missing
                queueConfig = None
                monCore = None
                check_interval = self.config.default_check_interval
                got_plugin = True
                if self.queueConfigMapper.has_queue(queueName, configID):
                    try:
                        queueConfig = self.queueConfigMapper.get_queue(queueName, configID)
                        monCore = self.pluginFactory.get_plugin(queueConfig.monitor)
                        check_interval = self.get_check_interval(monCore, queueName, tmpQueLog)
                    except Exception:
                        got_plugin = False
                        tmpQueLog.error("failed to get queue configuration or monitor plugin")
                        core_utils.dump_error_message(tmpQueLog)
                # sort out workers to check
                outcome_map = {}
                workSpecsToCheck = []
                for workSpecs in workSpecsList:
                    workSpec = workSpecs[0]
                    if workSpec.status in _active_statuses:
                        workSpecsToCheck.append(workSpecs)
                    elif workSpec.is_final_status():
                        outcome_map[workSpec.workerID] = ("final", False)
                    else:
                        outcome_map[workSpec.workerID] = ("skipped", False)
                # check workers
                if workSpecsToCheck:
                    retMap = None
                    if got_plugin:
                        try:
                            retMap = self.check_and_update_workers(locked_by, queueName, workSpecsToCheck, queueConfig, monCore, check_specs)
                        except Exception as e:
                            tmpQueLog.error(f"check_and_update_workers excepted with {e}")
                            core_utils.dump_error_message(tmpQueLog)
                    for workSpecs in workSpecsToCheck:
                        workerID = workSpecs[0].workerID
                        outcome_map[workerID] = ("retry", False) if retMap is None else retMap.get(workerID, ("retry", False))
                # release items
                for workerID, (outcome, postproc) in outcome_map.items():
                    # the same jitter for each group of workers, so that they are released with one statement
                    if len(release_list) % _jitter_group_size == 0:
                        jitter = random.uniform(-_jitter_fraction, _jitter_fraction)
                    release_list.append(self.make_release_item(check_specs[workerID], check_start, outcome, check_interval, jitter, postproc))
                    n_outcome[outcome] = n_outcome.get(outcome, 0) + 1
                    if postproc:
                        n_outcome["postproc"] = n_outcome.get("postproc", 0) + 1
        # release workers
        if not self.dbProxy.release_checked_workers(lock_token, check_start, release_list):
            mainLog.error("failed to release workers; they will be checked again after lockInterval")
        if sw.get_elapsed_time_in_sec() > self.config.lock_interval:
            mainLog.warning(f"checking {len(check_specs)} workers was longer than lockInterval " + sw.get_elapsed_time())
        mainLog.info(f"checked {len(check_specs)} workers with outcomes {n_outcome}; max lag {max_lag:.0f} sec" + sw.get_elapsed_time())
        return len(check_specs)

    # core of monitor agent to check workers in workSpecsList of queueName
    def check_and_update_workers(self, lockedBy, queueName, workSpecsList, queueConfig, monCore, check_specs):
        """
        Check workers and update them in DB. queueConfig and monCore are None if the queue configuration is missing.
        Return {workerID: (outcome, postproc)} as arguments of make_release_item, or None if not checked at all.
        """
        tmpQueLog = self.make_logger(_logger, f"id={lockedBy} queue={queueName}", method_name="check_and_update_workers")
        allWorkers = [item for sublist in workSpecsList for item in sublist]
        if queueConfig is None:
            tmpQueLog.error("config not found")
            # nothing can check the workers; end the ones too old to keep waiting for the queue configuration
            tmpRetMap = self.end_unconfigured_workers(allWorkers, tmpQueLog)
            if not tmpRetMap:
                return None
            workSpecsList = [workSpecs for workSpecs in workSpecsList if workSpecs[0].workerID in tmpRetMap]
            messenger = None
        else:
            messenger = self.pluginFactory.get_plugin(queueConfig.messenger)
            # check workers
            tmpQueLog.debug(f"checking {len(allWorkers)} workers")
            tmpStat, tmpRetMap = self.check_workers(monCore, messenger, allWorkers, queueConfig, tmpQueLog, check_specs)
            if not tmpStat:
                tmpQueLog.error("failed to check workers")
                return None
        # loop over all worker chunks
        tmpQueLog.debug("update jobs and workers")
        retMap = {}
        for workSpecs in workSpecsList:
            jobSpecs = None
            pandaIDsList = []
            eventsToUpdateList = []
            filesToStageOutList = dict()
            mapType = workSpecs[0].mapType
            chunk_outcome_map = {}
            # loop over workSpecs
            for workSpec in workSpecs:
                tmpLog = self.make_logger(_logger, f"id={lockedBy} workerID={workSpec.workerID}", method_name="check_and_update_workers")
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
                # check status
                if newStatus not in WorkSpec.ST_LIST:
                    tmpLog.error(f"unknown status={newStatus}. Keep status={workSpec.status}")
                    newStatus = workSpec.status
                    monStatus = workSpec.status
                    isChecked = False
                # update worker
                workSpec.set_status(newStatus)
                workSpec.set_work_attributes(workAttributes)
                workSpec.set_dialog_message(diagMessage)
                if monStatus == WorkSpec.ST_failed:
                    if not workSpec.has_pilot_error() and workSpec.errorCode is None:
                        workSpec.set_pilot_error(PilotErrors.GENERALERROR, diagMessage)
                elif monStatus == WorkSpec.ST_cancelled:
                    if not workSpec.has_pilot_error() and workSpec.errorCode is None:
                        workSpec.set_pilot_error(PilotErrors.PANDAKILL, diagMessage)
                # final on the resource or killed
                endedOnResource = monStatus in _final_mon_statuses
                if endedOnResource:
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
                    tmpQueLog.debug(f"newStatus: {newStatus} monStatus: {monStatus} oldStatus: {oldStatus} workSpecStatus: {workSpec.status}")
                    self.apfmon.update_worker(workSpec, monStatus)
                # outcome
                if workSpec.is_final_status():
                    chunk_outcome_map[workSpec.workerID] = ("final", False)
                else:
                    # post-processing or stage-out is pending when ended on the resource
                    chunk_outcome_map[workSpec.workerID] = ("checked" if isChecked else "failed", endedOnResource)
            # update jobs and workers
            if jobSpecs is not None and len(jobSpecs) > 0:
                tmpQueLog.debug(f"updating {len(jobSpecs)} jobs with {len(workSpecs)} workers")
                core_utils.update_job_attributes_with_workers(mapType, jobSpecs, workSpecs, filesToStageOutList, eventsToUpdateList)
            # update local database
            tmpRet = self.dbProxy.update_jobs_workers(jobSpecs, workSpecs, lockedBy, pandaIDsList)
            if not tmpRet:
                for workSpec in workSpecs:
                    tmpLog = self.make_logger(_logger, f"id={lockedBy} workerID={workSpec.workerID}", method_name="check_and_update_workers")
                    tmpLog.error("failed to update the DB. Will retry")
                    chunk_outcome_map[workSpec.workerID] = ("retry", False)
            else:
                if jobSpecs is not None:
                    for jobSpec in jobSpecs:
                        tmpLog = self.make_logger(_logger, f"id={lockedBy} PandaID={jobSpec.PandaID}", method_name="check_and_update_workers")
                        tmpLog.debug(f"new status={jobSpec.status} subStatus={jobSpec.subStatus} status_in_metadata={jobSpec.get_job_status_from_attributes()}")
            # send ACK to workers for events and files
            if messenger is not None and (len(eventsToUpdateList) > 0 or len(filesToStageOutList) > 0):
                for workSpec in workSpecs:
                    try:
                        messenger.acknowledge_events_files(workSpec)
                    except Exception:
                        core_utils.dump_error_message(tmpQueLog)
                        tmpQueLog.error(f"failed to send ACK to workerID={workSpec.workerID}")
            retMap.update(chunk_outcome_map)
        tmpQueLog.debug("done")
        return retMap

    # end workers older than workerMaxLifetimeHours without checking them, when their queue configuration is gone
    def end_unconfigured_workers(self, all_workers, tmp_log):
        """
        Return {workerID: check result} in the format of check_workers, only for workers to end
        """
        retMap = {}
        if not self.config.worker_max_lifetime_hours:
            return retMap
        timeLimit = core_utils.naive_utcnow() - datetime.timedelta(hours=self.config.worker_max_lifetime_hours)
        for workSpec in all_workers:
            if workSpec.creationTime is None or workSpec.creationTime > timeLimit:
                continue
            diagMessage = f"Cancelled by Harvester since queue configuration is missing and worker is older than {self.config.worker_max_lifetime_hours} hours"
            tmp_log.warning(f"cancel workerID={workSpec.workerID} created at {workSpec.creationTime} without killing it, since queue configuration is missing")
            if not workSpec.has_pilot_error():
                workSpec.set_pilot_error(PilotErrors.FAILEDBYSERVER, diagMessage)
            retMap[workSpec.workerID] = make_check_result(workSpec, WorkSpec.ST_cancelled, diagMessage)
        return retMap

    # wrapper for checkWorkers
    def check_workers(self, mon_core, messenger, all_workers, queue_config, tmp_log, check_specs):
        """
        check_specs is {workerID: WorkerCheckSpec}, giving the start of consecutive check failures before this check
        """
        checkTimeout = getattr(mon_core, "checkTimeout", self.config.check_timeout)
        workerQueueTimeLimit = self.config.worker_queue_time_limit
        workersToCheck = []
        workerIDsToKill = set()
        thingsToPostProcess = []
        retMap = dict()
        for workSpec in all_workers:
            workStatus = None
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
            retMap[workSpec.workerID] = make_check_result(workSpec, workStatus, n_jobs_to_refill=nJobsToReFill)
        # check workers
        tmp_log.debug("checking workers with plugin")
        try:
            if workersToCheck:
                tmpStat, tmpOut = mon_core.check_workers(workersToCheck)
                if not tmpStat:
                    tmp_log.error(f"failed to check workers with: {tmpOut}")
                    for unchecked in workersToCheck:
                        retMap[unchecked.workerID].update(
                            newStatus=unchecked.status,
                            monStatus=unchecked.status,
                            isChecked=False,
                            workAttributes=unchecked.workAttributes or {},
                            diagMessage=f"WORKER_STATE_UNVERIFIED: monitor query failed: {tmpOut}",
                        )
                    workersToCheck = []
                    tmpOut = []
                else:
                    tmp_log.debug("checked")
            else:
                tmp_log.debug("Nothing to be checked with plugin")
                tmpOut = []
            timeNow = core_utils.naive_utcnow()
            for workSpec, (newStatus, diagMessage) in itertools.chain(zip(workersToCheck, tmpOut), thingsToPostProcess):
                workerID = workSpec.workerID
                tmp_log.debug(f"Going to check workerID={workerID}")
                pandaIDs = []
                if workerID in retMap:
                    # failed to check status
                    if newStatus is None:
                        tmp_log.warning(f"Failed to check workerID={workerID} with {diagMessage}")
                        retMap[workerID]["isChecked"] = False
                        # set status when checks keep failing for longer than checkTimeout
                        firstFailTime = check_specs[workerID].firstFailTime
                        if firstFailTime is not None and checkTimeout is not None and timeNow - firstFailTime > datetime.timedelta(seconds=checkTimeout):
                            if getattr(mon_core, "preserveUnknownWorkers", False) and getattr(workSpec, "nativeStatus", None) == "unknown":
                                if workSpec.killTime is not None and workSpec.killTime <= timeNow:
                                    # Harvester has already ordered the kill, so the scheduler's silence ends the worker
                                    tmp_log.debug(f"end workerID={workerID}: killed by Harvester and its batch job no longer in the scheduler")
                                    newStatus = WorkSpec.ST_cancelled
                                    diagMessage = "Killed by Harvester; batch job no longer in the scheduler. " + diagMessage
                                    if not workSpec.has_pilot_error():
                                        workSpec.set_pilot_error(PilotErrors.FAILEDBYSERVER, diagMessage)
                                else:
                                    tmp_log.error(
                                        f"WORKER_STATE_UNVERIFIED workerID={workerID}: "
                                        f"scheduler state unknown beyond checkTimeout; "
                                        f"retaining {workSpec.status}; {diagMessage}"
                                    )
                                    newStatus = workSpec.status
                            else:
                                # kill due to timeout
                                tmp_log.debug(f"kill workerID={workerID} due to consecutive check failures since {firstFailTime}")
                                workerIDsToKill.add(workerID)
                                newStatus = WorkSpec.ST_cancelled
                                diagMessage = "Killed by Harvester due to consecutive worker check failures. " + diagMessage
                                workSpec.set_pilot_error(PilotErrors.FAILEDBYSERVER, diagMessage)
                        else:
                            # use original status
                            newStatus = workSpec.status
                    # request kill
                    if messenger.kill_requested(workSpec):
                        tmp_log.debug(f"kill workerID={workerID} as requested")
                        workerIDsToKill.add(workerID)
                    # stuck queuing for too long
                    if workSpec.status == WorkSpec.ST_submitted and timeNow > workSpec.submitTime + datetime.timedelta(seconds=workerQueueTimeLimit):
                        tmp_log.debug(f"kill workerID={workerID} due to queuing longer than {workerQueueTimeLimit} seconds")
                        workerIDsToKill.add(workerID)
                        diagMessage = "Killed by Harvester due to worker queuing too long. " + diagMessage
                        workSpec.set_pilot_error(PilotErrors.FAILEDBYSERVER, diagMessage)
                        # set closed
                        workSpec.set_pilot_closed()
                    # alive for too long
                    if (
                        self.config.worker_max_lifetime_hours
                        and newStatus not in _final_mon_statuses
                        and workSpec.creationTime is not None
                        and timeNow > workSpec.creationTime + datetime.timedelta(hours=self.config.worker_max_lifetime_hours)
                    ):
                        tmp_log.warning(
                            f"kill workerID={workerID} created at {workSpec.creationTime} due to living longer than {self.config.worker_max_lifetime_hours} hours"
                        )
                        workerIDsToKill.add(workerID)
                        newStatus = WorkSpec.ST_cancelled
                        diagMessage = "Killed by Harvester due to worker living longer than workerMaxLifetimeHours. " + diagMessage
                        workSpec.set_pilot_error(PilotErrors.FAILEDBYSERVER, diagMessage)
                    # expired heartbeat - only when requested in the configuration
                    try:
                        # check if the queue configuration requires checking for worker heartbeat
                        worker_heartbeat_limit = int(queue_config.messenger["worker_heartbeat"])
                    except (AttributeError, KeyError):
                        worker_heartbeat_limit = None
                    tmp_log.debug(f"workerID={workerID} heartbeat limit is configured to {worker_heartbeat_limit}")
                    if worker_heartbeat_limit:
                        if messenger.is_alive(workSpec, worker_heartbeat_limit):
                            tmp_log.debug(f"heartbeat for workerID={workerID} is valid")
                        else:
                            tmp_log.debug(f"heartbeat for workerID={workerID} expired: sending kill request")
                            workerIDsToKill.add(workerID)
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
                    if newStatus in _final_mon_statuses:
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
                                    ppTimeOut = self.config.post_process_timeout
                                    if ppTimeOut > 0:
                                        timeLimit = core_utils.naive_utcnow() - datetime.timedelta(minutes=ppTimeOut)
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
                    retMap[workerID]["newStatus"] = newStatus
                    retMap[workerID]["diagMessage"] = diagMessage
                else:
                    tmp_log.debug(f"workerID={workerID} not in retMap")
            # request kill with one statement
            if workerIDsToKill:
                self.dbProxy.mark_workers_to_kill_by_workerids(sorted(workerIDsToKill))
            return True, retMap
        except Exception:
            core_utils.dump_error_message(tmp_log)
            return False, None

    # ask plugins for workers updated on the resource, and make them due now
    def monitor_event_deliverer(self):
        tmpLog = self.make_logger(_logger, f"id=monitor-{self.get_pid()}", method_name="monitor_event_deliverer")
        tmpLog.debug("start")
        timeNow_timestamp = time.time()
        # look back further when the last delivery was long ago, e.g. after an outage
        time_window = self.config.event_time_window
        cacheSpec = self.dbProxy.get_cache(*_event_delivery_cache_keys, from_local_cache=False)
        if cacheSpec is not None:
            try:
                last_delivery_timestamp = float(cacheSpec.data)
                overlap = max(self.config.event_time_window - self.config.event_check_interval, 0)
                time_window = max(time_window, timeNow_timestamp - last_delivery_timestamp + overlap)
            except Exception:
                tmpLog.warning(f"invalid time of last delivery: {cacheSpec.data}")
        if time_window > self.config.event_max_time_window:
            tmpLog.warning(f"last delivery was long ago; look back only {self.config.event_max_time_window} sec instead of {time_window:.0f} sec")
            time_window = self.config.event_max_time_window
        # get events
        event_list = []
        all_ok = True
        for mon_core in self.eventBasedMonCoreList:
            tmpLog.debug(f"run with {mon_core.__class__.__name__} within {time_window:.0f} sec")
            try:
                for workerID, updateTimestamp in mon_core.report_updated_workers(time_window=time_window):
                    event_list.append((workerID, core_utils.naive_utcfromtimestamp(updateTimestamp)))
            except PartialReportError as e:
                # take what was got, but look back from the last full delivery next time
                all_ok = False
                tmpLog.error(f"got updated workers only partially with {mon_core.__class__.__name__}: {e}")
                for workerID, updateTimestamp in e.workers:
                    event_list.append((workerID, core_utils.naive_utcfromtimestamp(updateTimestamp)))
            except Exception:
                all_ok = False
                tmpLog.error(f"failed to get updated workers with {mon_core.__class__.__name__}")
                core_utils.dump_error_message(tmpLog)
        # make workers due
        n_applied = self.dbProxy.apply_worker_events(event_list)
        tmpLog.info(f"applied {n_applied} new events out of {len(event_list)} reported")
        # record the time of the delivery
        if all_ok:
            self.dbProxy.refresh_cache(*_event_delivery_cache_keys, f"{timeNow_timestamp:.3f}")
        tmpLog.debug("done")

    # make the schedule consistent with workers in DB
    def reconcile_schedule(self):
        tmpLog = self.make_logger(_logger, f"id=monitor-{self.get_pid()}", method_name="reconcile_schedule")
        tmpLog.debug("start")
        retVal = self.dbProxy.reconcile_worker_checks(
            self.config.default_check_interval, self.config.max_schedule_ahead, self.config.stale_check_age, jitter_fraction=self.config.jitter_fraction
        )
        if retVal is None:
            tmpLog.error("failed to reconcile")
            return
        tmpLog.info(f"done with {retVal}")
        # added schedules are expected only when starting with an empty or outdated schedule
        if retVal["n_inserted"] > 0 and Monitor.reconciledInProcess:
            tmpLog.warning(f"added schedules of {retVal['n_inserted']} workers missing one")
        if retVal["n_clamped"] > 0:
            tmpLog.warning(f"made due now {retVal['n_clamped']} workers scheduled too far in the future")
        if retVal["n_stale"] > 0:
            tmpLog.warning(f"{retVal['n_stale']} active workers not checked for longer than {self.config.stale_check_age} sec")
        Monitor.reconciledInProcess = True
        # delete the cache of target hosts no longer refreshed, e.g. removed from configuration; once in a while
        if time.time() >= Monitor.lastTargetCacheCleanupTimestamp + _target_cache_max_age:
            Monitor.lastTargetCacheCleanupTimestamp = time.time()
            n_deleted = self.dbProxy.clean_worker_target_cache(_target_cache_max_age)
            tmpLog.debug(f"deleted {n_deleted} rows of worker target cache not refreshed for {_target_cache_max_age} sec")
