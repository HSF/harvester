import datetime
import math
import socket

from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterbody.monitor import get_schedule_config
from pandaharvester.harvesterbody.worker_adjuster import (
    DEFAULT_JOB_TYPE,
    WorkerAdjuster,
)
from pandaharvester.harvesterbody.worker_maker import WorkerMaker
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.command_spec import CommandSpec
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.pilot_errors import PilotErrors
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestermisc.apfmon import Apfmon

# logger
_logger = core_utils.setup_logger("submitter")


# class to submit workers
class Submitter(AgentBase):
    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.queue_configMapper = queue_config_mapper
        self.dbProxy = DBProxy()
        self.workerMaker = WorkerMaker()
        self.workerAdjuster = WorkerAdjuster(queue_config_mapper)
        self.pluginFactory = PluginFactory()
        # delay of the first monitor check of new workers
        self.first_check_delay = get_schedule_config()[0].first_check_delay
        self.apfmon = Apfmon(self.queue_configMapper)

    # main loop
    def run(self):
        locked_by = f"submitter-{self.get_pid()}"
        queue_lock_interval = getattr(harvester_config.submitter, "queueLockInterval", harvester_config.submitter.lockInterval)
        while True:
            sw_main = core_utils.get_stopwatch()
            main_log = self.make_logger(_logger, f"id={locked_by}", method_name="run")
            main_log.debug("getting queues to submit workers")

            # get queues associated to a site to submit workers
            current_workers, site_name, res_map = self.dbProxy.get_queues_to_submit(harvester_config.submitter.lookupTime, locked_by, queue_lock_interval)
            submitted = False
            if site_name is not None:
                main_log.debug(f"got {len(current_workers)} queues for site {site_name}")
                try:
                    # get commands from panda server
                    self.apply_set_n_workers_commands(site_name, current_workers, res_map, main_log)
                    # define number of new workers
                    if len(current_workers) == 0:
                        n_workers_per_queue_jt_rt = dict()
                    else:
                        n_workers_per_queue_jt_rt = self.workerAdjuster.define_num_workers(current_workers, site_name)
                    if n_workers_per_queue_jt_rt is None:
                        main_log.error("WorkerAdjuster failed to define the number of workers")
                    else:
                        # loop over all queues
                        for queue_name, n_workers_per_jt_rt in n_workers_per_queue_jt_rt.items():
                            if self.submit_for_queue(queue_name, n_workers_per_jt_rt, locked_by):
                                submitted = True
                except Exception:
                    core_utils.dump_error_message(main_log)
                finally:
                    # release the site
                    self.dbProxy.release_site(site_name, locked_by)
                if sw_main.get_elapsed_time_in_sec() > queue_lock_interval:
                    main_log.warning(f"a submitter cycle was longer than queue_lock_interval {queue_lock_interval} sec" + sw_main.get_elapsed_time())
            main_log.debug("done")
            # define sleep interval
            if site_name is None or getattr(harvester_config.submitter, "respectSleepTime", False):
                sleepTime = harvester_config.submitter.sleepTime
            else:
                sleepTime = 0
                interval = getattr(harvester_config.submitter, "minSubmissionInterval", 0)
                if submitted and interval > 0:
                    newTime = core_utils.naive_utcnow() + datetime.timedelta(seconds=interval)
                    self.dbProxy.update_panda_queue_attribute("submitTime", newTime, site_name=site_name)

            # time the cycle
            main_log.debug("done a submitter cycle" + sw_main.get_elapsed_time())
            # check if being terminated
            if self.terminated(sleepTime):
                main_log.debug("terminated")
                return

    # apply setNWorkers commands from panda server to the numbers of new workers
    def apply_set_n_workers_commands(self, site_name, current_workers, res_map, tmp_log):
        com_str = f"{CommandSpec.COM_setNWorkers}:{site_name}"
        command_specs = self.dbProxy.get_commands_for_receiver("submitter", com_str)
        tmp_log.debug(f"got {len(command_specs)} {com_str} commands")
        for command_spec in command_specs:
            new_limits = self.dbProxy.set_queue_limit(site_name, command_spec.params)
            for tmp_jt_vals in new_limits.values():
                res_map.setdefault(DEFAULT_JOB_TYPE, {})
                for tmp_resource_type, tmp_new_val in tmp_jt_vals.items():
                    # if available, overwrite new worker value with the command from panda server
                    if tmp_resource_type in res_map[DEFAULT_JOB_TYPE]:
                        tmp_queue_name = res_map[DEFAULT_JOB_TYPE][tmp_resource_type]
                        if tmp_queue_name in current_workers:
                            # pilot_type "ANY" to collect all nNewWorkers from the command
                            current_workers[tmp_queue_name][DEFAULT_JOB_TYPE][tmp_resource_type]["ANY"]["nNewWorkers"] = tmp_new_val

    # submit workers of a queue for all resource types and pilot types; return whether any worker was submitted
    def submit_for_queue(self, queue_name, n_workers_per_jt_rt, locked_by):
        job_type = DEFAULT_JOB_TYPE
        tmp_log = self.make_logger(_logger, f"id={locked_by} queue={queue_name}", method_name="submit_for_queue")
        try:
            # get queue and plugins; skip the queue before locking any job if one is missing
            queue_config = self.queue_configMapper.get_queue(queue_name)
            if queue_config is None:
                tmp_log.error("config not found")
                return False
            plugins = {
                "workerMaker": self.workerMaker.get_plugin(queue_config),
                "submitter": self.pluginFactory.get_plugin(queue_config.submitter),
                "messenger": self.pluginFactory.get_plugin(queue_config.messenger),
            }
            for plugin_name, plugin in plugins.items():
                if plugin is None:
                    tmp_log.error(f"{plugin_name} plugin not found")
                    return False
        except Exception:
            core_utils.dump_error_message(tmp_log)
            return False
        submitted = False
        # loop over resource types and pilot types
        for resource_type, n_workers_per_pt in n_workers_per_jt_rt.get(job_type, {}).items():
            for pilot_type, tmp_val in n_workers_per_pt.items():
                tmp_log = self.make_logger(
                    _logger,
                    f"id={locked_by} queue={queue_name} jtype={job_type} rtype={resource_type} ptype={pilot_type}",
                    method_name="submit_for_queue",
                )
                try:
                    if self.submit_for_resource_type(queue_config, plugins, resource_type, pilot_type, tmp_val, locked_by, tmp_log):
                        submitted = True
                except Exception:
                    core_utils.dump_error_message(tmp_log)
        return submitted

    # submit workers of a queue for a resource type and pilot type; return whether workers were submitted
    def submit_for_resource_type(self, queue_config, plugins, resource_type, pilot_type, tmp_val, locked_by, tmp_log):
        job_type = DEFAULT_JOB_TYPE
        worker_maker_core = plugins["workerMaker"]
        tmp_log.debug("start")
        tmp_log.debug(f"workers status: {tmp_val}")
        nReady = tmp_val["nReady"]
        nWorkers = self.get_num_workers_to_make(worker_maker_core, tmp_val, tmp_log)
        if nWorkers <= 0:
            return False
        # get job chunks; jobs are locked from here
        jobChunks = self.get_job_chunks(queue_config, worker_maker_core, nWorkers, nReady, locked_by, tmp_log)
        tmp_log.debug(f"got {len(jobChunks)} job chunks")
        if len(jobChunks) == 0:
            return False
        # jobs to release at the end
        pandaIDs = set()
        try:
            # make workers
            okChunks, ngChunks = self.workerMaker.make_workers(
                jobChunks,
                queue_config,
                nReady,
                job_type,
                resource_type,
                prod_source_label=core_utils.special_pilot_type_to_prod_source_label(pilot_type),
                maker=worker_maker_core,
            )
            if len(ngChunks) == 0:
                tmp_log.debug(f"successfully made {len(okChunks)} workers")
            else:
                tmp_log.debug(f"made {len(okChunks)} workers, while {len(ngChunks)} workers failed")
            timeNow = core_utils.naive_utcnow()
            self.handle_failed_chunks(ngChunks, getattr(worker_maker_core, "skipOnFail", False) is True, timeNow, pandaIDs, locked_by)
            work_specList = self.prepare_workers(okChunks, queue_config, pandaIDs)
            if len(work_specList) == 0:
                return False
            # setup access points and feed jobs
            messenger = plugins["messenger"]
            messenger.setup_access_points(work_specList)
            for work_spec in work_specList:
                if work_spec.hasJob == 1:
                    tmpStat = messenger.feed_jobs(work_spec, work_spec.get_jobspec_list())
                    if tmpStat is False:
                        tmp_log.error(f"failed to send jobs to workerID={work_spec.workerID}")
                    else:
                        tmp_log.debug(f"sent jobs to workerID={work_spec.workerID} with {tmpStat}")
            # submit
            sw = core_utils.get_stopwatch()
            tmp_log.info(f"submitting {len(work_specList)} workers")
            result_list = self.submit_workers(plugins["submitter"], work_specList, tmp_log)
            tmp_log.debug(f"done submitting {len(result_list)} workers" + sw.get_elapsed_time())
            # jobs of successful workers
            okPandaIDs = set()
            for work_spec, tmpRet, _ in result_list:
                if tmpRet:
                    for job_spec in work_spec.get_jobspec_list() or []:
                        okPandaIDs.add(job_spec.PandaID)
            # register workers; a failure of one worker doesn't stop the others
            for work_spec, tmpRet, tmpStr in result_list:
                try:
                    self.register_submitted_worker(work_spec, tmpRet, tmpStr, okPandaIDs, queue_config, timeNow, pandaIDs, locked_by, tmp_log)
                except Exception:
                    core_utils.dump_error_message(tmp_log)
                    self.handle_unregistered_worker(work_spec, tmpRet, pandaIDs, tmp_log)
            return True
        finally:
            # release jobs
            self.dbProxy.release_jobs(pandaIDs, locked_by)
            tmp_log.info("done")

    # get the number of workers to make, considering ready resources for dynamic sizing
    def get_num_workers_to_make(self, worker_maker_core, tmp_val, tmp_log):
        nWorkers = tmp_val["nNewWorkers"] + tmp_val["nReady"]
        # no new workers
        if nWorkers <= 0:
            tmp_log.debug("skipped since no new worker is needed based on current stats")
            return 0
        # check if resource is ready
        if getattr(worker_maker_core, "dynamicSizing", False) is not True:
            return nWorkers
        numReadyResources = worker_maker_core.num_ready_resources()
        tmp_log.debug(f"numReadyResources: {numReadyResources}")
        if numReadyResources:
            return min(nWorkers, numReadyResources)
        if not hasattr(worker_maker_core, "staticWorkers"):
            tmp_log.debug("skip since no resources are ready")
            return 0
        nQRWorkers = tmp_val["nQueue"] + tmp_val["nRunning"]
        tmp_log.debug(f"staticWorkers: {worker_maker_core.staticWorkers}, nQRWorkers(Queue+Running): {nQRWorkers}")
        if nQRWorkers >= worker_maker_core.staticWorkers:
            tmp_log.debug("No left static workers, skip")
            return 0
        nWorkers = min(worker_maker_core.staticWorkers - nQRWorkers, nWorkers)
        tmp_log.debug(f"staticWorkers: {worker_maker_core.staticWorkers}, nWorkers: {nWorkers}")
        return nWorkers

    # get job chunks for workers according to the mapping type; jobs in the chunks are locked by locked_by
    def get_job_chunks(self, queue_config, worker_maker_core, n_workers, n_ready, locked_by, tmp_log):
        map_type = queue_config.mapType
        if map_type == WorkSpec.MT_NoJob:
            # workers without jobs
            return [[] for _ in range(n_workers)]
        common_args = {
            "queue_name": queue_config.queueName,
            "n_workers": n_workers,
            "n_ready": n_ready,
            "use_job_late_binding": queue_config.useJobLateBinding,
            "check_interval": harvester_config.submitter.checkInterval,
            "lock_interval": harvester_config.submitter.lockInterval,
            "locked_by": locked_by,
        }
        if map_type == WorkSpec.MT_OneToOne:
            # one worker per one job
            return self.dbProxy.get_job_chunks_for_workers(n_jobs_per_worker=1, n_workers_per_job=None, **common_args)
        if map_type == WorkSpec.MT_MultiJobs:
            # one worker for multiple jobs
            nJobsPerWorker = worker_maker_core.get_num_jobs_per_worker(n_workers)
            tmp_log.debug(f"nJobsPerWorker={nJobsPerWorker}")
            return self.dbProxy.get_job_chunks_for_workers(
                n_jobs_per_worker=nJobsPerWorker, n_workers_per_job=None, allow_job_mixture=queue_config.allowJobMixture, **common_args
            )
        if map_type == WorkSpec.MT_MultiWorkers:
            # multiple workers for one job
            nWorkersPerJob = worker_maker_core.get_num_workers_per_job(n_workers)
            tmp_log.debug(f"nWorkersPerJob={nWorkersPerJob}")
            return self.dbProxy.get_job_chunks_for_workers(
                n_jobs_per_worker=None,
                n_workers_per_job=nWorkersPerJob,
                max_workers_per_job_in_total=worker_maker_core.get_max_workers_per_job_in_total(),
                max_workers_per_job_per_cycle=worker_maker_core.get_max_workers_per_job_per_cycle(),
                **common_args,
            )
        tmp_log.error(f"unknown mapType={map_type}")
        return []

    # handle jobs of chunks for which no worker was made: release them if skip_on_fail, otherwise fail them
    def handle_failed_chunks(self, ng_chunks, skip_on_fail, time_now, panda_ids, locked_by):
        for ngJobs in ng_chunks:
            for job_spec in ngJobs:
                if skip_on_fail:
                    # release jobs when workers are not made
                    panda_ids.add(job_spec.PandaID)
                else:
                    job_spec.status = "failed"
                    job_spec.subStatus = "failed_to_make"
                    job_spec.stateChangeTime = time_now
                    job_spec.locked_by = None
                    job_spec.set_pilot_error(PilotErrors.SETUPFAILURE, "failed to make a worker")
                    job_spec.trigger_propagation()
                    self.dbProxy.update_job(job_spec, {"locked_by": locked_by, "subStatus": "prepared"})

    # set jobs and queue attributes to workers made; jobs not given to workers are added to panda_ids to release
    def prepare_workers(self, ok_chunks, queue_config, panda_ids):
        work_specList = []
        for work_spec, okJobs in ok_chunks:
            # has job
            if (queue_config.useJobLateBinding and work_spec.workerID is None) or queue_config.mapType == WorkSpec.MT_NoJob:
                work_spec.hasJob = 0
            else:
                work_spec.hasJob = 1
                if work_spec.nJobsToReFill in [None, 0]:
                    work_spec.set_jobspec_list(okJobs)
                else:
                    # refill free slots during the worker is running
                    work_spec.set_jobspec_list(okJobs[: work_spec.nJobsToReFill])
                    for job_spec in okJobs[work_spec.nJobsToReFill :]:
                        panda_ids.add(job_spec.PandaID)
                    work_spec.nJobsToReFill = None
                work_spec.set_num_jobs_with_list()
            # map type
            work_spec.mapType = queue_config.mapType
            # queue name
            work_spec.computingSite = queue_config.queueName
            # master queue for subqueues
            work_spec.masterQueue = queue_config.masterQueue
            # set access point
            work_spec.accessPoint = queue_config.messenger["accessPoint"]
            # sync level
            work_spec.syncLevel = queue_config.get_synchronization_level()
            # events
            if len(okJobs) > 0 and ("eventService" in okJobs[0].jobParams or "cloneJob" in okJobs[0].jobParams or "isHPO" in okJobs[0].jobParams):
                work_spec.eventsRequest = WorkSpec.EV_useEvents
            work_specList.append(work_spec)
        return work_specList

    # set the status of a worker after submission and register it with its jobs; jobs of the worker are added to panda_ids to release
    def register_submitted_worker(self, work_spec, tmp_ret, tmp_str, ok_panda_ids, queue_config, time_now, panda_ids, locked_by, tmp_log):
        # set harvesterHost
        work_spec.harvesterHost = socket.gethostname()
        # use associated job list since it can be truncated for re-filling
        jobList = work_spec.get_jobspec_list()
        # set status
        if not tmp_ret:
            # failed submission
            errStr = f"failed to submit a workerID={work_spec.workerID} with {tmp_str}"
            tmp_log.error(errStr)
            work_spec.set_status(WorkSpec.ST_missed)
            work_spec.set_dialog_message(tmp_str)
            work_spec.set_pilot_error(PilotErrors.SETUPFAILURE, errStr)
            work_spec.set_pilot_closed()
            if jobList is not None:
                # increment attempt number
                newJobList = []
                for job_spec in jobList:
                    # skip if successful with another worker
                    if job_spec.PandaID in ok_panda_ids:
                        continue
                    if job_spec.submissionAttempts is None:
                        job_spec.submissionAttempts = 0
                    job_spec.submissionAttempts += 1
                    # max attempt or permanent error
                    if tmp_ret is False or job_spec.submissionAttempts >= queue_config.maxSubmissionAttempts:
                        newJobList.append(job_spec)
                    else:
                        self.dbProxy.increment_submission_attempt(job_spec.PandaID, job_spec.submissionAttempts)
                jobList = newJobList
        elif queue_config.useJobLateBinding and work_spec.hasJob == 1:
            # directly go to running after feeding jobs for late biding
            work_spec.set_status(WorkSpec.ST_running)
        else:
            # normal successful submission
            work_spec.set_status(WorkSpec.ST_submitted)
        work_spec.submitTime = time_now
        work_spec.modificationTime = time_now
        work_spec.checkTime = time_now
        # prefetch events
        if tmp_ret and work_spec.hasJob == 1 and work_spec.eventsRequest == WorkSpec.EV_useEvents and queue_config.prefetchEvents:
            work_spec.eventsRequest = WorkSpec.EV_requestEvents
            eventsRequestParams = dict()
            for job_spec in jobList:
                eventsRequestParams[job_spec.PandaID] = {
                    "pandaID": job_spec.PandaID,
                    "taskID": job_spec.taskID,
                    "jobsetID": job_spec.jobParams["jobsetID"],
                    "nRanges": max(int(math.ceil(work_spec.nCore / len(jobList))), job_spec.jobParams["coreCount"]) * queue_config.initEventsMultipler,
                }
                if "isHPO" in job_spec.jobParams:
                    eventsRequestParams[job_spec.PandaID].update({"isHPO": True, "jobsetID": 0, "sourceURL": job_spec.jobParams.get("sourceURL")})
            work_spec.eventsRequestParams = eventsRequestParams
        # register worker
        tmpStat = self.dbProxy.register_worker(work_spec, jobList, locked_by, first_check_time=time_now + datetime.timedelta(seconds=self.first_check_delay))
        batch_str = f"submissionHost={work_spec.submissionHost} batchID={work_spec.batchID}"
        if not tmpStat:
            self.handle_unregistered_worker(work_spec, tmp_ret, panda_ids, tmp_log)
        for job_spec in jobList or []:
            panda_ids.add(job_spec.PandaID)
            if not tmpStat:
                tmp_log.error(f"failed to register a worker for PandaID={job_spec.PandaID} with {batch_str}")
            elif tmp_ret:
                tmp_log.info(f"submitted a workerID={work_spec.workerID} for PandaID={job_spec.PandaID} with {batch_str}")
            else:
                tmp_log.error(f"failed to submit a workerID={work_spec.workerID} for PandaID={job_spec.PandaID}")

    # handle a worker which failed to be registered: release all its jobs, and report it if it was submitted since it is unknown to harvester
    def handle_unregistered_worker(self, work_spec, tmp_ret, panda_ids, tmp_log):
        for job_spec in work_spec.get_jobspec_list() or []:
            panda_ids.add(job_spec.PandaID)
        if tmp_ret:
            tmp_log.error(
                f"workerID={work_spec.workerID} was submitted but failed to be registered, so it is not managed by harvester; "
                f"submissionHost={work_spec.submissionHost} batchID={work_spec.batchID}"
            )
        else:
            tmp_log.error(f"workerID={work_spec.workerID} failed to be registered after failed submission")

    # submit workers with the submitter plugin, skipping ready and running workers
    # return a list of (workspec, result, message), where result is True for success, False for a permanent error, None for a temporary error
    def submit_workers(self, submitter_core, workspec_list, tmp_log):
        result_list = []
        workersToSubmit = []
        for work_spec in workspec_list:
            if work_spec.status in [WorkSpec.ST_ready, WorkSpec.ST_running]:
                result_list.append((work_spec, True, ""))
            else:
                workersToSubmit.append(work_spec)
        tmpRetList = submitter_core.submit_workers(workersToSubmit) or []

        # submit the workers to the monitoring
        self.apfmon.create_workers(workersToSubmit)

        if len(tmpRetList) != len(workersToSubmit):
            # results cannot be matched to workers
            errStr = f"submitter plugin returned {len(tmpRetList)} results for {len(workersToSubmit)} workers"
            tmp_log.error(errStr)
            tmpRetList = [(None, errStr)] * len(workersToSubmit)
        for work_spec, (tmpRet, tmpStr) in zip(workersToSubmit, tmpRetList):
            result_list.append((work_spec, tmpRet, tmpStr))
        return result_list
