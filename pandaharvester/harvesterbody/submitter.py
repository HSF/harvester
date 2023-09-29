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
        self.monitor_fifo = MonitorFIFO()
        self.apfmon = Apfmon(self.queue_configMapper)

    # main loop
    def run(self):
        locked_by = "submitter-{0}".format(self.get_pid())
        monitor_fifo = self.monitor_fifo
        queue_lock_interval = getattr(harvester_config.submitter, "queueLockInterval", harvester_config.submitter.lockInterval)
        while True:
            sw_main = core_utils.get_stopwatch()
            main_log = self.make_logger(_logger, "id={0}".format(locked_by), method_name="run")
            main_log.debug("getting queues to submit workers")

            # get queues associated to a site to submit workers
            current_workers, site_name, res_map = self.dbProxy.get_queues_to_submit(
                harvester_config.submitter.nQueues,
                harvester_config.submitter.lookupTime,
                harvester_config.submitter.lockInterval,
                locked_by,
                queue_lock_interval,
            )
            submitted = False
            if site_name is not None:
                main_log.debug("got {0} queues for site {1}".format(len(current_workers), site_name))

                # get commands from panda server
                com_str = "{0}:{1}".format(CommandSpec.COM_setNWorkers, site_name)
                command_specs = self.dbProxy.get_commands_for_receiver("submitter", com_str)
                main_log.debug("got {0} {1} commands".format(len(command_specs), com_str))
                for command_spec in command_specs:
                    new_limits = self.dbProxy.set_queue_limit(site_name, command_spec.params)
                    for tmp_job_type, tmp_jt_vals in iteritems(new_limits):
                        res_map.setdefault(tmp_job_type, {})
                        for tmp_resource_type, tmp_new_val in iteritems(tmp_jt_vals):
                            # if available, overwrite new worker value with the command from panda server
                            if tmp_resource_type in res_map[tmp_job_type]:
                                tmp_queue_name = res_map[tmp_job_type][tmp_resource_type]
                                if tmp_queue_name in current_workers:
                                    current_workers[tmp_queue_name][tmp_job_type][tmp_resource_type]["nNewWorkers"] = tmp_new_val

                # define number of new workers
                if len(current_workers) == 0:
                    n_workers_per_queue_jt_rt = dict()
                else:
                    n_workers_per_queue_jt_rt = self.workerAdjuster.define_num_workers(current_workers, site_name)

                if n_workers_per_queue_jt_rt is None:
                    main_log.error("WorkerAdjuster failed to define the number of workers")
                elif len(n_workers_per_queue_jt_rt) == 0:
                    pass
                else:
                    # loop over all queues and resource types
                    for queue_name in n_workers_per_queue_jt_rt:
                        for job_type in n_workers_per_queue_jt_rt[queue_name]:
                            for resource_type in n_workers_per_queue_jt_rt[queue_name][job_type]:
                                tmp_val = n_workers_per_queue_jt_rt[queue_name][job_type][resource_type]
                                tmp_log = self.make_logger(
                                    _logger, "id={0} queue={1} jtype={2} rtype={3}".format(locked_by, queue_name, job_type, resource_type), method_name="run"
                                )
                                try:
                                    tmp_log.debug("start")
                                    tmp_log.debug("workers status: %s" % tmp_val)
                                    nWorkers = tmp_val["nNewWorkers"] + tmp_val["nReady"]
                                    nReady = tmp_val["nReady"]

                                    # check queue
                                    if not self.queue_configMapper.has_queue(queue_name):
                                        tmp_log.error("config not found")
                                        continue

                                    # no new workers
                                    if nWorkers == 0:
                                        tmp_log.debug("skipped since no new worker is needed based on current stats")
                                        continue
                                    # get queue
                                    queue_config = self.queue_configMapper.get_queue(queue_name)
                                    workerMakerCore = self.workerMaker.get_plugin(queue_config)
                                    # check if resource is ready
                                    if hasattr(workerMakerCore, "dynamicSizing") and workerMakerCore.dynamicSizing is True:
                                        numReadyResources = self.workerMaker.num_ready_resources(queue_config, job_type, resource_type, workerMakerCore)
                                        tmp_log.debug("numReadyResources: %s" % numReadyResources)
                                        if not numReadyResources:
                                            if hasattr(workerMakerCore, "staticWorkers"):
                                                nQRWorkers = tmp_val["nQueue"] + tmp_val["nRunning"]
                                                tmp_log.debug("staticWorkers: %s, nQRWorkers(Queue+Running): %s" % (workerMakerCore.staticWorkers, nQRWorkers))
                                                if nQRWorkers >= workerMakerCore.staticWorkers:
                                                    tmp_log.debug("No left static workers, skip")
                                                    continue
                                                else:
                                                    nWorkers = min(workerMakerCore.staticWorkers - nQRWorkers, nWorkers)
                                                    tmp_log.debug("staticWorkers: %s, nWorkers: %s" % (workerMakerCore.staticWorkers, nWorkers))
                                            else:
                                                tmp_log.debug("skip since no resources are ready")
                                                continue
                                        else:
                                            nWorkers = min(nWorkers, numReadyResources)
                                    # post action of worker maker
                                    if hasattr(workerMakerCore, "skipOnFail") and workerMakerCore.skipOnFail is True:
                                        skipOnFail = True
                                    else:
                                        skipOnFail = False
                                    # actions based on mapping type
                                    if queue_config.mapType == WorkSpec.MT_NoJob:
                                        # workers without jobs
                                        jobChunks = []
                                        for i in range(nWorkers):
                                            jobChunks.append([])
                                    elif queue_config.mapType == WorkSpec.MT_OneToOne:
                                        # one worker per one job
                                        jobChunks = self.dbProxy.get_job_chunks_for_workers(
                                            queue_name,
                                            nWorkers,
                                            nReady,
                                            1,
                                            None,
                                            queue_config.useJobLateBinding,
                                            harvester_config.submitter.checkInterval,
                                            harvester_config.submitter.lockInterval,
                                            locked_by,
                                        )
                                    elif queue_config.mapType == WorkSpec.MT_MultiJobs:
                                        # one worker for multiple jobs
                                        nJobsPerWorker = self.workerMaker.get_num_jobs_per_worker(
                                            queue_config, nWorkers, job_type, resource_type, maker=workerMakerCore
                                        )
                                        tmp_log.debug("nJobsPerWorker={0}".format(nJobsPerWorker))
                                        jobChunks = self.dbProxy.get_job_chunks_for_workers(
                                            queue_name,
                                            nWorkers,
                                            nReady,
                                            nJobsPerWorker,
                                            None,
                                            queue_config.useJobLateBinding,
                                            harvester_config.submitter.checkInterval,
                                            harvester_config.submitter.lockInterval,
                                            locked_by,
                                            queue_config.allowJobMixture,
                                        )
                                    elif queue_config.mapType == WorkSpec.MT_MultiWorkers:
                                        # multiple workers for one job
                                        nWorkersPerJob = self.workerMaker.get_num_workers_per_job(
                                            queue_config, nWorkers, job_type, resource_type, maker=workerMakerCore
                                        )
                                        maxWorkersPerJob = self.workerMaker.get_max_workers_per_job_in_total(
                                            queue_config, job_type, resource_type, maker=workerMakerCore
                                        )
                                        maxWorkersPerJobPerCycle = self.workerMaker.get_max_workers_per_job_per_cycle(
                                            queue_config, job_type, resource_type, maker=workerMakerCore
                                        )
                                        tmp_log.debug("nWorkersPerJob={0}".format(nWorkersPerJob))
                                        jobChunks = self.dbProxy.get_job_chunks_for_workers(
                                            queue_name,
                                            nWorkers,
                                            nReady,
                                            None,
                                            nWorkersPerJob,
                                            queue_config.useJobLateBinding,
                                            harvester_config.submitter.checkInterval,
                                            harvester_config.submitter.lockInterval,
                                            locked_by,
                                            max_workers_per_job_in_total=maxWorkersPerJob,
                                            max_workers_per_job_per_cycle=maxWorkersPerJobPerCycle,
                                        )
                                    else:
                                        tmp_log.error("unknown mapType={0}".format(queue_config.mapType))
                                        continue

                                    tmp_log.debug("got {0} job chunks".format(len(jobChunks)))
                                    if len(jobChunks) == 0:
                                        continue
                                    # make workers
                                    okChunks, ngChunks = self.workerMaker.make_workers(
                                        jobChunks, queue_config, nReady, job_type, resource_type, maker=workerMakerCore
                                    )

                                    if len(ngChunks) == 0:
                                        tmp_log.debug("successfully made {0} workers".format(len(okChunks)))
                                    else:
                                        tmp_log.debug("made {0} workers, while {1} workers failed".format(len(okChunks), len(ngChunks)))
                                    timeNow = datetime.datetime.utcnow()
                                    timeNow_timestamp = time.time()
                                    pandaIDs = set()
                                    # NG (=not good)
                                    for ngJobs in ngChunks:
                                        for job_spec in ngJobs:
                                            if skipOnFail:
                                                # release jobs when workers are not made
                                                pandaIDs.add(job_spec.PandaID)
                                            else:
                                                job_spec.status = "failed"
                                                job_spec.subStatus = "failed_to_make"
                                                job_spec.stateChangeTime = timeNow
                                                job_spec.locked_by = None
                                                errStr = "failed to make a worker"
                                                job_spec.set_pilot_error(PilotErrors.SETUPFAILURE, errStr)
                                                job_spec.trigger_propagation()
                                                self.dbProxy.update_job(job_spec, {"locked_by": locked_by, "subStatus": "prepared"})
                                    # OK
                                    work_specList = []
                                    if len(okChunks) > 0:
                                        for work_spec, okJobs in okChunks:
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
                                                    work_spec.nJobsToReFill = None
                                                    for job_spec in okJobs[work_spec.nJobsToReFill :]:
                                                        pandaIDs.add(job_spec.PandaID)
                                                work_spec.set_num_jobs_with_list()
                                            # map type
                                            work_spec.mapType = queue_config.mapType
                                            # queue name
                                            work_spec.computingSite = queue_config.queueName
                                            # set access point
                                            work_spec.accessPoint = queue_config.messenger["accessPoint"]
                                            # sync level
                                            work_spec.syncLevel = queue_config.get_synchronization_level()
                                            # events
                                            if len(okJobs) > 0 and (
                                                "eventService" in okJobs[0].jobParams or "cloneJob" in okJobs[0].jobParams or "isHPO" in okJobs[0].jobParams
                                            ):
                                                work_spec.eventsRequest = WorkSpec.EV_useEvents
                                            work_specList.append(work_spec)
                                    if len(work_specList) > 0:
                                        sw = core_utils.get_stopwatch()
                                        # get plugin for submitter
                                        submitterCore = self.pluginFactory.get_plugin(queue_config.submitter)
                                        if submitterCore is None:
                                            # not found
                                            tmp_log.error("submitter plugin for {0} not found".format(job_spec.computingSite))
                                            continue
                                        # get plugin for messenger
                                        messenger = self.pluginFactory.get_plugin(queue_config.messenger)
                                        if messenger is None:
                                            # not found
                                            tmp_log.error("messenger plugin for {0} not found".format(job_spec.computingSite))
                                            continue
                                        # setup access points
                                        messenger.setup_access_points(work_specList)
                                        # feed jobs
                                        for work_spec in work_specList:
                                            if work_spec.hasJob == 1:
                                                tmpStat = messenger.feed_jobs(work_spec, work_spec.get_jobspec_list())
                                                if tmpStat is False:
                                                    tmp_log.error("failed to send jobs to workerID={0}".format(work_spec.workerID))
                                                else:
                                                    tmp_log.debug("sent jobs to workerID={0} with {1}".format(work_spec.workerID, tmpStat))
                                        # insert workers
                                        self.dbProxy.insert_workers(work_specList, locked_by)
                                        # submit
                                        sw.reset()
                                        tmp_log.info("submitting {0} workers".format(len(work_specList)))
                                        work_specList, tmpRetList, tmpStrList = self.submit_workers(submitterCore, work_specList)
                                        tmp_log.debug("done submitting {0} workers".format(len(work_specList)) + sw.get_elapsed_time())
                                        # collect successful jobs
                                        okPandaIDs = set()
                                        for iWorker, (tmpRet, tmpStr) in enumerate(zip(tmpRetList, tmpStrList)):
                                            if tmpRet:
                                                work_spec, jobList = okChunks[iWorker]
                                                jobList = work_spec.get_jobspec_list()
                                                if jobList is not None:
                                                    for job_spec in jobList:
                                                        okPandaIDs.add(job_spec.PandaID)
                                        # loop over all workers
                                        for iWorker, (tmpRet, tmpStr) in enumerate(zip(tmpRetList, tmpStrList)):
                                            work_spec, jobList = okChunks[iWorker]
                                            # set harvesterHost
                                            work_spec.harvesterHost = socket.gethostname()
                                            # use associated job list since it can be truncated for re-filling
                                            jobList = work_spec.get_jobspec_list()
                                            # set status
                                            if not tmpRet:
                                                # failed submission
                                                errStr = "failed to submit a workerID={0} with {1}".format(work_spec.workerID, tmpStr)
                                                tmp_log.error(errStr)
                                                work_spec.set_status(WorkSpec.ST_missed)
                                                work_spec.set_dialog_message(tmpStr)
                                                work_spec.set_pilot_error(PilotErrors.SETUPFAILURE, errStr)
                                                work_spec.set_pilot_closed()
                                                if jobList is not None:
                                                    # increment attempt number
                                                    newJobList = []
                                                    for job_spec in jobList:
                                                        # skip if successful with another worker
                                                        if job_spec.PandaID in okPandaIDs:
                                                            continue
                                                        if job_spec.submissionAttempts is None:
                                                            job_spec.submissionAttempts = 0
                                                        job_spec.submissionAttempts += 1
                                                        # max attempt or permanent error
                                                        if tmpRet is False or job_spec.submissionAttempts >= queue_config.maxSubmissionAttempts:
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
                                            work_spec.submitTime = timeNow
                                            work_spec.modificationTime = timeNow
                                            work_spec.checkTime = timeNow
                                            if self.monitor_fifo.enabled:
                                                work_spec.set_work_params({"lastCheckAt": timeNow_timestamp})
                                            # prefetch events
                                            if (
                                                tmpRet
                                                and work_spec.hasJob == 1
                                                and work_spec.eventsRequest == WorkSpec.EV_useEvents
                                                and queue_config.prefetchEvents
                                            ):
                                                work_spec.eventsRequest = WorkSpec.EV_requestEvents
                                                eventsRequestParams = dict()
                                                for job_spec in jobList:
                                                    eventsRequestParams[job_spec.PandaID] = {
                                                        "pandaID": job_spec.PandaID,
                                                        "taskID": job_spec.taskID,
                                                        "jobsetID": job_spec.jobParams["jobsetID"],
                                                        "nRanges": max(int(math.ceil(work_spec.nCore / len(jobList))), job_spec.jobParams["coreCount"])
                                                        * queue_config.initEventsMultipler,
                                                    }
                                                    if "isHPO" in job_spec.jobParams:
                                                        if "sourceURL" in job_spec.jobParams:
                                                            sourceURL = job_spec.jobParams["sourceURL"]
                                                        else:
                                                            sourceURL = None
                                                        eventsRequestParams[job_spec.PandaID].update({"isHPO": True, "jobsetID": 0, "sourceURL": sourceURL})
                                                work_spec.eventsRequestParams = eventsRequestParams
                                            # register worker
                                            tmpStat = self.dbProxy.register_worker(work_spec, jobList, locked_by)
                                            if jobList is not None:
                                                for job_spec in jobList:
                                                    pandaIDs.add(job_spec.PandaID)
                                                    if tmpStat:
                                                        if tmpRet:
                                                            tmpStr = "submitted a workerID={0} for PandaID={1} with submissionHost={2} batchID={3}"
                                                            tmp_log.info(
                                                                tmpStr.format(work_spec.workerID, job_spec.PandaID, work_spec.submissionHost, work_spec.batchID)
                                                            )
                                                        else:
                                                            tmpStr = "failed to submit a workerID={0} for PandaID={1}"
                                                            tmp_log.error(tmpStr.format(work_spec.workerID, job_spec.PandaID))
                                                    else:
                                                        tmpStr = "failed to register a worker for PandaID={0} with submissionHost={1} batchID={2}"
                                                        tmp_log.error(tmpStr.format(job_spec.PandaID, work_spec.submissionHost, work_spec.batchID))
                                        # enqueue to monitor fifo
                                        if self.monitor_fifo.enabled and queue_config.mapType != WorkSpec.MT_MultiWorkers:
                                            work_specsToEnqueue = [[w] for w in work_specList if w.status in (WorkSpec.ST_submitted, WorkSpec.ST_running)]
                                            check_delay = min(
                                                getattr(harvester_config.monitor, "eventBasedCheckInterval", harvester_config.monitor.checkInterval),
                                                getattr(harvester_config.monitor, "fifoCheckInterval", harvester_config.monitor.checkInterval),
                                            )
                                            monitor_fifo.put((queue_name, work_specsToEnqueue), time.time() + check_delay)
                                            main_log.debug("put workers to monitor FIFO")
                                        submitted = True
                                    # release jobs
                                    self.dbProxy.release_jobs(pandaIDs, locked_by)
                                    tmp_log.info("done")
                                except Exception:
                                    core_utils.dump_error_message(tmp_log)
                # release the site
                self.dbProxy.release_site(site_name, locked_by)
                if sw_main.get_elapsed_time_in_sec() > queue_lock_interval:
                    main_log.warning("a submitter cycle was longer than queue_lock_interval {0} sec".format(queue_lock_interval) + sw_main.get_elapsed_time())
            main_log.debug("done")
            # define sleep interval
            if site_name is None or (hasattr(harvester_config.submitter, "respectSleepTime") and harvester_config.submitter.respectSleepTime):
                sleepTime = harvester_config.submitter.sleepTime
            else:
                sleepTime = 0
                if submitted and hasattr(harvester_config.submitter, "minSubmissionInterval"):
                    interval = harvester_config.submitter.minSubmissionInterval
                    if interval > 0:
                        newTime = datetime.datetime.utcnow() + datetime.timedelta(seconds=interval)
                        self.dbProxy.update_panda_queue_attribute("submitTime", newTime, site_name=site_name)

            # time the cycle
            main_log.debug("done a submitter cycle" + sw_main.get_elapsed_time())
            # check if being terminated
            if self.terminated(sleepTime):
                main_log.debug("terminated")
                return

    # wrapper for submitWorkers to skip ready workers
    def submit_workers(self, submitter_core, workspec_list):
        retList = []
        strList = []
        newSpecList = []
        workersToSubmit = []
        for work_spec in workspec_list:
            if work_spec.status in [WorkSpec.ST_ready, WorkSpec.ST_running]:
                newSpecList.append(work_spec)
                retList.append(True)
                strList.append("")
            else:
                workersToSubmit.append(work_spec)
        tmpRetList = submitter_core.submit_workers(workersToSubmit)

        # submit the workers to the monitoring
        self.apfmon.create_workers(workersToSubmit)

        for tmpRet, tmpStr in tmpRetList:
            retList.append(tmpRet)
            strList.append(tmpStr)
        newSpecList += workersToSubmit
        return newSpecList, retList, strList
