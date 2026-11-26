import datetime
import time

from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.command_spec import CommandSpec
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.pilot_errors import PilotErrors

# logger
_logger = core_utils.setup_logger("propagator")

STATS_PERIOD = 300
METRICS_PERIOD = 300

# job statuses with heartbeat suppressed for queues without queue config, assumed to be truepilot
TRUEPILOT_NO_HEARTBEAT_STATUSES = ["running", "transferring", "finished", "failed"]

# propagate important checkpoints to panda


class Propagator(AgentBase):
    # constructor
    def __init__(self, communicator, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.communicator = communicator
        self.queueConfigMapper = queue_config_mapper
        self._last_stats_update = None
        self._last_metrics_update = None

        # clean old service metrics at start up of the agent
        self.dbProxy.clean_service_metrics()

    # main loop
    def run(self):
        while True:
            sw_main = core_utils.get_stopwatch()
            main_log = self.make_logger(_logger, f"id={self.get_pid()}", method_name="run")
            self.run_stages(
                (
                    self.propagate_jobs,
                    self.propagate_workers,
                    self.process_commands,
                    self.report_worker_stats,
                    self.report_service_metrics,
                    self.send_dialog_messages,
                ),
                main_log,
            )
            if sw_main.get_elapsed_time_in_sec() > harvester_config.propagator.lockInterval:
                main_log.warning("a single cycle was longer than lockInterval. done" + sw_main.get_elapsed_time())
            else:
                main_log.debug("done" + sw_main.get_elapsed_time())
            # check if being terminated
            if self.terminated(harvester_config.propagator.sleepTime):
                main_log.debug("terminated")
                return

    # update jobs in central database
    def propagate_jobs(self):
        tmp_log = self.make_logger(_logger, f"id={self.get_pid()}", method_name="propagate_jobs")
        tmp_log.debug("getting jobs to propagate")
        sw = core_utils.get_stopwatch()
        job_specs = self.dbProxy.get_jobs_to_propagate(
            harvester_config.propagator.maxJobs, harvester_config.propagator.lockInterval, harvester_config.propagator.updateInterval, self.get_pid()
        )
        tmp_log.debug(f"got {len(job_specs)} jobs" + sw.get_elapsed_time())
        # statuses with heartbeat suppressed for each site
        hb_suppress_map = {}
        # a failure in one bulk does not skip the others; jobs left locked are retried after lockInterval
        for job_list in core_utils.create_shards(job_specs, harvester_config.propagator.nJobsInBulk):
            try:
                self.propagate_job_bulk(job_list, hb_suppress_map, tmp_log)
            except Exception:
                core_utils.dump_error_message(tmp_log)

    # update a bulk of jobs in central database
    def propagate_job_bulk(self, job_list, hb_suppress_map, tmp_log):
        # collect jobs to update or check, and results as (job, result, whether updated) with skipped jobs first
        results = []
        jobs_to_check = []
        jobs_to_update = []
        for job_spec in job_list:
            if job_spec.computingSite not in hb_suppress_map:
                queue_config = self.queueConfigMapper.get_queue(job_spec.computingSite, job_spec.configID)
                if queue_config:
                    hb_suppress_map[job_spec.computingSite] = queue_config.get_no_heartbeat_status()
                else:
                    hb_suppress_map[job_spec.computingSite] = TRUEPILOT_NO_HEARTBEAT_STATUSES
            # heartbeat is suppressed
            if job_spec.get_status() in hb_suppress_map[job_spec.computingSite] and not job_spec.not_suppress_heartbeat():
                # check running job to detect lost heartbeat
                if job_spec.status == "running":
                    jobs_to_check.append(job_spec)
                else:
                    results.append((job_spec, {"StatusCode": 0, "command": None}, False))
            else:
                jobs_to_update.append(job_spec)
        # the communicator returns one result per job
        sw = core_utils.get_stopwatch()
        if jobs_to_check:
            check_results = self.communicator.check_jobs(jobs_to_check)
            tmp_log.debug(f"check_jobs for {len(jobs_to_check)} jobs" + sw.get_elapsed_time())
            results += [(job_spec, ret_map, False) for job_spec, ret_map in zip(jobs_to_check, check_results)]
        sw.reset()
        if jobs_to_update:
            update_results = self.communicator.update_jobs(jobs_to_update, self.get_pid())
            tmp_log.debug(f"update_jobs for {len(jobs_to_update)} jobs took" + sw.get_elapsed_time())
            results += [(job_spec, ret_map, True) for job_spec, ret_map in zip(jobs_to_update, update_results)]
        # a failure in one job does not skip the others
        for job_spec, ret_map, is_updated in results:
            try:
                self.handle_job_result(job_spec, ret_map, is_updated, tmp_log)
            except Exception:
                core_utils.dump_error_message(tmp_log)

    # handle the result of updating or checking a job
    def handle_job_result(self, job_spec, ret_map, is_updated, tmp_log):
        if ret_map.get("StatusCode") != 0:
            tmp_log.error(f"failed to update PandaID={job_spec.PandaID} status={job_spec.status}")
            return
        if is_updated:
            tmp_log.debug(f"updated PandaID={job_spec.PandaID} status={job_spec.status}")
        else:
            tmp_log.debug(f"skip updating PandaID={job_spec.PandaID} status={job_spec.status}")
        # release job
        job_spec.propagatorLock = None
        if job_spec.is_final_status() and job_spec.status == job_spec.get_status():
            # unset to disable further updating
            job_spec.propagatorTime = None
            job_spec.subStatus = "done"
            job_spec.modificationTime = core_utils.naive_utcnow()
        elif job_spec.is_final_status() and not job_spec.all_events_done():
            # trigger next propagation to update remaining events
            job_spec.trigger_propagation()
        else:
            command = ret_map.get("command")
            # check event availability
            if job_spec.status == "starting" and "eventService" in job_spec.jobParams and job_spec.subStatus != "submitted":
                ev_stat, ev_ret = self.communicator.check_event_availability(job_spec)
                if ev_stat:
                    if ev_ret is not None:
                        job_spec.nRemainingEvents = ev_ret
                    if ev_ret == 0:
                        tmp_log.debug(f"kill PandaID={job_spec.PandaID} due to no event")
                        command = "tobekilled"
            # got kill command
            if command == "tobekilled":
                n_workers = self.dbProxy.mark_workers_to_kill_by_pandaid(job_spec.PandaID)
                if n_workers == 0:
                    # no workers
                    job_spec.status = "cancelled"
                    job_spec.subStatus = "killed"
                    job_spec.set_pilot_error(PilotErrors.PANDAKILL, PilotErrors.pilot_error_msg[PilotErrors.PANDAKILL])
                    job_spec.stateChangeTime = core_utils.naive_utcnow()
                    job_spec.trigger_propagation()
        self.dbProxy.update_job(job_spec, {"propagatorLock": self.get_pid()}, update_out_file=True)

    # update workers in central database
    def propagate_workers(self):
        tmp_log = self.make_logger(_logger, f"id={self.get_pid()}", method_name="propagate_workers")
        tmp_log.debug("getting workers to propagate")
        sw = core_utils.get_stopwatch()
        work_specs = self.dbProxy.get_workers_to_propagate(harvester_config.propagator.maxWorkers, harvester_config.propagator.updateInterval)
        tmp_log.debug(f"got {len(work_specs)} workers" + sw.get_elapsed_time())
        sw.reset()
        # a failure in one bulk does not skip the others
        for work_list in core_utils.create_shards(work_specs, harvester_config.propagator.nWorkersInBulk):
            try:
                self.propagate_worker_bulk(work_list, tmp_log)
            except Exception:
                core_utils.dump_error_message(tmp_log)
        tmp_log.debug(f"update_workers for {len(work_specs)} workers took" + sw.get_elapsed_time())

    # update a bulk of workers in central database
    def propagate_worker_bulk(self, work_list, tmp_log):
        # the communicator returns one result per worker, or None on failure
        ret_list, err_str = self.communicator.update_workers(work_list)
        if ret_list is None:
            tmp_log.error(f"failed to update workers with {err_str}")
            return
        # a failure in one worker does not skip the others
        for work_spec, ret in zip(work_list, ret_list):
            try:
                self.handle_worker_result(work_spec, ret, tmp_log)
            except Exception:
                core_utils.dump_error_message(tmp_log)

    # handle the result of updating a worker
    def handle_worker_result(self, work_spec, ret, tmp_log):
        if not ret:
            tmp_log.error(f"failed to update workerID={work_spec.workerID} status={work_spec.status}")
            return
        tmp_log.debug(f"updated workerID={work_spec.workerID} status={work_spec.status}")
        # upload logs; a failed upload is retried in the next update
        for log_file_path, log_offset, log_size, log_remote_name in work_spec.get_log_files_to_upload():
            try:
                with open(log_file_path, "rb") as log_file_obj:
                    tmp_stat, tmp_err = self.communicator.upload_file(log_remote_name, log_file_obj, log_offset, log_size)
                if tmp_stat:
                    work_spec.update_log_files_to_upload(log_file_path, log_offset + log_size)
            except Exception:
                core_utils.dump_error_message(tmp_log)
        # disable further update
        if work_spec.is_final_status():
            work_spec.disable_propagation()
        self.dbProxy.update_worker(work_spec, {"workerID": work_spec.workerID})

    # process commands to report worker stats
    def process_commands(self):
        tmp_log = self.make_logger(_logger, f"id={self.get_pid()}", method_name="process_commands")
        tmp_log.debug("getting commands")
        command_specs = self.dbProxy.get_commands_for_receiver("propagator")
        tmp_log.debug(f"got {len(command_specs)} commands")
        for command_spec in command_specs:
            if not command_spec.command.startswith(CommandSpec.COM_reportWorkerStats):
                continue
            # get worker stats
            site_name = command_spec.command.split(":")[-1]
            worker_stats = self.dbProxy.get_worker_stats(site_name)
            if len(worker_stats) == 0:
                tmp_log.error(f"failed to get worker stats for {site_name}")
                continue
            # report worker stats
            tmp_ret, tmp_str = self.communicator.update_worker_stats(site_name, worker_stats)
            if tmp_ret:
                tmp_log.debug(f"updated worker stats (command) for {site_name}")
            else:
                tmp_log.error(f"failed to update worker stats (command) for {site_name} err={tmp_str}")

    # report worker stats of all sites periodically
    def report_worker_stats(self):
        if self._last_stats_update and time.time() - self._last_stats_update <= STATS_PERIOD:
            return
        tmp_log = self.make_logger(_logger, f"id={self.get_pid()}", method_name="report_worker_stats")
        # get active UPS queues. PanDA server needs to know about them and which harvester instance is taking care of them
        active_ups_queues = self.queueConfigMapper.get_active_ups_queues()
        # update worker stats for all sites
        worker_stats_bulk = self.dbProxy.get_worker_stats_bulk(active_ups_queues)
        if not worker_stats_bulk:
            tmp_log.error("failed to get worker stats in bulk")
            return
        for site_name, worker_stats in worker_stats_bulk.items():
            tmp_ret, tmp_str = self.communicator.update_worker_stats(site_name, worker_stats)
            if tmp_ret:
                tmp_log.debug(f"update of worker stats (bulk) for {site_name}")
                self._last_stats_update = time.time()
            else:
                tmp_log.error(f"failed to update worker stats (bulk) for {site_name} err={tmp_str}")

    # report service metrics periodically
    def report_service_metrics(self):
        if self._last_metrics_update and core_utils.naive_utcnow() - self._last_metrics_update <= datetime.timedelta(seconds=METRICS_PERIOD):
            return
        tmp_log = self.make_logger(_logger, f"id={self.get_pid()}", method_name="report_service_metrics")
        # get latest metrics from DB
        service_metrics_list = self.dbProxy.get_service_metrics(self._last_metrics_update)
        if not service_metrics_list:
            if self._last_metrics_update:
                tmp_log.error("failed to get service metrics")
            self._last_metrics_update = core_utils.naive_utcnow()
            return
        tmp_ret, tmp_str = self.communicator.update_service_metrics(service_metrics_list)
        if tmp_ret:
            tmp_log.debug("update of service metrics OK")
            self._last_metrics_update = core_utils.naive_utcnow()
        else:
            tmp_log.error(f"failed to update service metrics err={tmp_str}")

    # send dialog messages
    def send_dialog_messages(self):
        tmp_log = self.make_logger(_logger, f"id={self.get_pid()}", method_name="send_dialog_messages")
        tmp_log.debug("getting dialog messages to propagate")
        max_dialogs = getattr(harvester_config.propagator, "maxDialogs", 50)
        diag_specs = self.dbProxy.get_dialog_messages_to_send(max_dialogs, harvester_config.propagator.lockInterval)
        tmp_log.debug(f"got {len(diag_specs)} dialogs")
        if not diag_specs:
            return
        tmp_stat, tmp_str = self.communicator.send_dialog_messages(diag_specs)
        if tmp_stat:
            self.dbProxy.delete_dialog_messages([diag_spec.diagID for diag_spec in diag_specs])
            tmp_log.debug(f"sent {len(diag_specs)} dialogs")
        else:
            tmp_log.error(f"failed to send dialogs err={tmp_str}")
