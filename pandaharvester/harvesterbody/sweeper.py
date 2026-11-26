import datetime
import os

from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.command_spec import CommandSpec
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory

# logger
_logger = core_utils.setup_logger("sweeper")


# get durations in hours to keep workers in each final status before cleanup
def get_status_timeout_map():
    return {
        "finished": harvester_config.sweeper.keepFinished,
        "failed": harvester_config.sweeper.keepFailed,
        "cancelled": harvester_config.sweeper.keepCancelled,
        "missed": getattr(harvester_config.sweeper, "keepMissed", 24),
    }


# class for cleanup
class Sweeper(AgentBase):
    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.queueConfigMapper = queue_config_mapper
        self.pluginFactory = PluginFactory()
        self.lockedBy = None

    # main loop
    def run(self):
        self.lockedBy = f"sweeper-{self.get_pid()}"
        while True:
            sw_main = core_utils.get_stopwatch()
            main_log = self.make_logger(_logger, f"id={self.lockedBy}", method_name="run")
            self.run_stages(
                (
                    self.process_kill_commands,
                    self.kill_marked_workers,
                    self.clean_up_workers,
                    self.delete_old_jobs,
                    self.clean_up_disk,
                ),
                main_log,
            )
            # time the cycle
            main_log.debug("done a sweeper cycle" + sw_main.get_elapsed_time())
            # check if being terminated
            if self.terminated(harvester_config.sweeper.sleepTime):
                main_log.debug("terminated")
                return

    # process commands for marking workers that need to be killed
    def process_kill_commands(self):
        tmp_log = self.make_logger(_logger, f"id={self.lockedBy}", method_name="process_kill_commands")
        for command_string, mark_workers_to_kill in (
            # KILL_WORKER commands that were sent to panda server and forwarded to harvester; params are a query on workers
            (CommandSpec.COM_killWorkers, self.dbProxy.mark_workers_to_kill_by_query),
            # SYNC_WORKERS_KILL commands from comparing worker status provided by pilot and harvester; params are workerIDs
            (CommandSpec.COM_syncWorkersKill, self.dbProxy.mark_workers_to_kill_by_workerids),
        ):
            sw = core_utils.get_stopwatch()
            command_specs = self.dbProxy.get_commands_for_receiver("sweeper", command_string)
            tmp_log.debug(f"got {len(command_specs)} {command_string} commands")
            for command_spec in command_specs:
                n_to_kill = mark_workers_to_kill(command_spec.params)
                tmp_log.debug(f"will kill {n_to_kill} workers with {command_spec.params}")
            tmp_log.debug(f"done handling {command_string} commands" + sw.get_elapsed_time())

    # kill workers marked to be killed
    def kill_marked_workers(self):
        tmp_log = self.make_logger(_logger, f"id={self.lockedBy}", method_name="kill_marked_workers")
        sw = core_utils.get_stopwatch()
        workers_to_kill = self.dbProxy.get_workers_to_kill(harvester_config.sweeper.maxWorkers, harvester_config.sweeper.checkInterval)
        tmp_log.debug(f"got {len(workers_to_kill)} queues to kill workers")
        self.run_per_queue(workers_to_kill, "kill_marked_workers", self.kill_queue_workers)
        tmp_log.debug("done all killing" + sw.get_elapsed_time())

    # clean up workers in final status, and delete them from DB
    def clean_up_workers(self):
        tmp_log = self.make_logger(_logger, f"id={self.lockedBy}", method_name="clean_up_workers")
        sw = core_utils.get_stopwatch()
        workers_for_cleanup = self.dbProxy.get_workers_for_cleanup(harvester_config.sweeper.maxWorkers, get_status_timeout_map())
        tmp_log.debug(f"got {len(workers_for_cleanup)} queues for workers cleanup")
        self.run_per_queue(workers_for_cleanup, "clean_up_workers", self.clean_up_queue_workers, on_unconfigured=self.delete_unconfigured_workers)
        tmp_log.debug("done all cleanup" + sw.get_elapsed_time())

    # run action(queue_config, workspec_list, tmp_log) for the workers of each queue and configID in {queue_name: {configID: [workspec, ...]}}
    # a failure with one queue does not skip the others; workers whose queue config is gone are passed to on_unconfigured if given
    def run_per_queue(self, workers_map, method_name, action, on_unconfigured=None):
        for queue_name, config_id_workspec_map in workers_map.items():
            for config_id, workspec_list in config_id_workspec_map.items():
                queue_log = self.make_logger(_logger, f"queue={queue_name} configID={config_id}", method_name=method_name)
                if not self.queueConfigMapper.has_queue(queue_name, config_id):
                    queue_log.error("queue config not found")
                    if on_unconfigured is not None:
                        on_unconfigured(workspec_list, queue_log)
                    continue
                sw = core_utils.get_stopwatch()
                try:
                    action(self.queueConfigMapper.get_queue(queue_name, config_id), workspec_list, queue_log)
                except Exception:
                    core_utils.dump_error_message(queue_log)
                queue_log.debug(f"done with {len(workspec_list)} workers" + sw.get_elapsed_time())

    # kill workers of a queue with the sweeper plugin
    def kill_queue_workers(self, queue_config, workspec_list, tmp_log):
        self.kill_workers(self.pluginFactory.get_plugin(queue_config.sweeper), workspec_list, tmp_log)

    # clean up workers of a queue with the sweeper and messenger plugins, and delete swept workers from DB
    def clean_up_queue_workers(self, queue_config, workspec_list, tmp_log):
        sweeper_core = self.pluginFactory.get_plugin(queue_config.sweeper)
        messenger = self.pluginFactory.get_plugin(queue_config.messenger)
        # make sure workers to clean up are all terminated; go on with cleanup even if it fails
        tmp_log.debug("making sure workers to clean up are all terminated")
        try:
            self.kill_workers(sweeper_core, workspec_list, tmp_log)
        except Exception:
            core_utils.dump_error_message(tmp_log)
        # clean up
        for workspec in workspec_list:
            worker_log = self.make_logger(_logger, f"workerID={workspec.workerID}", method_name="clean_up_queue_workers")
            try:
                worker_log.debug("start cleaning up one worker")
                tmp_stat, tmp_out = sweeper_core.sweep_worker(workspec)
                worker_log.debug(f"swept_worker with status={tmp_stat} diag={tmp_out}")
                mc_tmp_stat, mc_tmp_out = messenger.clean_up(workspec)
                worker_log.debug(f"messenger cleaned up with status={mc_tmp_stat} diag={mc_tmp_out}")
                # delete from DB only when swept, to retry later otherwise
                if tmp_stat:
                    self.dbProxy.delete_worker(workspec.workerID)
            except Exception:
                core_utils.dump_error_message(worker_log)

    # kill workers with the sweeper plugin, with the bulk method kill_workers if the plugin has it, otherwise with kill_worker one by one
    def kill_workers(self, sweeper_core, workspec_list, tmp_log):
        if hasattr(sweeper_core, "kill_workers"):
            ret_list = sweeper_core.kill_workers(workspec_list) or []
            if len(ret_list) != len(workspec_list):
                # results cannot be matched to workers
                err_str = f"sweeper plugin returned {len(ret_list)} results for {len(workspec_list)} workers"
                tmp_log.error(err_str)
                ret_list = [(False, err_str)] * len(workspec_list)
        else:
            ret_list = []
            for workspec in workspec_list:
                try:
                    # unpack here, so that an invalid result such as None counts as a failure of the worker
                    tmp_stat, tmp_out = sweeper_core.kill_worker(workspec)
                except Exception:
                    worker_log = self.make_logger(_logger, f"workerID={workspec.workerID}", method_name="kill_workers")
                    tmp_stat, tmp_out = False, core_utils.dump_error_message(worker_log)
                ret_list.append((tmp_stat, tmp_out))
        for workspec, (tmp_stat, tmp_out) in zip(workspec_list, ret_list):
            tmp_log.debug(f"done killing workerID={workspec.workerID} with status={tmp_stat} diag={tmp_out}")
        n_killed = sum(1 for tmp_stat, _ in ret_list if tmp_stat)
        tmp_log.debug(f"killed {n_killed}/{len(workspec_list)} workers")

    # delete workers to clean up whose queue configuration is gone, without cleanup on the resource or of messenger files
    def delete_unconfigured_workers(self, workspec_list, tmp_log):
        # wait as long as the monitor does before ending such workers, in case the queue configuration comes back
        lifetime_hours = getattr(harvester_config.monitor, "workerMaxLifetimeHours", 720)
        if not lifetime_hours:
            return
        time_limit = core_utils.naive_utcnow() - datetime.timedelta(hours=lifetime_hours)
        n_deleted = 0
        for workspec in workspec_list:
            if workspec.creationTime is None or workspec.creationTime > time_limit:
                continue
            worker_log = self.make_logger(_logger, f"workerID={workspec.workerID}", method_name="delete_unconfigured_workers")
            worker_log.warning(
                f"delete from DB without cleanup since queue config not found and created at {workspec.creationTime}, over {lifetime_hours} hours ago"
            )
            if self.dbProxy.delete_worker(workspec.workerID):
                n_deleted += 1
        tmp_log.debug(f"deleted {n_deleted}/{len(workspec_list)} workers without queue config older than {lifetime_hours} hours")

    # delete old jobs and orphaned job info
    def delete_old_jobs(self):
        tmp_log = self.make_logger(_logger, f"id={self.lockedBy}", method_name="delete_old_jobs")
        sw = core_utils.get_stopwatch()
        # keep jobs longer than workers
        job_timeout = max(get_status_timeout_map().values()) + 1
        self.dbProxy.delete_old_jobs(job_timeout)
        self.dbProxy.delete_orphaned_job_info()
        tmp_log.debug("done deletion of old jobs" + sw.get_elapsed_time())

    # get the total size of files under a directory, and a list of (mtime, full path, size) of the files
    @staticmethod
    def scan_dir(dir_name):
        total_size = 0
        file_list = []
        for root, _, filenames in os.walk(dir_name):
            for base_name in filenames:
                full_name = os.path.join(root, base_name)
                try:
                    file_stat = os.stat(full_name)
                except OSError:
                    # e.g. deleted during the scan
                    continue
                total_size += file_stat.st_size
                file_list.append((file_stat.st_mtime, full_name, file_stat.st_size))
        return total_size, file_list

    # delete the oldest files not used by active jobs in directories over high watermarks, by one thread at a time over the instance
    def clean_up_disk(self):
        clean_up_interval = getattr(harvester_config.sweeper, "diskCleanUpInterval", None)
        high_watermarks = getattr(harvester_config.sweeper, "diskHighWatermark", None)
        if clean_up_interval is None or not high_watermarks:
            return
        if not self.dbProxy.get_process_lock("sweeper", self.get_pid(), clean_up_interval * 60 * 60):
            return
        tmp_log = self.make_logger(_logger, f"id={self.lockedBy}", method_name="clean_up_disk")
        all_active_files = None
        # comma-concatenated list of directory_name|high_watermark_in_GB
        for item in high_watermarks.split(","):
            try:
                dir_name, watermark = (x.strip() for x in item.split("|"))
                tmp_log.debug(f"checking {dir_name} for cleanup with watermark {watermark} GB")
                watermark = int(watermark) * 10**9
                total_size, file_list = self.scan_dir(dir_name)
                if total_size < watermark:
                    tmp_log.debug(f"skip cleanup {dir_name} due to total_size {total_size // 10 ** 9} GB < watermark {watermark // 10 ** 9} GB")
                    continue
                tmp_log.debug(f"cleanup {dir_name} due to total_size {total_size // 10 ** 9} GB >= watermark {watermark // 10 ** 9} GB")
                # get active input files not to delete
                if all_active_files is None:
                    all_active_files = self.dbProxy.get_all_active_input_files()
                    if all_active_files is None:
                        tmp_log.error("failed to get active input files; skip disk cleanup")
                        return
                # delete the oldest files first
                deleted_size = 0
                file_list.sort(key=lambda x: x[0])
                for _, full_name, f_size in file_list:
                    if total_size - deleted_size < watermark:
                        break
                    # keep if active
                    if os.path.basename(full_name) in all_active_files:
                        continue
                    try:
                        os.remove(full_name)
                    except OSError:
                        core_utils.dump_error_message(tmp_log)
                        continue
                    deleted_size += f_size
                tmp_log.debug(f"deleted {deleted_size // 10 ** 9} GB in {dir_name}")
            except Exception:
                core_utils.dump_error_message(tmp_log)
