import os

try:
    from os import walk
except ImportError:
    from scandir import walk

from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.command_spec import CommandSpec
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory

# logger
_logger = core_utils.setup_logger("sweeper")


# class for cleanup
class Sweeper(AgentBase):
    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.queueConfigMapper = queue_config_mapper
        self.pluginFactory = PluginFactory()
        self.lockedBy = None

    def process_kill_commands(self):
        # process commands for marking workers that need to be killed

        tmp_log = self.make_logger(_logger, f"id={self.lockedBy}", method_name="process_commands")

        # 1. KILL_WORKER commands that were sent to panda server and forwarded to harvester
        stopwatch = core_utils.get_stopwatch()
        command_string = CommandSpec.COM_killWorkers
        tmp_log.debug(f"try to get {command_string} commands")
        command_specs = self.dbProxy.get_commands_for_receiver("sweeper", command_string)
        tmp_log.debug(f"got {len(command_specs)} {command_string} commands")
        for command_spec in command_specs:
            n_to_kill = self.dbProxy.mark_workers_to_kill_by_query(command_spec.params)
            tmp_log.debug(f"will kill {n_to_kill} workers with {command_spec.params}")
        tmp_log.debug(f"done handling {command_string} commands took {stopwatch.get_elapsed_time()}s")

        # 2. SYNC_WORKERS_KILL commands from comparing worker status provided by pilot and harvester
        stopwatch = core_utils.get_stopwatch()
        command_string = CommandSpec.COM_syncWorkersKill
        tmp_log.debug(f"try to get {command_string} commands")
        command_specs = self.dbProxy.get_commands_for_receiver("sweeper", command_string)
        tmp_log.debug(f"got {len(command_specs)} {command_string} commands")
        for command_spec in command_specs:
            n_to_kill = self.dbProxy.mark_workers_to_kill_by_workerids(command_spec.params)
            tmp_log.debug(f"will kill {n_to_kill} workers with {command_spec.params}")
        tmp_log.debug(f"done handling {command_string} commands took {stopwatch.get_elapsed_time()}s")

    # main loop
    def run(self):
        self.lockedBy = f"sweeper-{self.get_pid()}"
        while True:
            sw_main = core_utils.get_stopwatch()
            main_log = self.make_logger(_logger, f"id={self.lockedBy}", method_name="run")

            # process commands that mark workers to be killed
            try:
                self.process_kill_commands()
            except Exception:
                core_utils.dump_error_message(main_log)

            # actual killing stage
            sw_kill = core_utils.get_stopwatch()
            main_log.debug("try to get workers to kill")
            # get workers to kill
            workers_to_kill = self.dbProxy.get_workers_to_kill(harvester_config.sweeper.maxWorkers, harvester_config.sweeper.checkInterval)
            main_log.debug(f"got {len(workers_to_kill)} queues to kill workers")
            # loop over all workers
            sw = core_utils.get_stopwatch()
            for queue_name, configIdWorkSpecList in workers_to_kill.items():
                for configID, workspec_list in configIdWorkSpecList.items():
                    # get sweeper
                    if not self.queueConfigMapper.has_queue(queue_name, configID):
                        main_log.error(f"queue config for {queue_name}/{configID} not found")
                        continue
                    queue_config = self.queueConfigMapper.get_queue(queue_name, configID)
                    try:
                        sweeper_core = self.pluginFactory.get_plugin(queue_config.sweeper)
                    except Exception:
                        main_log.error(f"failed to launch sweeper plugin for {queue_name}/{configID}")
                        core_utils.dump_error_message(main_log)
                        continue
                    sw.reset()
                    n_workers = len(workspec_list)
                    try:
                        # try bulk method
                        tmp_log = self.make_logger(_logger, f"id={self.lockedBy}", method_name="run")
                        tmp_log.debug("start killing")
                        tmp_list = sweeper_core.kill_workers(workspec_list)
                    except AttributeError:
                        # fall back to single-worker method
                        for workspec in workspec_list:
                            tmp_log = self.make_logger(_logger, f"workerID={workspec.workerID}", method_name="run")
                            try:
                                tmp_log.debug("start killing one worker")
                                tmp_stat, tmp_out = sweeper_core.kill_worker(workspec)
                                tmp_log.debug(f"done killing with status={tmp_stat} diag={tmp_out}")
                            except Exception:
                                core_utils.dump_error_message(tmp_log)
                    except Exception:
                        core_utils.dump_error_message(main_log)
                    else:
                        # bulk method
                        n_killed = 0
                        for workspec, (tmp_stat, tmp_out) in zip(workspec_list, tmp_list):
                            tmp_log.debug(f"done killing workerID={workspec.workerID} with status={tmp_stat} diag={tmp_out}")
                            if tmp_stat:
                                n_killed += 1
                        tmp_log.debug(f"killed {n_killed}/{n_workers} workers")
                    main_log.debug(f"done killing {n_workers} workers" + sw.get_elapsed_time())
            main_log.debug("done all killing" + sw_kill.get_elapsed_time())

            # cleanup stage
            sw_cleanup = core_utils.get_stopwatch()
            # timeout for missed
            try:
                keep_missed = harvester_config.sweeper.keepMissed
            except Exception:
                keep_missed = 24
            try:
                keep_pending = harvester_config.sweeper.keepPending
            except Exception:
                keep_pending = 24
            # get workers for cleanup
            statusTimeoutMap = {
                "finished": harvester_config.sweeper.keepFinished,
                "failed": harvester_config.sweeper.keepFailed,
                "cancelled": harvester_config.sweeper.keepCancelled,
                "missed": keep_missed,
                "pending": keep_pending,
            }
            workersForCleanup = self.dbProxy.get_workers_for_cleanup(harvester_config.sweeper.maxWorkers, statusTimeoutMap)
            main_log.debug(f"got {len(workersForCleanup)} queues for workers cleanup")
            sw = core_utils.get_stopwatch()
            for queue_name, configIdWorkSpecList in workersForCleanup.items():
                for configID, workspec_list in configIdWorkSpecList.items():
                    # get sweeper
                    if not self.queueConfigMapper.has_queue(queue_name, configID):
                        main_log.error(f"queue config for {queue_name}/{configID} not found")
                        continue
                    queue_config = self.queueConfigMapper.get_queue(queue_name, configID)
                    sweeper_core = self.pluginFactory.get_plugin(queue_config.sweeper)
                    messenger = self.pluginFactory.get_plugin(queue_config.messenger)
                    sw.reset()
                    n_workers = len(workspec_list)
                    # make sure workers to clean up are all terminated
                    main_log.debug("making sure workers to clean up are all terminated")
                    try:
                        # try bulk method
                        tmp_list = sweeper_core.kill_workers(workspec_list)
                    except AttributeError:
                        # fall back to single-worker method
                        for workspec in workspec_list:
                            tmp_log = self.make_logger(_logger, f"workerID={workspec.workerID}", method_name="run")
                            try:
                                tmp_stat, tmp_out = sweeper_core.kill_worker(workspec)
                            except Exception:
                                core_utils.dump_error_message(tmp_log)
                    except Exception:
                        core_utils.dump_error_message(main_log)
                    main_log.debug("made sure workers to clean up are all terminated")
                    # start cleanup
                    for workspec in workspec_list:
                        tmp_log = self.make_logger(_logger, f"workerID={workspec.workerID}", method_name="run")
                        try:
                            tmp_log.debug("start cleaning up one worker")
                            # sweep worker
                            tmp_stat, tmp_out = sweeper_core.sweep_worker(workspec)
                            tmp_log.debug(f"swept_worker with status={tmp_stat} diag={tmp_out}")
                            tmp_log.debug("start messenger cleanup")
                            mc_tmp_stat, mc_tmp_out = messenger.clean_up(workspec)
                            tmp_log.debug(f"messenger cleaned up with status={mc_tmp_stat} diag={mc_tmp_out}")
                            if tmp_stat:
                                self.dbProxy.delete_worker(workspec.workerID)
                        except Exception:
                            core_utils.dump_error_message(tmp_log)
                    main_log.debug(f"done cleaning up {n_workers} workers" + sw.get_elapsed_time())
            main_log.debug("done all cleanup" + sw_cleanup.get_elapsed_time())

            # old-job-deletion stage
            sw_delete = core_utils.get_stopwatch()
            main_log.debug("delete old jobs")
            jobTimeout = max(statusTimeoutMap.values()) + 1
            self.dbProxy.delete_old_jobs(jobTimeout)
            # delete orphaned job info
            self.dbProxy.delete_orphaned_job_info()
            main_log.debug("done deletion of old jobs" + sw_delete.get_elapsed_time())
            # disk cleanup
            if hasattr(harvester_config.sweeper, "diskCleanUpInterval") and hasattr(harvester_config.sweeper, "diskHighWatermark"):
                locked = self.dbProxy.get_process_lock("sweeper", self.get_pid(), harvester_config.sweeper.diskCleanUpInterval * 60 * 60)
                if locked:
                    try:
                        all_active_files = None
                        for item in harvester_config.sweeper.diskHighWatermark.split(","):
                            # dir name and watermark in GB
                            dir_name, watermark = item.split("|")
                            main_log.debug(f"checking {dir_name} for cleanup with watermark {watermark} GB")
                            watermark = int(watermark) * 10**9
                            total_size = 0
                            file_dict = {}
                            # scan dir
                            for root, dirs, filenames in walk(dir_name):
                                for base_name in filenames:
                                    full_name = os.path.join(root, base_name)
                                    f_size = os.path.getsize(full_name)
                                    total_size += f_size
                                    mtime = os.path.getmtime(full_name)
                                    file_dict.setdefault(mtime, set())
                                    file_dict[mtime].add((base_name, full_name, f_size))
                            # delete if necessary
                            if total_size < watermark:
                                main_log.debug(f"skip cleanup {dir_name} due to total_size {total_size // 10 ** 9} GB < watermark {watermark // 10 ** 9} GB")
                            else:
                                main_log.debug(f"cleanup {dir_name} due to total_size {total_size // 10 ** 9} GB >= watermark {watermark // 10 ** 9} GB")
                                # get active input files
                                if all_active_files is None:
                                    all_active_files = self.dbProxy.get_all_active_input_files()
                                deleted_size = 0
                                mtimes = sorted(file_dict.keys())
                                for mtime in mtimes:
                                    for base_name, full_name, f_size in file_dict[mtime]:
                                        # keep if active
                                        if base_name in all_active_files:
                                            continue
                                        try:
                                            os.remove(full_name)
                                        except Exception:
                                            core_utils.dump_error_message(main_log)
                                        deleted_size += f_size
                                        if total_size - deleted_size < watermark:
                                            break
                                    if total_size - deleted_size < watermark:
                                        break
                    except Exception:
                        core_utils.dump_error_message(main_log)
            # time the cycle
            main_log.debug("done a sweeper cycle" + sw_main.get_elapsed_time())
            # check if being terminated
            if self.terminated(harvester_config.sweeper.sleepTime):
                main_log.debug("terminated")
                return
