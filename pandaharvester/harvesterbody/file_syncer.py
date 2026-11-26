import itertools

from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterbody.agent_plugin_utils import (
    make_plugins_from_plugin_configs,
    make_plugins_from_queue_configs,
)
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory

# logger
_logger = core_utils.setup_logger("file_syncer")


# file syncer
class FileSyncer(AgentBase):
    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.queue_config_mapper = queue_config_mapper
        self.pluginFactory = PluginFactory()
        self.dbProxy = DBProxy()
        # plugin cores from harvester config
        tmp_log = self.make_logger(_logger, method_name="__init__")
        self.exe_cores = make_plugins_from_plugin_configs(self.pluginFactory, getattr(harvester_config.file_syncer, "pluginConfigs", []), tmp_log)
        # plugin cores from queue config; made in each cycle once the lock is taken
        self.queue_exe_cores = []

    # update plugin cores from queue config
    def update_cores_from_queue_config(self):
        tmp_log = self.make_logger(_logger, method_name="update_cores_from_queue_config")
        self.queue_exe_cores = make_plugins_from_queue_configs(self.pluginFactory, self.queue_config_mapper, "file_syncer", tmp_log)

    # main loop
    def run(self):
        main_log = self.make_logger(_logger, f"id={self.get_pid()}", method_name="run")
        while True:
            self.run_stages((self.execute,), main_log)
            # check if being terminated
            if self.terminated(harvester_config.file_syncer.sleepTime, randomize=False):
                return

    # sync files
    def execute(self):
        # get lock
        if not self.dbProxy.get_process_lock("file_syncer", self.get_pid(), harvester_config.file_syncer.sleepTime):
            return
        # update plugin cores from queue config; the previous ones are kept on failure
        self.run_stages((self.update_cores_from_queue_config,), self.make_logger(_logger, f"id={self.get_pid()}", method_name="execute"))
        # loop over all plugins
        for exe_core in itertools.chain(self.exe_cores, self.queue_exe_cores):
            tmp_log = self.make_logger(_logger, f"{exe_core.__class__.__name__} {exe_core.setup_name}", method_name="execute")
            try:
                # check freshness
                tmp_log.debug("check")
                if not exe_core.check():
                    tmp_log.debug("no need to update, skip")
                else:
                    # update if necessary
                    tmp_log.debug("updating")
                    tmp_stat, tmp_out = exe_core.update()
                    if not tmp_stat:
                        tmp_log.error(f"failed : {tmp_out}")
                        continue
                    tmp_log.debug("updated")
            except Exception:
                core_utils.dump_error_message(tmp_log)
            tmp_log.debug("done")
