import itertools

from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterbody.agent_plugin_utils import (
    as_list,
    make_plugins_from_plugin_configs,
    make_plugins_from_queue_configs,
)
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory

# logger
_logger = core_utils.setup_logger("cred_manager")


# credential manager
class CredManager(AgentBase):
    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.queue_config_mapper = queue_config_mapper
        self.pluginFactory = PluginFactory()
        self.dbProxy = DBProxy()
        # general cred managers that are not queue based, e.g. VOMS renewal on harvester instance
        self.exe_cores = self.make_cores_from_harvester_config()
        # cred managers that are queue based, e.g. VOMS refresh on K8S clusters; made in each cycle once the lock is taken
        self.queue_exe_cores = []

    # make plugin cores from harvester config
    def make_cores_from_harvester_config(self):
        tmp_log = self.make_logger(_logger, method_name="make_cores_from_harvester_config")
        config = harvester_config.credmanager
        # traditional attributes, one value per plugin in each
        module_names = as_list(getattr(config, "moduleName", []))
        class_names = as_list(getattr(config, "className", []))
        # file names of original certificates; certFile is the old name
        in_cert_files = as_list(getattr(config, "inCertFile", getattr(config, "certFile", [])))
        # file names of certificates to be generated; by default the certificate for panda connection
        if hasattr(config, "outCertFile"):
            out_cert_files = as_list(config.outCertFile)
        else:
            out_cert_files = as_list(harvester_config.pandacon.cert_file)
        vomses = as_list(getattr(config, "voms", []))
        attr_lists = (module_names, class_names, in_cert_files, out_cert_files, vomses)
        n_values = [len(attr_list) for attr_list in attr_lists]
        # warn only when traditional attributes are used; outCertFile alone has the default from pandacon
        traditional_attrs = ("moduleName", "className", "inCertFile", "certFile", "outCertFile", "voms")
        if any(hasattr(config, attr) for attr in traditional_attrs) and len(set(n_values)) > 1:
            tmp_log.warning(f"moduleName, className, inCertFile, outCertFile, voms have {n_values} values; only the first {min(n_values)} plugins are used")
        exe_cores = []
        for module_name, class_name, in_cert_file, out_cert_file, voms in zip(*attr_lists):
            plugin_params = {"module": module_name, "name": class_name, "inCertFile": in_cert_file, "outCertFile": out_cert_file, "voms": voms}
            try:
                exe_core = self.pluginFactory.get_plugin(plugin_params)
                if exe_core is not None:
                    exe_cores.append(exe_core)
            except Exception:
                tmp_log.error(f"failed to launch credmanager with traditional attributes for {plugin_params}")
                core_utils.dump_error_message(tmp_log)
        # direct and merged plugin configuration in json
        exe_cores += make_plugins_from_plugin_configs(self.pluginFactory, getattr(config, "pluginConfigs", []), tmp_log)
        return exe_cores

    # update plugin cores from queue config
    def update_cores_from_queue_config(self):
        tmp_log = self.make_logger(_logger, method_name="update_cores_from_queue_config")
        self.queue_exe_cores = make_plugins_from_queue_configs(self.pluginFactory, self.queue_config_mapper, "credmanagers", tmp_log)

    # make a logger for a plugin core
    def make_core_logger(self, exe_core, method_name):
        if hasattr(exe_core, "setup_name"):
            credmanager_name = exe_core.setup_name
        else:
            credmanager_name = f"{exe_core.inCertFile} {exe_core.outCertFile}"
        return self.make_logger(_logger, f"{exe_core.__class__.__name__} {credmanager_name}", method_name=method_name)

    # main loop
    def run(self):
        main_log = self.make_logger(_logger, f"id={self.get_pid()}", method_name="run")
        while True:
            self.run_stages((self.execute,), main_log)
            # check if being terminated
            if self.terminated(harvester_config.credmanager.sleepTime, randomize=False):
                return

    # check and renew credentials
    def execute(self):
        # get lock
        if not self.dbProxy.get_process_lock("credmanager", self.get_pid(), harvester_config.credmanager.sleepTime):
            return
        # update plugin cores from queue config; the previous ones are kept on failure
        self.run_stages((self.update_cores_from_queue_config,), self.make_logger(_logger, f"id={self.get_pid()}", method_name="execute"))
        # loop over all plugins
        for exe_core in itertools.chain(self.exe_cores, self.queue_exe_cores):
            tmp_log = self.make_core_logger(exe_core, "execute")
            try:
                # check credential
                tmp_log.debug("check credential")
                if exe_core.check_credential():
                    tmp_log.debug("valid")
                else:
                    # renew it if necessary
                    tmp_log.debug("invalid")
                    tmp_log.debug("renew credential")
                    tmp_stat, tmp_out = exe_core.renew_credential()
                    if not tmp_stat:
                        tmp_log.error(f"failed : {tmp_out}")
                        continue
            except Exception:
                core_utils.dump_error_message(tmp_log)
            tmp_log.debug("done")

    # get credential lifetimes for monitoring
    def execute_monit(self):
        self.update_cores_from_queue_config()
        metrics = {}
        # loop over all plugins
        for exe_core in itertools.chain(self.exe_cores, self.queue_exe_cores):
            tmp_log = self.make_core_logger(exe_core, "execute_monit")
            try:
                # check credential lifetime
                tmp_log.debug("check credential lifetime")
                lifetime = exe_core.check_credential_lifetime()
                if lifetime is not None:
                    metrics[exe_core.outCertFile] = lifetime
            except Exception:
                core_utils.dump_error_message(tmp_log)
            tmp_log.debug("done")
        return metrics
