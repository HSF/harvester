import re
import itertools

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvesterbody.agent_base import AgentBase

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
        # plugin cores
        self.exe_cores = []  # general cred managers that are not queue based, e.g. VOMS renewal on harvester instance
        self.queue_exe_cores = []  # cred manages that are queue based, e.g. VOMS refresh on K8S clusters
        # get plugin from harvester config
        self.get_cores_from_harvester_config()
        # update plugin cores from queue config
        self.update_cores_from_queue_config()

    # get list
    def get_list(self, data):
        if isinstance(data, list):
            return data
        else:
            return [data]

    # get plugin cores from harvester config
    def get_cores_from_harvester_config(self):
        # get module and class names
        if hasattr(harvester_config.credmanager, "moduleName"):
            moduleNames = self.get_list(harvester_config.credmanager.moduleName)
        else:
            moduleNames = []
        if hasattr(harvester_config.credmanager, "className"):
            classNames = self.get_list(harvester_config.credmanager.className)
        else:
            classNames = []
        # file names of original certificates
        if hasattr(harvester_config.credmanager, "inCertFile"):
            inCertFiles = self.get_list(harvester_config.credmanager.inCertFile)
        elif hasattr(harvester_config.credmanager, "certFile"):
            inCertFiles = self.get_list(harvester_config.credmanager.certFile)
        else:
            inCertFiles = []
        # file names of certificates to be generated
        if hasattr(harvester_config.credmanager, "outCertFile"):
            outCertFiles = self.get_list(harvester_config.credmanager.outCertFile)
        else:
            # use the file name of the certificate for panda connection as output name
            outCertFiles = self.get_list(harvester_config.pandacon.cert_file)
        # VOMS
        if hasattr(harvester_config.credmanager, "voms"):
            vomses = self.get_list(harvester_config.credmanager.voms)
        else:
            vomses = []
        # direct and merged plugin configuration in json
        if hasattr(harvester_config.credmanager, "pluginConfigs"):
            pluginConfigs = harvester_config.credmanager.pluginConfigs
        else:
            pluginConfigs = []
        # from traditional attributes
        for moduleName, className, inCertFile, outCertFile, voms in zip(moduleNames, classNames, inCertFiles, outCertFiles, vomses):
            plugin_params = {}
            plugin_params["module"] = moduleName
            plugin_params["name"] = className
            plugin_params["inCertFile"] = inCertFile
            plugin_params["outCertFile"] = outCertFile
            plugin_params["voms"] = voms
            try:
                exe_core = self.pluginFactory.get_plugin(plugin_params)
                self.exe_cores.append(exe_core)
            except Exception:
                _logger.error("failed to launch credmanager with traditional attributes for {0}".format(plugin_params))
                core_utils.dump_error_message(_logger)
        # from pluginConfigs
        for pc in pluginConfigs:
            try:
                setup_maps = pc["configs"]
                for setup_name, setup_map in setup_maps.items():
                    try:
                        plugin_params = {"module": pc["module"], "name": pc["name"], "setup_name": setup_name}
                        plugin_params.update(setup_map)
                        exe_core = self.pluginFactory.get_plugin(plugin_params)
                        self.exe_cores.append(exe_core)
                    except Exception:
                        _logger.error("failed to launch credmanager in pluginConfigs for {0}".format(plugin_params))
                        core_utils.dump_error_message(_logger)
            except Exception:
                _logger.error("failed to parse pluginConfigs {0}".format(pc))
                core_utils.dump_error_message(_logger)

    # update plugin cores from queue config
    def update_cores_from_queue_config(self):
        self.queue_exe_cores = []
        for queue_name, queue_config in self.queue_config_mapper.get_all_queues().items():
            if queue_config.queueStatus == "offline" or not hasattr(queue_config, "credmanagers") or not isinstance(queue_config.credmanagers, list):
                continue
            for cm_setup in queue_config.credmanagers:
                try:
                    plugin_params = {"module": cm_setup["module"], "name": cm_setup["name"], "setup_name": queue_name, "queueName": queue_name}
                    for k, v in cm_setup.items():
                        if k in ("module", "name"):
                            pass
                        if isinstance(v, str) and "$" in v:
                            # replace placeholders
                            value = v
                            patts = re.findall("\$\{([a-zA-Z\d_.]+)\}", v)
                            for patt in patts:
                                tmp_ph = "${" + patt + "}"
                                tmp_val = None
                                if patt == "harvesterID":
                                    tmp_val = harvester_config.master.harvester_id
                                elif patt == "queueName":
                                    tmp_val = queue_name
                                elif patt.startswith("common."):
                                    # values from common blocks
                                    attr = patt.replace("common.", "")
                                    if hasattr(queue_config, "common") and attr in queue_config.common:
                                        tmp_val = queue_config.common[attr]
                                if tmp_val is not None:
                                    value = value.replace(tmp_ph, tmp_val)
                            # fill in
                            plugin_params[k] = value
                        else:
                            # fill in
                            plugin_params[k] = v
                    exe_core = self.pluginFactory.get_plugin(plugin_params)
                    self.queue_exe_cores.append(exe_core)
                except Exception:
                    _logger.error("failed to launch plugin for queue={0} and {1}".format(queue_name, plugin_params))
                    core_utils.dump_error_message(_logger)

    # main loop
    def run(self):
        while True:
            # update plugin cores from queue config
            self.update_cores_from_queue_config()

            # execute
            self.execute()  # this is the main run

            # check if being terminated
            if self.terminated(harvester_config.credmanager.sleepTime, randomize=False):
                return

    # main
    def execute(self):
        # get lock
        locked = self.dbProxy.get_process_lock("credmanager", self.get_pid(), harvester_config.credmanager.sleepTime)
        if not locked:
            return
        # loop over all plugins
        for exe_core in itertools.chain(self.exe_cores, self.queue_exe_cores):
            # do nothing
            if exe_core is None:
                continue
            # make logger
            if hasattr(exe_core, "setup_name"):
                credmanager_name = exe_core.setup_name
            else:
                credmanager_name = "{0} {1}".format(exe_core.inCertFile, exe_core.outCertFile)
            mainLog = self.make_logger(_logger, "{0} {1}".format(exe_core.__class__.__name__, credmanager_name), method_name="execute")
            try:
                # check credential
                mainLog.debug("check credential")
                isValid = exe_core.check_credential()
                if isValid:
                    mainLog.debug("valid")
                elif not isValid:
                    # renew it if necessary
                    mainLog.debug("invalid")
                    mainLog.debug("renew credential")
                    tmpStat, tmpOut = exe_core.renew_credential()
                    if not tmpStat:
                        mainLog.error("failed : {0}".format(tmpOut))
                        continue
            except Exception:
                core_utils.dump_error_message(mainLog)
            mainLog.debug("done")

    # monit main
    def execute_monit(self):
        self.update_cores_from_queue_config()

        metrics = {}
        # loop over all plugins
        for exe_core in itertools.chain(self.exe_cores, self.queue_exe_cores):
            # do nothing
            if exe_core is None:
                continue

            # make logger
            if hasattr(exe_core, "setup_name"):
                credmanager_name = exe_core.setup_name
            else:
                credmanager_name = "{0} {1}".format(exe_core.inCertFile, exe_core.outCertFile)

            sub_log = self.make_logger(_logger, "{0} {1}".format(exe_core.__class__.__name__, credmanager_name), method_name="execute_monit")
            try:
                # check credential
                sub_log.debug("check credential lifetime")
                lifetime = exe_core.check_credential_lifetime()
                if lifetime is not None:
                    metrics[exe_core.outCertFile] = lifetime
            except Exception:
                core_utils.dump_error_message(sub_log)

            sub_log.debug("done")

        return metrics
