import json
import re
import itertools

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvesterbody.agent_base import AgentBase

# logger
_logger = core_utils.setup_logger('cred_manager')


# credential manager
class CredManager(AgentBase):

    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.queue_config_mapper = queue_config_mapper
        self.pluginFactory = PluginFactory()
        self.dbProxy = DBProxy()
        # get module and class names
        if hasattr(harvester_config.credmanager, 'moduleName'):
            moduleNames = self.get_list(harvester_config.credmanager.moduleName)
        else:
            moduleNames = []
        if hasattr(harvester_config.credmanager, 'className'):
            classNames = self.get_list(harvester_config.credmanager.className)
        else:
            classNames = []
        # file names of original certificates
        if hasattr(harvester_config.credmanager, 'inCertFile'):
            inCertFiles = self.get_list(harvester_config.credmanager.inCertFile)
        elif hasattr(harvester_config.credmanager, 'certFile'):
            inCertFiles = self.get_list(harvester_config.credmanager.certFile)
        else:
            inCertFiles = []
        # file names of certificates to be generated
        if hasattr(harvester_config.credmanager, 'outCertFile'):
            outCertFiles = self.get_list(harvester_config.credmanager.outCertFile)
        else:
            # use the file name of the certificate for panda connection as output name
            outCertFiles = self.get_list(harvester_config.pandacon.cert_file)
        # VOMS
        if hasattr(harvester_config.credmanager, 'voms'):
            vomses = self.get_list(harvester_config.credmanager.voms)
        else:
            vomses = []
        # direct and merged plugin configuration in json
        if hasattr(harvester_config.credmanager, 'pluginConfigs'):
            pluginConfigs = harvester_config.credmanager.pluginConfigs
        else:
            pluginConfigs = []
        # plugin cores
        self.exeCores = []
        self.queue_exe_cores = []
        # get plugin from harvester config
        self.get_cores_from_harvester_config()

    # get list
    def get_list(self, data):
        if isinstance(data, list):
            return data
        else:
            return [data]

    # get plugin cores from harvester config
    def get_cores_from_harvester_config(self):
        # from traditional attributes
        for moduleName, className, inCertFile, outCertFile, voms in \
                zip(moduleNames, classNames, inCertFiles, outCertFiles, vomses):
            pluginPar = {}
            pluginPar['module'] = moduleName
            pluginPar['name'] = className
            pluginPar['inCertFile'] = inCertFile
            pluginPar['outCertFile'] = outCertFile
            pluginPar['voms'] = voms
            try:
                exeCore = self.pluginFactory.get_plugin(pluginPar)
                self.exeCores.append(exeCore)
            except Exception:
                _logger.error('failed to launch credmanager with traditional attributes for {0}'.format(pluginPar))
                core_utils.dump_error_message(_logger)
        # from pluginConfigs
        for pc in pluginConfigs:
            try:
                setup_maps = json.loads(pc['configs'])
                for setup_name, setup_map in setup_maps.items():
                    try:
                        pluginPar = {}
                        pluginPar['module'] = pc['module']
                        pluginPar['name'] = pc['name']
                        pluginPar['setup_name'] = setup_name
                        pluginPar.update(setup_map)
                        exeCore = self.pluginFactory.get_plugin(pluginPar)
                        self.exeCores.append(exeCore)
                    except Exception:
                        _logger.error('failed to launch credmanager in pluginConfigs for {0}'.format(pluginPar))
                        core_utils.dump_error_message(_logger)
            except Exception:
                _logger.error('failed to parse pluginConfigs {0}'.format(pc))
                core_utils.dump_error_message(_logger)

    # update plugin cores from queue config
    def update_cores_from_queue_config(self):
        self.queue_exe_cores = []
        for queue_name, queue_config in self.queue_config_mapper.get_all_queues().items():
            if not hasattr(queue_config, 'credmanagers'):
                continue
            if not isinstance(queue_config.credmanagers, list):
                continue
            for cm_setup in queue_config.credmanagers.items:
                try:
                    pluginPar = {}
                    pluginPar['module'] = cm_setup['module']
                    pluginPar['name'] = cm_setup['name']
                    pluginPar['setup_name'] = queue_name
                    for k, v in cm_setup.items():
                        if k in ('module', 'name'):
                            pass
                        if isinstance(v, str) and '$' in v:
                            # replace placeholders
                            value = ''
                            patts = re.findall('\$\{([a-zA-Z\d_.]+)\}', v)
                            for patt in patts:
                                tmp_ph = '${' + patt + '}'
                                tmp_val = None
                                if patt == 'harvesterID':
                                    tmp_val = harvester_config.master.harvester_id
                                elif patt == 'queueName':
                                    tmp_val = queue_name
                                if tmp_val is not None:
                                    value = value.replace(tmp_ph, tmp_val)
                            # fill in
                            pluginPar[k] = value
                        else:
                            # fill in
                            pluginPar[k] = v
                    exe_core = self.pluginFactory.get_plugin(pluginPar)
                    self.queue_exe_cores.append(exe_core)
                except Exception:
                    _logger.error('failed to launch about queue={0} for {1}'.format(queue_name, pluginPar))
                    core_utils.dump_error_message(_logger)

    # main loop
    def run(self):
        while True:
            # update plugin cores from queue config
            self.update_cores_from_queue_config()
            # execute
            self.execute()
            # check if being terminated
            if self.terminated(harvester_config.credmanager.sleepTime, randomize=False):
                return

    # main
    def execute(self):
        # get lock
        locked = self.dbProxy.get_process_lock('credmanager', self.get_pid(),
                                               harvester_config.credmanager.sleepTime)
        if not locked:
            return
        # loop over all plugins
        for exeCore in itertools.chain(self.exeCores, self.queue_exe_cores):
            # do nothing
            if exeCore is None:
                continue
            # make logger
            credmanager_name = ''
            if hasattr(exeCore, 'setup_name'):
                credmanager_name = exeCore.setup_name
            else:
                credmanager_name = '{0} {1}'.format(exeCore.inCertFile, exeCore.outCertFile)
            mainLog = self.make_logger(_logger,
                                        '{0} {1}'.format(exeCore.__class__.__name__, credmanager_name),
                                        method_name='execute')
            try:
                # check credential
                mainLog.debug('check credential')
                isValid = exeCore.check_credential()
                if isValid:
                    mainLog.debug('valid')
                elif not isValid:
                    # renew it if necessary
                    mainLog.debug('invalid')
                    mainLog.debug('renew credential')
                    tmpStat, tmpOut = exeCore.renew_credential()
                    if not tmpStat:
                        mainLog.error('failed : {0}'.format(tmpOut))
                        continue
            except Exception:
                core_utils.dump_error_message(mainLog)
            mainLog.debug('done')
