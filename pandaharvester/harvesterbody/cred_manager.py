import json

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
    def __init__(self, single_mode=False):
        AgentBase.__init__(self, single_mode)
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
        # get plugin
        self.exeCores = []
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
                _logger.error('failed to launch credmanager for {0}'.format(pluginPar))
                core_utils.dump_error_message(_logger)
        for pc in pluginConfigs:
            try:
                with open(pc['config_file']) as f:
                    setup_maps = json.load(f)
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
                        _logger.error('failed to launch credmanager in pluginConfigs setup {0} for {1}'.format(setup_name, pluginPar))
                        core_utils.dump_error_message(_logger)
            except Exception:
                _logger.error('failed to parse pluginConfigs {0}'.format(pc))
                core_utils.dump_error_message(_logger)

    # get list
    def get_list(self, data):
        if isinstance(data, list):
            return data
        else:
            return [data]

    # main loop
    def run(self):
        while True:
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
        for exeCore in self.exeCores:
            # do nothing
            if exeCore is None:
                continue

            # make logger
            credmanager_name = ''
            if hasattr(exeCore, 'setup_name'):
                credmanager_name = exeCore.setup_name
            else:
                credmanager_name = '{0} {1}'.format(exeCore.inCertFile, exeCore.outCertFile)
            mainLog = self.make_logger(_logger, '{0} {1}'.format(exeCore.__class__.__name__,
                                                                         credmanager_name),
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
