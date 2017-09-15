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
        moduleNames = self.get_list(harvester_config.credmanager.moduleName)
        classNames = self.get_list(harvester_config.credmanager.className)
        # file names of original certificates
        inCertFiles = self.get_list(harvester_config.credmanager.certFile)
        # file names of certificates to be generated
        if hasattr(harvester_config.credmanager, 'outCertFile'):
            outCertFiles = self.get_list(harvester_config.credmanager.outCertFile)
        else:
            # use the file name of the certificate for panda connection as output name
            outCertFiles = self.get_list(harvester_config.pandacon.cert_file)
        # VOMS
        vomses = self.get_list(harvester_config.credmanager.voms)
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
            exeCore = self.pluginFactory.get_plugin(pluginPar)
            self.exeCores.append(exeCore)

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
            mainLog = core_utils.make_logger(_logger, "{0} {1}".format(exeCore.__class__.__name__,
                                                                       exeCore.outCertFile),
                                             method_name='execute')
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
            mainLog.debug('done')
