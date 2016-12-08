from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvesterbody.agent_base import AgentBase

# logger
_logger = core_utils.setup_logger()


# credential manager
class CredManager(AgentBase):

    # constructor
    def __init__(self, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.pluginFactory = PluginFactory()
        # get plugin
        pluginPar = {}
        pluginPar['module'] = harvester_config.credmanager.moduleName
        pluginPar['name'] = harvester_config.credmanager.className
        pluginPar['config'] = harvester_config.credmanager
        self.exeCore = self.pluginFactory.get_plugin(pluginPar)


    # main loop
    def run(self):
        while True:
            # execute
            self.execute()
            # check if being terminated
            if self.terminated(harvester_config.credmanager.sleepTime):
                return


    # main
    def execute(self):
        # do nothing
        if self.exeCore is None:
            return
            # make logger
        mainLog = core_utils.make_logger(_logger)
        # check credential
        mainLog.debug('check credential')
        isValid = self.exeCore.check_credential(harvester_config.pandacon.key_file)
        # renew it if necessary
        if not isValid:
            mainLog.debug('renew credential')
            tmpStat, tmpOut = self.exeCore.renew_credential(harvester_config.pandacon.key_file)
            if not tmpStat:
                mainLog.error('failed : {0}'.format(tmpOut))
                return
        mainLog.debug('done')
