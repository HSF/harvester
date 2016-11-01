import threading

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.PluginFactory import PluginFactory

# logger
_logger = CoreUtils.setupLogger()


# credential manager
class CredManager(threading.Thread):
    # constructor
    def __init__(self, single_mode=False):
        threading.Thread.__init__(self)
        self.singleMode = single_mode
        self.pluginFactory = PluginFactory()
        # get plugin
        pluginPar = {}
        pluginPar['module'] = harvester_config.credmanager.moduleName
        pluginPar['name'] = harvester_config.credmanager.className
        pluginPar['config'] = harvester_config.credmanager
        self.exeCore = self.pluginFactory.getPlugin(pluginPar)

    # main loop
    def run(self):
        while True:
            # execute
            self.execute()
            # escape if single mode
            if self.singleMode:
                return
            # sleep
            CoreUtils.sleep(harvester_config.credmanager.sleepTime)

    # main
    def execute(self):
        # do nothing
        if self.exeCore is None:
            return
            # make logger
        mainLog = CoreUtils.makeLogger(_logger)
        # check credential
        mainLog.debug('check credential')
        isValid = self.exeCore.checkCredential(harvester_config.pandacon.key_file)
        # renew it if necessary
        if not isValid:
            mainLog.debug('renew credential')
            tmpStat, tmpOut = self.exeCore.renewCredential(harvester_config.pandacon.key_file)
            if not tmpStat:
                mainLog.error('failed : {0}'.format(tmpOut))
                return
        mainLog.debug('done')
