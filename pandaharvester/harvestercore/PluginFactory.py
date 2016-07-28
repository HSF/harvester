import CoreUtils

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('PluginFactory')


# plugin factory
class PluginFactory:

    # constructor
    def __init__(self):
        self.classMap = {}


    # get plugin
    def getPlugin(self,pluginConf):
        # use module + class as key
        moduleName = pluginConf['module']
        className  = pluginConf['name']
        pluginKey = '{0}.{1}'.format(moduleName,className)
        # get class
        if not pluginKey in self.classMap:
            tmpLog = CoreUtils.makeLogger(_logger)
            # import module
            tmpLog.debug("importing {0}".format(moduleName))
            mod = __import__(moduleName)
            for subModuleName in moduleName.split('.')[1:]:
                mod = getattr(mod,subModuleName)
            # get class
            tmpLog.debug("getting class {0}".format(className))
            cls = getattr(mod,className)
            # add
            self.classMap[pluginKey] = cls
        # make args
        args = {}
        for tmpKey,tmpVal in pluginConf.iteritems():
            if tmpKey in ['module','name']:
                continue
            args[tmpKey] = tmpVal
        # instantiate
        cls = self.classMap[pluginKey]
        impl = cls(**args)
        return impl
