from future.utils import iteritems

from . import core_utils
from .db_interface import DBInterface

# logger
_logger = core_utils.setup_logger('plugin_factory')


# plugin factory
class PluginFactory:
    # constructor
    def __init__(self):
        self.classMap = {}

    # get plugin
    def get_plugin(self, plugin_conf):
        # use module + class as key
        moduleName = plugin_conf['module']
        className = plugin_conf['name']
        pluginKey = '{0}.{1}'.format(moduleName, className)
        if moduleName is None or className is None:
            return None
        # get class
        if pluginKey not in self.classMap:
            tmpLog = core_utils.make_logger(_logger, method_name='get_plugin')
            # import module
            tmpLog.debug("importing {0}".format(moduleName))
            mod = __import__(moduleName)
            for subModuleName in moduleName.split('.')[1:]:
                mod = getattr(mod, subModuleName)
            # get class
            tmpLog.debug("getting class {0}".format(className))
            cls = getattr(mod, className)
            # add
            self.classMap[pluginKey] = cls
        # make args
        args = {}
        for tmpKey, tmpVal in iteritems(plugin_conf):
            if tmpKey in ['module', 'name']:
                continue
            args[tmpKey] = tmpVal
        # add database interface
        args['dbInterface'] = DBInterface()
        # instantiate
        cls = self.classMap[pluginKey]
        impl = cls(**args)
        return impl
