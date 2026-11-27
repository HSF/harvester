import importlib

from . import core_utils
from .db_interface import DBInterface

# logger
_logger = core_utils.setup_logger("plugin_factory")


# plugin factory
class PluginFactory(object):
    # constructor
    def __init__(self, no_db=False):
        # plugin classes by plugin key
        self.classMap = {}
        self.noDB = no_db

    # get plugin key, module + class
    def get_plugin_key(self, plugin_conf):
        return f"{plugin_conf['module']}.{plugin_conf['name']}"

    # get plugin instance; None if module or class is None
    def get_plugin(self, plugin_conf):
        module_name = plugin_conf["module"]
        class_name = plugin_conf["name"]
        if module_name is None or class_name is None:
            return None
        # get class, importing the module at the first call
        plugin_key = self.get_plugin_key(plugin_conf)
        if plugin_key not in self.classMap:
            tmp_log = core_utils.make_logger(_logger, method_name="get_plugin")
            tmp_log.debug(f"importing {module_name}")
            module = importlib.import_module(module_name)
            tmp_log.debug(f"getting class {class_name}")
            self.classMap[plugin_key] = getattr(module, class_name)
        cls = self.classMap[plugin_key]
        # plugin parameters become instance attributes
        args = {key: value for key, value in plugin_conf.items() if key not in ("module", "name")}
        # add database interface
        if not self.noDB:
            args["dbInterface"] = DBInterface()
        impl = cls(**args)
        # bare instance when middleware is used
        if "original_config" in plugin_conf and "bareFunctions" in plugin_conf:
            impl.bare_impl = self.get_plugin(plugin_conf["original_config"])
        return impl
