import json
import types

import six

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from .ssh_master_pool import sshMasterPool

# logger
_logger = core_utils.setup_logger("direct_ssh_herder")


# is mutable object to handle
def is_mutable(obj):
    return isinstance(obj, (list, dict)) or hasattr(obj, "__dict__")


# update changes recursively of an object from a new object
def update_object(old_obj, new_obj):
    if isinstance(old_obj, list):
        for i in range(len(old_obj)):
            try:
                new_obj[i]
            except IndexError:
                pass
            else:
                if is_mutable(old_obj[i]):
                    update_object(old_obj[i], new_obj[i])
                else:
                    old_obj[i] = new_obj[i]
    elif isinstance(old_obj, dict):
        for k in old_obj:
            try:
                new_obj[k]
            except KeyError:
                pass
            else:
                if is_mutable(old_obj[k]):
                    update_object(old_obj[k], new_obj[k])
                else:
                    old_obj[k] = new_obj[k]
    elif hasattr(old_obj, "__dict__"):
        for k in old_obj.__dict__:
            try:
                new_obj.__dict__[k]
            except KeyError:
                pass
            else:
                if k in ["isNew", "new_status"]:
                    # skip attributes omitted in workspec pickling
                    pass
                elif is_mutable(old_obj.__dict__[k]):
                    update_object(old_obj.__dict__[k], new_obj.__dict__[k])
                else:
                    old_obj.__dict__[k] = new_obj.__dict__[k]


# function class
class Method(object):
    # constructor
    def __init__(self, plugin_config, function_name, conn):
        self.plugin_config = plugin_config
        self.function_name = function_name
        self.conn = conn

    # execution
    def __call__(self, *args, **kwargs):
        tmpLog = core_utils.make_logger(_logger, method_name=self.function_name)
        tmpLog.debug("start")
        if self.conn is None:
            tmpLog.warning("connection is not alive; method {0} returns None".format(self.function_name))
            return None
        params = {
            "plugin_config": self.plugin_config,
            "function_name": self.function_name,
            "args": core_utils.pickle_to_text(args),
            "kwargs": core_utils.pickle_to_text(kwargs),
        }
        stdout, stderr = self.conn.communicate(input=six.b(json.dumps(params)))
        if self.conn.returncode == 0:
            return_dict = json.loads(stdout)
            if "exception" in return_dict:
                errMsg = core_utils.unpickle_from_text(str(return_dict["dialog"]))
                tmpLog.error("Exception from remote : " + errMsg)
                raise core_utils.unpickle_from_text(str(return_dict["exception"]))
            # propagate changes in mutable args
            new_args = core_utils.unpickle_from_text(str(return_dict["args"]))
            for old_arg, new_arg in zip(args, new_args):
                update_object(old_arg, new_arg)
            new_kwargs = core_utils.unpickle_from_text(str(return_dict["kwargs"]))
            for key in kwargs:
                old_kwarg = kwargs[key]
                new_kwarg = new_kwargs[key]
                update_object(old_kwarg, new_kwarg)
            return core_utils.unpickle_from_text(str(return_dict["return"]))
        else:
            tmpLog.error("execution failed with {0}; method={1} returns None".format(self.conn.returncode, self.function_name))
            return None


# Direct SSH herder
class DirectSshHerder(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        tmpLog = core_utils.make_logger(_logger, method_name="__init__")
        PluginBase.__init__(self, **kwarg)
        self.bare_impl = None
        self.sshUserName = getattr(self, "sshUserName", None)
        self.sshPassword = getattr(self, "sshPassword", None)
        self.privateKey = getattr(self, "privateKey", None)
        self.passPhrase = getattr(self, "passPhrase", None)
        self.jumpHost = getattr(self, "jumpHost", None)
        self.jumpPort = getattr(self, "jumpPort", 22)
        self.remoteHost = getattr(self, "remoteHost", None)
        self.remotePort = getattr(self, "remotePort", 22)
        self.bareFunctions = getattr(self, "bareFunctions", list())
        self.sockDir = getattr(self, "sockDir", "/tmp")
        self.numMasters = getattr(self, "numMasters", 1)
        self.execStr = getattr(self, "execStr", "")
        self.connectionLifetime = getattr(self, "connectionLifetime", None)
        try:
            self._get_connection()
        except Exception as e:
            core_utils.dump_error_message(tmpLog)
            tmpLog.error("failed to get connection")

    # get attribute
    def __getattr__(self, item):
        if item in self.__dict__:
            return self.__dict__[item]
        # bare functions
        if "bareFunctions" in self.__dict__ and self.__dict__["bareFunctions"] is not None and item in self.__dict__["bareFunctions"]:
            return getattr(object.__getattribute__(self, "bare_impl"), item)
        # remote functions
        bare_impl = object.__getattribute__(self, "bare_impl")
        if hasattr(bare_impl, item):
            if isinstance(getattr(bare_impl, item), types.MethodType):
                conn = self._get_connection()
                return Method(self.original_config, item, conn)
            else:
                return getattr(bare_impl, item)
        # others
        raise AttributeError(item)

    # ssh connection
    def _get_connection(self):
        tmpLog = core_utils.make_logger(_logger, method_name="_get_connection")
        tmpLog.debug("start")
        sshMasterPool.make_control_master(
            self.remoteHost,
            self.remotePort,
            self.numMasters,
            ssh_username=self.sshUserName,
            ssh_password=self.sshPassword,
            private_key=self.privateKey,
            pass_phrase=self.passPhrase,
            jump_host=self.jumpHost,
            jump_port=self.jumpPort,
            sock_dir=self.sockDir,
            connection_lifetime=self.connectionLifetime,
        )
        conn = sshMasterPool.get_connection(self.remoteHost, self.remotePort, self.execStr)
        if conn is not None:
            tmpLog.debug("connected successfully")
        else:
            tmpLog.error("failed to connect")
        return conn
