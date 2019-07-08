import json
import types

try:
    import cPickle as pickle
except Exception:
    import pickle

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from .ssh_master_pool import sshMasterPool

# logger
_logger = core_utils.setup_logger('direct_ssh_herder')


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
        tmpLog.debug('start')
        if self.conn is None:
            tmpLog.warning('connection is not alive; method {0} returns None'.format(self.function_name))
            return None
        params = {'plugin_config': self.plugin_config,
                  'function_name': self.function_name,
                  'args': pickle.dumps(args),
                  'kwargs': pickle.dumps(kwargs)}
        stdout, stderr = self.conn.communicate(input=json.dumps(params))
        if self.conn.returncode == 0:
            return_dict = json.loads(stdout)
            if 'exception' in return_dict:
                errMsg = pickle.loads(str(return_dict['dialog']))
                tmpLog.error('Exception from remote : ' + errMsg)
                raise pickle.loads(str(return_dict['exception']))
            # propagate changes in mutable args
            new_args = pickle.loads(str(return_dict['args']))
            for old_arg, new_arg in zip(args, new_args):
                if isinstance(old_arg, types.ListType):
                    del old_arg[:]
                    old_arg += new_arg
                elif isinstance(old_arg, types.DictionaryType):
                    old_arg.clear()
                    old_arg.update(new_arg)
                elif hasattr(old_arg, '__dict__'):
                    old_arg.__dict__ = new_arg.__dict__
            new_kwargs = pickle.loads(str(return_dict['kwargs']))
            for key in kwargs:
                old_kwarg = kwargs[key]
                new_kwarg = new_kwargs[key]
                if isinstance(old_kwarg, types.ListType):
                    del old_kwarg[:]
                    old_kwarg += new_kwarg
                elif isinstance(old_kwarg, types.DictionaryType):
                    old_kwarg.clear()
                    old_kwarg.update(new_kwarg)
                elif hasattr(old_kwarg, '__dict__'):
                    old_kwarg.__dict__ = new_kwarg.__dict__
            return pickle.loads(str(return_dict['return']))
        else:
            tmpLog.error('execution failed with {0}; method={1} returns None'.format(self.conn.returncode,
                                                                                     self.function_name))
            return None


# Direct SSH herder
class DirectSshHerder(PluginBase):

    # constructor
    def __init__(self, **kwarg):
        tmpLog = core_utils.make_logger(_logger, method_name='__init__')
        PluginBase.__init__(self, **kwarg)
        self.bare_impl = None
        self.sshUserName = getattr(self, 'sshUserName', None)
        self.sshPassword = getattr(self, 'sshPassword', None)
        self.privateKey = getattr(self, 'privateKey', None)
        self.passPhrase = getattr(self, 'passPhrase', None)
        self.jumpHost = getattr(self, 'jumpHost', None)
        self.jumpPort = getattr(self, 'jumpPort', 22)
        self.remoteHost = getattr(self, 'remoteHost', None)
        self.remotePort = getattr(self, 'remotePort', 22)
        self.bareFunctions = getattr(self, 'bareFunctions', list())
        self.sockDir = getattr(self, 'sockDir', '/tmp')
        self.numMasters = getattr(self, 'numMasters', 1)
        self.execStr = getattr(self, 'execStr', '')
        try:
            self._get_connection()
        except Exception as e:
            core_utils.dump_error_message(tmpLog)
            tmpLog.error('failed to get connection')

    # get attribute
    def __getattr__(self, item):
        if item in self.__dict__:
                return self.__dict__[item]
        # bare functions
        if 'bareFunctions' in self.__dict__ and self.__dict__['bareFunctions'] is not None \
                and item in self.__dict__['bareFunctions']:
            return getattr(object.__getattribute__(self, 'bare_impl'), item)
        # remote functions
        bare_impl = object.__getattribute__(self, 'bare_impl')
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
        tmpLog = core_utils.make_logger(_logger, method_name='_get_connection')
        tmpLog.debug('start')
        sshMasterPool.make_control_master(self.remoteHost, self.remotePort, self.numMasters,
                                          ssh_username=self.sshUserName, ssh_password=self.sshPassword,
                                          private_key=self.privateKey, pass_phrase=self.passPhrase,
                                          jump_host=self.jumpHost, jump_port=self.jumpPort, sock_dir=self.sockDir)
        conn = sshMasterPool.get_connection(self.remoteHost, self.remotePort, self.execStr)
        if conn is not None:
            tmpLog.debug('connected successfully')
        else:
            tmpLog.error('failed to connect')
        return conn
