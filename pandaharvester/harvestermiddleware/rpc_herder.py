import functools

import rpyc

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from .ssh_tunnel_pool import sshTunnelPool


# logger
_logger = core_utils.setup_logger('rpc_herder')


# RPC herder
class RpcHerder(PluginBase):

    # decorator
    def require_alive(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.is_connected:
                return func(self, *args, **kwargs)
            else:
                tmpLog = core_utils.make_logger(_logger, method_name=func.__name__)
                tmpLog.warning('instance not alive; method {0} returns None'.format(func.__name__))
                return None
        return wrapper

    # constructor
    def __init__(self, **kwarg):
        tmpLog = core_utils.make_logger(_logger, method_name='__init__')
        PluginBase.__init__(self, **kwarg)
        self.sshUserName = getattr(self, 'sshUserName', None)
        self.sshPassword = getattr(self, 'sshPassword', None)
        self.privateKey = getattr(self, 'privateKey', None)
        self.passPhrase = getattr(self, 'passPhrase', None)
        self.jumpHost = getattr(self, 'jumpHost', None)
        self.jumpPort = getattr(self, 'jumpPort', 22)
        self.remotePort = getattr(self, 'remotePort', 22)
        # is connected only if ssh forwarding works
        self.is_connected = False
        try:
            self._get_connection()
        except Exception as e:
            tmpLog.error('failed to get connection ; {0}: {1}'.format(e.__class__.__name__, e))
        else:
            self.is_connected = True

    # ssh and rpc connect
    def _get_connection(self):
        tmpLog = core_utils.make_logger(_logger, method_name='_get_connection')
        sshTunnelPool.make_tunnel_server(self.remoteHost, self.remotePort, self.remoteBindPort, self.numTunnels,
                                     ssh_username=self.sshUserName, ssh_password=self.sshPassword,
                                     private_key=self.privateKey, pass_phrase=self.passPhrase,
                                     jump_host=self.jumpHost, jump_port=self.jumpPort)
        tunnelHost, tunnelPort, tunnelCore = sshTunnelPool.get_tunnel(self.remoteHost, self.remotePort)
        self.conn = rpyc.connect(tunnelHost, tunnelPort, config={"allow_all_attrs": True,
                                                                    "allow_setattr": True,
                                                                    "allow_delattr": True})
        tmpLog.debug('connected successfully to {0}:{1}'.format(tunnelHost, tunnelPort))

    # submit workers
    @require_alive
    def submit_workers(self, workspec_list):
        tmpLog = core_utils.make_logger(_logger, method_name='submit_workers')
        try:
            ret = self.conn.root.submit_workers(self.original_config, workspec_list)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug('done')
        return ret

    # check workers
    @require_alive
    def check_workers(self, workspec_list):
        tmpLog = core_utils.make_logger(_logger, method_name='check_workers')
        try:
            ret = self.conn.root.check_workers(self.original_config, workspec_list)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug('done')
        return ret

    # setup access points
    @require_alive
    def setup_access_points(self, workspec_list):
        tmpLog = core_utils.make_logger(_logger, method_name='setup_access_points')
        try:
            ret = self.conn.root.setup_access_points(self.original_config, workspec_list)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug('done')
        return ret

    # feed jobs
    @require_alive
    def feed_jobs(self, workspec, jobspec_list):
        tmpLog = core_utils.make_logger(_logger, method_name='feed_jobs')
        try:
            ret = self.conn.root.feed_jobs(self.original_config, workspec, jobspec_list)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug('done')
        return ret

    # request job
    @require_alive
    def job_requested(self, workspec):
        tmpLog = core_utils.make_logger(_logger, method_name='job_requested')
        try:
            ret = self.conn.root.job_requested(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug('done')
        return ret

    # request kill
    @require_alive
    def kill_requested(self, workspec):
        tmpLog = core_utils.make_logger(_logger, method_name='kill_requested')
        try:
            ret = self.conn.root.kill_requested(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug('done')
        return ret

    # is alive
    @require_alive
    def is_alive(self, workspec, worker_heartbeat_limit):
        tmpLog = core_utils.make_logger(_logger, method_name='is_alive')
        try:
            ret = self.conn.root.is_alive(self.original_config, workspec, worker_heartbeat_limit)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug('done')
        return ret

    # get work attributes
    @require_alive
    def get_work_attributes(self, workspec):
        tmpLog = core_utils.make_logger(_logger, method_name='get_work_attributes')
        try:
            ret = self.conn.root.get_work_attributes(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug('done')
        return ret

    # get output files
    @require_alive
    def get_files_to_stage_out(self, workspec):
        tmpLog = core_utils.make_logger(_logger, method_name='get_files_to_stage_out')
        try:
            ret = self.conn.root.get_files_to_stage_out(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug('done')
        return ret

    # get events
    @require_alive
    def events_to_update(self, workspec):
        tmpLog = core_utils.make_logger(_logger, method_name='events_to_update')
        try:
            ret = self.conn.root.events_to_update(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug('done')
        return ret

    # request events
    @require_alive
    def events_requested(self, workspec):
        tmpLog = core_utils.make_logger(_logger, method_name='events_requested')
        try:
            ret = self.conn.root.events_requested(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug('done')
        return ret

    # get PandaIDs
    @require_alive
    def get_panda_ids(self, workspec):
        tmpLog = core_utils.make_logger(_logger, method_name='get_panda_ids')
        try:
            ret = self.conn.root.get_panda_ids(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug('done')
        return ret

    # post processing
    @require_alive
    def post_processing(self, workspec, jobspec_list, map_type):
        tmpLog = core_utils.make_logger(_logger, method_name='post_processing')
        try:
            ret = self.conn.root.post_processing(self.original_config, workspec, jobspec_list, map_type)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug('done')
        return ret

    # send ACK
    @require_alive
    def acknowledge_events_files(self, workspec):
        tmpLog = core_utils.make_logger(_logger, method_name='acknowledge_events_files')
        try:
            ret = self.conn.root.acknowledge_events_files(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug('done')
        return ret
