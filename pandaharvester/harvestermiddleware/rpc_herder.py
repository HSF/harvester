import functools

import rpyc

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from .ssh_tunnel_pool import sshTunnelPool


# logger
_logger = core_utils.setup_logger("rpc_herder")


# rpyc configuration
rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True
rpyc.core.protocol.DEFAULT_CONFIG["sync_request_timeout"] = 1800


# RPC herder
class RpcHerder(PluginBase):
    # decorator
    def require_alive(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.bareFunctions is not None and func.__name__ in self.bareFunctions:
                return getattr(self.bare_impl, func.__name__)(*args, **kwargs)
            elif self.is_connected:
                retVal = func(self, *args, **kwargs)
                return rpyc.utils.classic.obtain(retVal)
            else:
                tmpLog = core_utils.make_logger(_logger, method_name=func.__name__)
                tmpLog.warning("instance not alive; method {0} returns None".format(func.__name__))
                return None

        return wrapper

    # constructor
    def __init__(self, **kwarg):
        tmpLog = core_utils.make_logger(_logger, method_name="__init__")
        PluginBase.__init__(self, **kwarg)
        self.sshUserName = getattr(self, "sshUserName", None)
        self.sshPassword = getattr(self, "sshPassword", None)
        self.privateKey = getattr(self, "privateKey", None)
        self.passPhrase = getattr(self, "passPhrase", None)
        self.jumpHost = getattr(self, "jumpHost", None)
        self.jumpPort = getattr(self, "jumpPort", 22)
        self.remotePort = getattr(self, "remotePort", 22)
        self.bareFunctions = getattr(self, "bareFunctions", list())
        # is connected only if ssh forwarding works
        self.is_connected = False
        try:
            self._get_connection()
        except Exception as e:
            tmpLog.error("failed to get connection ; {0}: {1}".format(e.__class__.__name__, e))
        else:
            self.is_connected = True

    # ssh and rpc connect
    def _get_connection(self):
        tmpLog = core_utils.make_logger(_logger, method_name="_get_connection")
        tmpLog.debug("start")
        sshTunnelPool.make_tunnel_server(
            self.remoteHost,
            self.remotePort,
            self.remoteBindPort,
            self.numTunnels,
            ssh_username=self.sshUserName,
            ssh_password=self.sshPassword,
            private_key=self.privateKey,
            pass_phrase=self.passPhrase,
            jump_host=self.jumpHost,
            jump_port=self.jumpPort,
        )
        tunnelHost, tunnelPort, tunnelCore = sshTunnelPool.get_tunnel(self.remoteHost, self.remotePort)
        self.conn = rpyc.connect(tunnelHost, tunnelPort, config={"allow_all_attrs": True, "allow_setattr": True, "allow_delattr": True})
        tmpLog.debug("connected successfully to {0}:{1}".format(tunnelHost, tunnelPort))

    ######################
    # submitter section

    # submit workers
    @require_alive
    def submit_workers(self, workspec_list):
        tmpLog = core_utils.make_logger(_logger, method_name="submit_workers")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.submit_workers(self.original_config, workspec_list)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    ######################
    # monitor section

    # check workers
    @require_alive
    def check_workers(self, workspec_list):
        tmpLog = core_utils.make_logger(_logger, method_name="check_workers")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.check_workers(self.original_config, workspec_list)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    ######################
    # sweeper section

    # kill worker
    @require_alive
    def kill_worker(self, workspec):
        tmpLog = core_utils.make_logger(_logger, method_name="kill_worker")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.kill_worker(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # FIXME: cannot have this yet otherwise sweeper agent see this method while the real plugin may not implemented this method yet...
    # kill workers
    # @require_alive
    # def kill_workers(self, workspec_list):
    #     tmpLog = core_utils.make_logger(_logger, method_name='kill_workers')
    #     tmpLog.debug('start')
    #     try:
    #         ret = self.conn.root.kill_workers(self.original_config, workspec_list)
    #     except Exception:
    #         core_utils.dump_error_message(tmpLog)
    #         ret = None
    #     else:
    #         tmpLog.debug('done')
    #     return ret

    # cleanup for a worker
    @require_alive
    def sweep_worker(self, workspec):
        tmpLog = core_utils.make_logger(_logger, method_name="sweep_worker")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.sweep_worker(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    ######################
    # messenger section

    # setup access points
    @require_alive
    def setup_access_points(self, workspec_list):
        tmpLog = core_utils.make_logger(_logger, method_name="setup_access_points")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.setup_access_points(self.original_config, workspec_list)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # feed jobs
    @require_alive
    def feed_jobs(self, workspec, jobspec_list):
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="feed_jobs")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.feed_jobs(self.original_config, workspec, jobspec_list)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # request job
    @require_alive
    def job_requested(self, workspec):
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="job_requested")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.job_requested(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # request kill
    @require_alive
    def kill_requested(self, workspec):
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="kill_requested")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.kill_requested(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # is alive
    @require_alive
    def is_alive(self, workspec, worker_heartbeat_limit):
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="is_alive")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.is_alive(self.original_config, workspec, worker_heartbeat_limit)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # get work attributes
    @require_alive
    def get_work_attributes(self, workspec):
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="get_work_attributes")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.get_work_attributes(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # get output files
    @require_alive
    def get_files_to_stage_out(self, workspec):
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="get_files_to_stage_out")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.get_files_to_stage_out(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # feed events
    @require_alive
    def feed_events(self, workspec, events_dict):
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="feed_events")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.feed_events(self.original_config, workspec, events_dict)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # get events
    @require_alive
    def events_to_update(self, workspec):
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="events_to_update")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.events_to_update(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # request events
    @require_alive
    def events_requested(self, workspec):
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="events_requested")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.events_requested(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # get PandaIDs
    @require_alive
    def get_panda_ids(self, workspec):
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="get_panda_ids")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.get_panda_ids(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # post processing
    @require_alive
    def post_processing(self, workspec, jobspec_list, map_type):
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="post_processing")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.post_processing(self.original_config, workspec, jobspec_list, map_type)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # send ACK
    @require_alive
    def acknowledge_events_files(self, workspec):
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="acknowledge_events_files")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.acknowledge_events_files(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # clean up
    @require_alive
    def clean_up(self, workspec):
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="clean_up")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.clean_up(self.original_config, workspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    ######################
    # stager section

    # check stage out status
    @require_alive
    def check_stage_out_status(self, jobspec):
        tmpLog = core_utils.make_logger(_logger, method_name="check_stage_out_status")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.check_stage_out_status(self.original_config, jobspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # trigger stage out
    @require_alive
    def trigger_stage_out(self, jobspec):
        tmpLog = core_utils.make_logger(_logger, method_name="trigger_stage_out")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.trigger_stage_out(self.original_config, jobspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # zip output files
    @require_alive
    def zip_output(self, jobspec):
        tmpLog = core_utils.make_logger(_logger, method_name="zip_output")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.zip_output(self.original_config, jobspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    ######################
    # preparator section

    # check stage in status
    @require_alive
    def check_stage_in_status(self, jobspec):
        tmpLog = core_utils.make_logger(_logger, method_name="check_stage_in_status")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.check_stage_in_status(self.original_config, jobspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # trigger preparation
    @require_alive
    def trigger_preparation(self, jobspec):
        tmpLog = core_utils.make_logger(_logger, method_name="trigger_preparation")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.trigger_preparation(self.original_config, jobspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret

    # resolve input file paths
    @require_alive
    def resolve_input_paths(self, jobspec):
        tmpLog = core_utils.make_logger(_logger, method_name="resolve_input_paths")
        tmpLog.debug("start")
        try:
            ret = self.conn.root.resolve_input_paths(self.original_config, jobspec)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            ret = None
        else:
            tmpLog.debug("done")
        return ret
