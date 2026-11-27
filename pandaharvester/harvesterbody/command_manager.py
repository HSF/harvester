import datetime
import socket

from pandaharvester import commit_timestamp, panda_pkg_info
from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.command_spec import CommandSpec
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy

# logger
_logger = core_utils.setup_logger("command_manager")

# interval in minutes to send heartbeat to panda server
HEARTBEAT_INTERVAL = 10


# class to retrieve commands from panda server
class CommandManager(AgentBase):
    # constructor
    def __init__(self, communicator, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.db_proxy = DBProxy()
        self.communicator = communicator
        self.queueConfigMapper = queue_config_mapper
        self.nodeName = socket.gethostname()
        self.lastHeartbeat = None

    # set single mode
    def set_single_mode(self, single_mode):
        self.singleMode = single_mode

    def convert_to_command_specs(self, commands):
        """
        Generates a list of CommandSpec objects for commands with a known receiver
        """
        command_specs = []
        for command in commands:
            command_spec = CommandSpec()
            command_spec.convert_command_json(command)
            command_spec.receiver = CommandSpec.get_receiver(command_spec.command)
            if command_spec.receiver is not None:
                command_specs.append(command_spec)
        return command_specs

    def get_lock(self):
        return self.db_proxy.get_process_lock("commandmanager", self.get_pid(), harvester_config.commandmanager.sleepTime)

    def run(self):
        """
        main
        """
        main_log = self.make_logger(_logger, f"id={self.get_pid()}", method_name="run")
        # tell panda server that this instance started
        try:
            if self.get_lock():
                self.report_start(main_log)
        except Exception:
            core_utils.dump_error_message(main_log)
        # main loop
        while True:
            if self.get_lock() or self.singleMode:
                try:
                    self.poll_commands(main_log)
                except Exception:
                    core_utils.dump_error_message(main_log)
            # check if being terminated
            if self.terminated(harvester_config.commandmanager.sleepTime, randomize=False):
                main_log.debug("terminated")
                return

    def report_start(self, tmp_log):
        """
        Send the start time and the software version to panda server, together with the commands to receive for queues in slave mode
        """
        site_names = set()
        command_list = []
        for queue_config in self.queueConfigMapper.get_active_queues().values():
            if queue_config is None or queue_config.runMode != "slave":
                continue
            # one command for all queues in one site
            target = {"computingSite": queue_config.siteName, "resourceType": queue_config.resourceType}
            if queue_config.siteName not in site_names:
                site_names.add(queue_config.siteName)
                command_list.append({"command": CommandSpec.COM_reportWorkerStats, **target})
            # one command for each queue
            command_list.append({"command": CommandSpec.COM_setNWorkers, **target})
        data = {"startTime": core_utils.naive_utcnow(), "sw_version": panda_pkg_info.release_version, "commit_stamp": commit_timestamp.timestamp}
        if command_list:
            tmp_log.debug("sending command list to receive")
            data["commands"] = command_list
        self.communicator.is_alive(data)

    def poll_commands(self, tmp_log):
        """
        Send heartbeat, then retrieve commands from panda server and acknowledge processed ones, as long as full bulks of commands come
        """
        tmp_log.debug("polling commands loop")
        bulk_size = harvester_config.commandmanager.commands_bulk_size
        # send heartbeat
        time_now = core_utils.naive_utcnow()
        if self.lastHeartbeat is None or self.lastHeartbeat < time_now - datetime.timedelta(minutes=HEARTBEAT_INTERVAL):
            self.lastHeartbeat = time_now
            self.communicator.is_alive({})
        while True:
            # get commands from panda server for this harvester instance
            commands = self.communicator.get_commands(bulk_size)
            tmp_log.debug(f"got {len(commands)} commands (bulk size: {bulk_size})")
            command_specs = self.convert_to_command_specs(commands)
            # cache commands in internal DB
            if self.db_proxy.store_commands(command_specs):
                tmp_log.debug(f"cached {len(command_specs)} commands in internal DB")
            else:
                tmp_log.error(f"failed to cache {len(command_specs)} commands in internal DB")
            # acknowledge processed commands to panda server, and clean them once acknowledged; the rest are retried in the next poll
            for shard in core_utils.create_shards(self.db_proxy.get_commands_ack(), bulk_size):
                if self.communicator.ack_commands(shard):
                    tmp_log.debug(f"acknowledged {len(shard)} commands to panda server")
                    self.db_proxy.clean_commands_by_id(shard)
                else:
                    tmp_log.error(f"failed to acknowledge {len(shard)} commands to panda server")
            # clean commands that have been processed and do not need acknowledgement
            self.db_proxy.clean_processed_commands()
            # if we didn't collect the full bulk, give panda server a break
            if len(commands) < bulk_size:
                break
