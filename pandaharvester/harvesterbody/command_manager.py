import socket
import datetime
from future.utils import iteritems
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvestercore.command_spec import CommandSpec
from pandaharvester import commit_timestamp
from pandaharvester import panda_pkg_info


# logger
_logger = core_utils.setup_logger("command_manager")


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
        Generates a list of CommandSpec objects
        """
        command_specs = []
        for command in commands:
            command_spec = CommandSpec()
            command_spec.convert_command_json(command)
            for comStr, receiver in iteritems(CommandSpec.receiver_map):
                if command_spec.command.startswith(comStr):
                    command_spec.receiver = receiver
                    break
            if command_spec.receiver is not None:
                command_specs.append(command_spec)
        return command_specs

    def run(self):
        """
        main
        """
        main_log = self.make_logger(_logger, "id={0}".format(self.get_pid()), method_name="run")
        bulk_size = harvester_config.commandmanager.commands_bulk_size
        locked = self.db_proxy.get_process_lock("commandmanager", self.get_pid(), harvester_config.commandmanager.sleepTime)
        if locked:
            # send command list to be received
            siteNames = set()
            commandList = []
            for queueName, queueConfig in iteritems(self.queueConfigMapper.get_active_queues()):
                if queueConfig is None or queueConfig.runMode != "slave":
                    continue
                # one command for all queues in one site
                if queueConfig.siteName not in siteNames:
                    commandItem = {
                        "command": CommandSpec.COM_reportWorkerStats,
                        "computingSite": queueConfig.siteName,
                        "resourceType": queueConfig.resourceType,
                    }
                    commandList.append(commandItem)
                siteNames.add(queueConfig.siteName)
                # one command for each queue
                commandItem = {"command": CommandSpec.COM_setNWorkers, "computingSite": queueConfig.siteName, "resourceType": queueConfig.resourceType}
                commandList.append(commandItem)
            data = {"startTime": datetime.datetime.utcnow(), "sw_version": panda_pkg_info.release_version, "commit_stamp": commit_timestamp.timestamp}
            if len(commandList) > 0:
                main_log.debug("sending command list to receive")
                data["commands"] = commandList
            self.communicator.is_alive(data)

        # main loop
        while True:
            # get lock
            locked = self.db_proxy.get_process_lock("commandmanager", self.get_pid(), harvester_config.commandmanager.sleepTime)
            if locked or self.singleMode:
                main_log.debug("polling commands loop")

                # send heartbeat
                if self.lastHeartbeat is None or self.lastHeartbeat < datetime.datetime.utcnow() - datetime.timedelta(minutes=10):
                    self.lastHeartbeat = datetime.datetime.utcnow()
                    self.communicator.is_alive({})

                continuous_loop = True  # as long as there are commands, retrieve them

                while continuous_loop:
                    # get commands from panda server for this harvester instance
                    commands = self.communicator.get_commands(bulk_size)
                    main_log.debug("got {0} commands (bulk size: {1})".format(len(commands), bulk_size))
                    command_specs = self.convert_to_command_specs(commands)

                    # cache commands in internal DB
                    self.db_proxy.store_commands(command_specs)
                    main_log.debug("cached {0} commands in internal DB".format(len(command_specs)))

                    # retrieve processed commands from harvester cache
                    command_ids_ack = self.db_proxy.get_commands_ack()

                    for shard in core_utils.create_shards(command_ids_ack, bulk_size):
                        # post acknowledgements to panda server
                        self.communicator.ack_commands(shard)
                        main_log.debug("acknowledged {0} commands to panda server".format(len(shard)))

                        # clean acknowledged commands
                        self.db_proxy.clean_commands_by_id(shard)

                    # clean commands that have been processed and do not need acknowledgement
                    self.db_proxy.clean_processed_commands()

                    # if we didn't collect the full bulk, give panda server a break
                    if len(commands) < bulk_size:
                        continuous_loop = False

            # check if being terminated
            if self.terminated(harvester_config.commandmanager.sleepTime, randomize=False):
                main_log.debug("terminated")
                return
