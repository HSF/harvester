import socket
import datetime
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvestercore.command_spec import CommandSpec


# logger
_logger = core_utils.setup_logger()


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
            for comStr, receiver in CommandSpec.receiver_map.iteritems():
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
        main_log = core_utils.make_logger(_logger, 'id={0}'.format(self.ident))
        bulk_size = harvester_config.commandmanager.commands_bulk_size

        # report initial worker stats for the WORKER_STATS command
        siteNames = set()
        for queueName in harvester_config.qconf.queueList:
            queueConfig = self.queueConfigMapper.get_queue(queueName)
            if queueConfig.runMode != 'slave':
                continue
            # get siteName
            if queueConfig.siteName not in ['', None]:
                siteName = queueConfig.siteName
            else:
                siteName = queueConfig.queueName
            if siteName in siteNames:
                continue
            siteNames.add(siteName)
            # get worker stats
            workerStats = self.db_proxy.get_worker_stats(siteName)
            if len(workerStats) == 0:
                main_log.error('failed to get worker stats for {0}'.format(siteName))
            else:
                # report worker stats
                tmpRet, tmpStr = self.communicator.update_worker_stats(siteName, workerStats)
                if tmpRet:
                    main_log.debug('updated worker stats for {0}'.format(siteName))
                else:
                    main_log.error('failed to update worker stats for {0} err={1}'.format(siteName,
                                                                                          tmpStr))

        # main loop
        while True:
            main_log.debug('polling commands loop')

            # send heartbeat
            if self.lastHeartbeat is None \
                    or self.lastHeartbeat < datetime.datetime.utcnow() - datetime.timedelta(minutes=10):
                self.lastHeartbeat = datetime.datetime.utcnow()
                self.communicator.is_alive({'startTime': datetime.datetime.utcnow()})

            continuous_loop = True  # as long as there are commands, retrieve them

            while continuous_loop:

                # get commands from panda server for this harvester instance
                commands = self.communicator.get_commands(bulk_size)
                main_log.debug('got {0} commands (bulk size: {1})'.format(len(commands), bulk_size))
                command_specs = self.convert_to_command_specs(commands)

                # cache commands in internal DB
                self.db_proxy.store_commands(command_specs)
                main_log.debug('cached {0} commands in internal DB'.format(len(command_specs)))

                # retrieve processed commands from harvester cache
                command_ids_ack = self.db_proxy.get_commands_ack()

                for shard in core_utils.create_shards(command_ids_ack, bulk_size):
                    # post acknowledgements to panda server
                    self.communicator.ack_commands(shard)
                    main_log.debug('acknowledged {0} commands to panda server'.format(len(shard)))

                    # clean acknowledged commands
                    self.db_proxy.clean_commands_by_id(shard)

                # clean commands that have been processed and do not need acknowledgement
                self.db_proxy.clean_processed_commands()

                # if we didn't collect the full bulk, give panda server a break
                if len(commands) < bulk_size:
                    continuous_loop = False

            # check if being terminated
            if self.terminated(harvester_config.commandmanager.sleepTime):
                main_log.debug('terminated')
                return
