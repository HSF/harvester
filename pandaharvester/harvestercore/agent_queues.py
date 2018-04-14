from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_factory import PluginFactory


# base class of agent queue
class AgentQueueBase:
    def __init__(self, **kwarg):
        for tmpKey, tmpVal in iteritems(kwarg):
            setattr(self, tmpKey, tmpVal)
        self.agentQueueConfigSection = None
        self.config = None
        self.agent_queue = None

    # make logger
    def make_logger(self, base_log, token=None, method_name=None, send_dialog=True):
        if send_dialog and hasattr(self, 'dbInterface'):
            hook = self.dbInterface
        else:
            hook = None
        return core_utils.make_logger(base_log, token=token, method_name=method_name, hook=hook)

    # intialize agent_queue from harvester configuration
    def _initialize_agent_queue(self):
        self.agent_queue = None
        if hasattr(harvester_config, self.agentQueueConfigSection):
            self.config = getattr(harvester_config, self.agentQueueConfigSection)
            if hasattr(self.config, 'agentQueueModule') \
                and hasattr(self.config, 'agentQueueClass'):
                pluginConf = {'module': self.config.agentQueueModule,
                              'name': self.config.agentQueueClass}
                pluginFactory = PluginFactory()
                self.agent_queue = pluginFactory.get_plugin(pluginConf)


# monitor queue
class MonitorQueue(AgentQueueBase):
    def __init__(self, **kwarg):
        AgentQueueBase.__init__(self, **kwarg)
        self.agentQueueConfigSection = 'monitor_queue'
        self._initialize_agent_queue()
