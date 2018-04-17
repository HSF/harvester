from future.utils import iteritems

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy


# base class of fifo message queue
class FIFOBase:
    _attrs_from_plugin = ('size', 'put', 'get', 'getlast', 'peek', 'clear')

    # constructor
    def __init__(self, **kwarg):
        for tmpKey, tmpVal in iteritems(kwarg):
            setattr(self, tmpKey, tmpVal)
        self.dbProxy = DBProxy()

    # make logger
    def make_logger(self, base_log, token=None, method_name=None, send_dialog=True):
        if send_dialog and hasattr(self, 'dbInterface'):
            hook = self.dbInterface
        else:
            hook = None
        return core_utils.make_logger(base_log, token=token, method_name=method_name, hook=hook)

    # intialize fifo from harvester configuration
    def _initialize_fifo(self):
        self.config = getattr(harvester_config, self.fifoConfigSection)
        if hasattr(self.config, 'fifoModule') \
            and hasattr(self.config, 'fifoClass'):
            pluginConf = vars(self.config).copy()
            pluginConf.update( {'module': self.config.fifoModule,
                                'name': self.config.fifoClass,} )
            pluginFactory = PluginFactory()
            self.fifo = pluginFactory.get_plugin(pluginConf)
            # for tmpAttr in self._attrs_from_plugin:
            #     setattr(self, tmpAttr, vars(self.fifo)[tmpAttr])


# monitor fifo
class MonitorFIFO(FIFOBase):
    # constructor
    def __init__(self, **kwarg):
        FIFOBase.__init__(self, **kwarg)
        self.fifoConfigSection = 'monitor_fifo'
        self._initialize_fifo()

    def populate(self, seconds_ago=0, clear_fifo=False):
        """
        Populate monitor fifo with all active worker chunks from DB
        with modificationTime earlier than seconds_ago seconds ago
        object in fifo = [(queueName_1, [[worker_1_1], [worker_1_2], ...]), (queueName_2, ...)]
        """
        if clear_fifo:
            self.fifo.clear()
        n_workers = self.config.maxWorkersToPopulate
        workspec_iterator = self.dbProxy.get_active_workers(n_workers, seconds_ago)
        last_queueName = None
        workspec_chunk = []
        for workspec in workspec_iterator:
            if last_queueName == None:
                workspec_chunk = [[workspec]]
                last_queueName = workspec.computingSite
            elif workspec.computingSite == last_queueName \
                and len(workspec_chunk) < self.config.maxWorkersPerChunk:
                workspec_chunk.append([workspec])
            else:
                self.fifo.put((last_queueName, workspec_chunk))
                workspec_chunk = [[workspec]]
                last_queueName = workspec.computingSite
        if len(workspec_chunk) > 0:
            self.fifo.put((last_queueName, workspec_chunk))
