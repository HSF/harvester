import datetime
from future.utils import iteritems

try:
    from threading import get_ident
except ImportError:
    from thread import get_ident

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy

# logger
_logger = core_utils.setup_logger('fifos')

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
        self.config = getattr(harvester_config, self.fifoName)
        if hasattr(self.config, 'fifoModule') \
            and hasattr(self.config, 'fifoClass'):
            pluginConf = vars(self.config).copy()
            pluginConf.update( {'module': self.config.fifoModule,
                                'name': self.config.fifoClass,} )
            pluginFactory = PluginFactory()
            self.fifo = pluginFactory.get_plugin(pluginConf)

    # size of queue
    def size(self):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_ident()), method_name='size')
        retVal = self.fifo.size()
        return retVal

    # enqueue
    def put(self, obj):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_ident()), method_name='put')
        retVal = self.fifo.put(obj)
        return retVal

    # dequeue
    def get(self, timeout=None):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_ident()), method_name='get')
        retVal = self.fifo.get(timeout)
        return retVal

    # get without dequeuing
    def peek(self):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_ident()), method_name='peek')
        retVal = self.fifo.peek()
        return retVal


# monitor fifo
class MonitorFIFO(FIFOBase):
    # constructor
    def __init__(self, **kwarg):
        FIFOBase.__init__(self, **kwarg)
        self.fifoName = 'monitor_fifo'
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

    def to_check_worker(self, seconds_ago=0):
        """
        Justify whether to check any worker by the modificationTime of the first worker in fifo
        Return True if the first worker is earlier than seconds_ago seconds ago. Otherwise False
        """
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_ident()), method_name='to_check_worker')
        retVal = False
        timeNow = datetime.datetime.utcnow()
        obj_peeked = self.peek()
        if obj_peeked is not None:
            queueName, workSpecsList = obj_peeked
            _workspec = workSpecsList[0][0]
            if timeNow - datetime.timedelta(seconds=seconds_ago) > _workspec.modificationTime:
                retVal = True
                mainLog.debug('True')
            else:
                mainLog.debug('False. Worker too young to check')
        else:
            mainLog.debug('False. No worker in FIFO')
        return retVal
