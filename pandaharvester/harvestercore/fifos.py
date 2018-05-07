import time
import datetime
from calendar import timegm
from future.utils import iteritems

import json
try:
    import cPickle as pickle
except ImportError:
    import pickle

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


# # unicode, str, bytes of python 2 and 3
# try:
#     unicodeOrStr = unicode
# except NameError:
#     unicodeOrStr = str
# try:
#     strOrBytes = bytes
# except NameError:
#     strOrBytes = str
#
#
# # encoder for non-native json objects
# class PythonObjectEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, (list, dict, unicodeOrStr, str, int, float, bool, type(None))):
#             return json.JSONEncoder.default(self, obj)
#         elif isinstance(obj, (bytearray, strOrBytes)):
#             return json.JSONEncoder.default(self, obj.decode('ascii'))
#         else:
#             return {'_non_json_object': str(pickle.dumps(obj, 0))}
#
#
# # hook for decoder
# def as_python_object(dct):
#     if '_non_json_object' in dct:
#         return pickle.loads(str(dct['_non_json_object']).encode('ascii'))
#     return dct


# base class of fifo message queue
class FIFOBase:
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
        self.fifoName = '{0}_fifo'.format(self.agentName)
        self.config = getattr(harvester_config, self.agentName)
        pluginConf = vars(self.config).copy()
        pluginConf.update( {'agentName': self.agentName} )
        if hasattr(self.config, 'fifoModule') and hasattr(self.config, 'fifoClass'):
            pluginConf.update( {'module': self.config.fifoModule,
                                'name': self.config.fifoClass,} )
        else:
            if not hasattr(harvester_config, 'fifo'):
                return
            pluginConf.update( {'module': harvester_config.fifo.fifoModule,
                                'name': harvester_config.fifo.fifoClass,} )
        pluginFactory = PluginFactory()
        self.fifo = pluginFactory.get_plugin(pluginConf)

    # size of queue
    def size(self):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_ident()), method_name='size')
        retVal = self.fifo.size()
        mainLog.debug('size={0}'.format(retVal))
        return retVal

    # enqueue
    def put(self, obj, score=time.time()):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_ident()), method_name='put')
        # obj_serialized = json.dumps(obj, cls=PythonObjectEncoder)
        obj_serialized = pickle.dumps(obj, -1)
        retVal = self.fifo.put(obj_serialized, score)
        mainLog.debug('score={0}'.format(score))
        return retVal

    # dequeue
    def get(self, timeout=None):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_ident()), method_name='get')
        obj_serialized = self.fifo.get(timeout)
        # retVal = json.loads(obj_serialized, object_hook=as_python_object)
        if obj_serialized is None:
            retVal = None
        else:
            retVal = pickle.loads(obj_serialized)
        mainLog.debug('called')
        return retVal

    # get tuple of object and its score without dequeuing
    def peek(self):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_ident()), method_name='peek')
        obj_serialized, score = self.fifo.peek()
        # retVal = (json.loads(obj_serialized, object_hook=as_python_object), score)
        if obj_serialized is None and score is None:
            retVal = None, None
        else:
            retVal = (pickle.loads(obj_serialized), score)
        mainLog.debug('score={0}'.format(score))
        return retVal


# monitor fifo
class MonitorFIFO(FIFOBase):
    # constructor
    def __init__(self, **kwarg):
        FIFOBase.__init__(self, **kwarg)
        self.agentName = 'monitor'
        self._initialize_fifo()

    def populate(self, seconds_ago=0, clear_fifo=False):
        """
        Populate monitor fifo with all active worker chunks from DB
        with modificationTime earlier than seconds_ago seconds ago
        object in fifo = [(queueName_1, [[worker_1_1], [worker_1_2], ...]), (queueName_2, ...)]
        """
        if clear_fifo:
            self.fifo.clear()
        n_workers = self.config.fifoMaxWorkersToPopulate
        workspec_iterator = self.dbProxy.get_active_workers(n_workers, seconds_ago)
        last_queueName = None
        workspec_chunk = []
        timeNow_timestamp = time.time()
        score = timeNow_timestamp
        for workspec in workspec_iterator:
            workspec.set_work_params({'lastCheckAt': timeNow_timestamp})
            if last_queueName == None:
                try:
                    score = timegm(workspec.modificationTime.utctimetuple())
                except Exception:
                    pass
                workspec_chunk = [[workspec]]
                last_queueName = workspec.computingSite
            elif workspec.computingSite == last_queueName \
                and len(workspec_chunk) < self.config.fifoMaxWorkersPerChunk:
                workspec_chunk.append([workspec])
            else:
                self.put((last_queueName, workspec_chunk), score)
                try:
                    score = timegm(workspec.modificationTime.utctimetuple())
                except Exception:
                    pass
                workspec_chunk = [[workspec]]
                last_queueName = workspec.computingSite
        if len(workspec_chunk) > 0:
            self.put((last_queueName, workspec_chunk), score)

    def to_check_workers(self, check_interval=harvester_config.monitor.checkInterval):
        """
        Justify whether to check any worker by the modificationTime of the first worker in fifo
        Return True if OK to dequeue to check;
        Return False otherwise.
        """
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_ident()), method_name='to_check_worker')
        retVal = False
        timeNow_timestamp = time.time()
        peeked_tuple = self.peek()
        if peeked_tuple[0] is not None:
            queueName, workSpecsList = peeked_tuple[0]
            score = peeked_tuple[1]
            check_interval = harvester_config.monitor.checkInterval
            if timeNow_timestamp - check_interval > score:
                retVal = True
                mainLog.debug('True')
            else:
                mainLog.debug('False. Workers younger than {0} seconds ago to check'.format(check_interval))
        else:
            mainLog.debug('False. No workers in FIFO')
        return retVal
