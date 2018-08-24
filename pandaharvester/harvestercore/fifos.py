import os
import time
import datetime
import collections
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

# attribute list
_attribute_list = ['id', 'item', 'score']

# fifo object spec
FifoObject = collections.namedtuple('FifoObject', _attribute_list, verbose=False, rename=False)

# logger
_logger = core_utils.setup_logger('fifos')

# get process identifier
def get_pid():
    return '{0}-{1}'.format(os.getpid(), get_ident())


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
        if hasattr(self.config, 'fifoEnable') and self.config.fifoEnable:
            self.enabled = True
        else:
            self.enabled = False
            return
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
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_pid()), method_name='size')
        retVal = self.fifo.size()
        mainLog.debug('size={0}'.format(retVal))
        return retVal

    # enqueue
    def put(self, obj, score=None):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_pid()), method_name='put')
        # obj_serialized = json.dumps(obj, cls=PythonObjectEncoder)
        obj_serialized = pickle.dumps(obj, -1)
        if score is None:
            score = time.time()
        retVal = self.fifo.put(obj_serialized, score)
        mainLog.debug('score={0}'.format(score))
        return retVal

    # dequeue to get the fifo object
    def get(self, timeout=None, protective=False):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_pid()), method_name='get')
        object_tuple = self.fifo.get(timeout, protective)
        # retVal = json.loads(obj_serialized, object_hook=as_python_object)
        if object_tuple is None:
            retVal = None
        else:
            id, obj_serialized, score = object_tuple
            retVal = FifoObject(id, pickle.loads(obj_serialized), score)
        mainLog.debug('called. protective={0}'.format(protective))
        return retVal

    # get tuple of object and its score without dequeuing
    def peek(self):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_pid()), method_name='peek')
        obj_serialized, score = self.fifo.peek()
        # retVal = (json.loads(obj_serialized, object_hook=as_python_object), score)
        if obj_serialized is None and score is None:
            retVal = FifoObject(None, None, None)
        else:
            if score is None:
                score = time.time()
            retVal = FifoObject(None, obj_serialized, score)
        mainLog.debug('score={0}'.format(score))
        return retVal

    # remove objects by list of ids from temporary space
    def release(self, ids):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_pid()), method_name='release')
        retVal = self.fifo.delete(ids)
        mainLog.debug('released objects in {0}'.format(ids))
        return retVal

    # restore all objects from temporary space to fifo
    def restore(self):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_pid()), method_name='restore')
        retVal = self.fifo.restore()
        mainLog.debug('called')
        return retVal


# Benchmark fifo
class BenchmarkFIFO(FIFOBase):
    # constructor
    def __init__(self, **kwarg):
        FIFOBase.__init__(self, **kwarg)
        self.agentName = 'benchmark'
        self.fifoName = '{0}_fifo'.format(self.agentName)
        pluginConf = {}
        pluginConf.update( {'agentName': self.agentName} )
        pluginConf.update( {'module': harvester_config.fifo.fifoModule,
                            'name': harvester_config.fifo.fifoClass,} )
        pluginFactory = PluginFactory()
        self.fifo = pluginFactory.get_plugin(pluginConf)


# monitor fifo
class MonitorFIFO(FIFOBase):
    # constructor
    def __init__(self, **kwarg):
        FIFOBase.__init__(self, **kwarg)
        self.agentName = 'monitor'
        self._initialize_fifo()

    def populate(self, seconds_ago=0, clear_fifo=False):
        """
        Populate monitor fifo with all active worker chunks and timeNow as score from DB
        with modificationTime earlier than seconds_ago seconds ago
        object in fifo = [(queueName_1, [[worker_1_1], [worker_1_2], ...]), (queueName_2, ...)]
        """
        if clear_fifo:
            self.fifo.clear()
        try:
            fifoMaxWorkersToPopulate = self.config.fifoMaxWorkersToPopulate
        except AttributeError:
            fifoMaxWorkersToPopulate = 2**32
        try:
            fifoMaxWorkersPerChunk = self.config.fifoMaxWorkersPerChunk
        except AttributeError:
            fifoMaxWorkersPerChunk = 500
        workspec_iterator = self.dbProxy.get_active_workers(fifoMaxWorkersToPopulate, seconds_ago)
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
        retVal True if OK to dequeue to check;
        retVal False otherwise.
        Return retVal, overhead_time
        """
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, get_pid()), method_name='to_check_worker')
        retVal = False
        overhead_time = None
        timeNow_timestamp = time.time()
        peeked_tuple = self.peek()
        score = peeked_tuple.score
        overhead_time = timeNow_timestamp - score
        if overhead_time > 0:
            retVal = True
            if score < 0:
                mainLog.debug('True. Preempting')
                overhead_time = None
            else:
                mainLog.debug('True')
                mainLog.info('Overhead time is {0} sec'.format(overhead_time))
        else:
            mainLog.debug('False. Workers too young to check')
            mainLog.debug('Overhead time is {0} sec'.format(overhead_time))
        return retVal, overhead_time
