import os
import time
import datetime
import collections
import socket
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
from pandaharvester.harvestercore.db_interface import DBInterface

# attribute list
_attribute_list = ['id', 'item', 'score']

# fifo object spec
FifoObject = collections.namedtuple('FifoObject', _attribute_list, verbose=False, rename=False)

# logger
_logger = core_utils.setup_logger('fifos')


# base class of fifo message queue
class FIFOBase(object):
    # constructor
    def __init__(self, **kwarg):
        for tmpKey, tmpVal in iteritems(kwarg):
            setattr(self, tmpKey, tmpVal)
        self.hostname = socket.gethostname()
        self.os_pid = os.getpid()
        self.dbProxy = DBProxy()
        self.dbInterface = DBInterface()

    # get process identifier
    def get_pid(self):
        thread_id = get_ident()
        if thread_id is None:
            thread_id = 0
        return '{0}_{1}-{2}'.format(self.hostname, self.os_pid, format(get_ident(), 'x'))

    # make logger
    def make_logger(self, base_log, token=None, method_name=None, send_dialog=True):
        if send_dialog and hasattr(self, 'dbInterface'):
            hook = self.dbInterface
        else:
            hook = None
        return core_utils.make_logger(base_log, token=token, method_name=method_name, hook=hook)

    # intialize fifo from harvester configuration
    def _initialize_fifo(self):
        self.fifoName = '{0}_fifo'.format(self.titleName)
        self.config = getattr(harvester_config, self.titleName)
        if hasattr(self.config, 'fifoEnable') and self.config.fifoEnable:
            self.enabled = True
        else:
            self.enabled = False
            return
        pluginConf = vars(self.config).copy()
        pluginConf.update( {'titleName': self.titleName} )
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

    # encode
    def encode(self, obj):
        obj_serialized = pickle.dumps(obj, -1)
        return obj_serialized

    # decode
    def decode(self, obj_serialized):
        obj = pickle.loads(obj_serialized)
        return obj

    # size of queue
    def size(self):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, self.get_pid()), method_name='size')
        retVal = self.fifo.size()
        mainLog.debug('size={0}'.format(retVal))
        return retVal

    # enqueue
    def put(self, obj, score=None, encode_item=True):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, self.get_pid()), method_name='put')
        if encode_item:
            # obj_serialized = json.dumps(obj, cls=PythonObjectEncoder)
            obj_serialized = self.encode(obj)
        else:
            obj_serialized = obj
        if score is None:
            score = time.time()
        retVal = self.fifo.put(obj_serialized, score)
        mainLog.debug('score={0}'.format(score))
        return retVal

    # enqueue by id, which is unique
    def putbyid(self, id, obj, score=None, encode_item=True):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, self.get_pid()), method_name='putbyid')
        if encode_item:
            # obj_serialized = json.dumps(obj, cls=PythonObjectEncoder)
            obj_serialized = self.encode(obj)
        else:
            obj_serialized = obj
        if score is None:
            score = time.time()
        retVal = self.fifo.putbyid(id, obj_serialized, score)
        mainLog.debug('id={0} score={1}'.format(id, score))
        return retVal

    # dequeue to get the first fifo object
    def get(self, timeout=None, protective=False, decode_item=True):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, self.get_pid()), method_name='get')
        object_tuple = self.fifo.get(timeout, protective)
        # retVal = json.loads(obj_serialized, object_hook=as_python_object)
        if object_tuple is None:
            retVal = None
        else:
            id, obj_serialized, score = object_tuple
            if decode_item:
                obj = self.decode(obj_serialized)
            else:
                obj = obj_serialized
            retVal = FifoObject(id, obj, score)
        mainLog.debug('called. protective={0}'.format(protective))
        return retVal

    # dequeue to get the last fifo object
    def getlast(self, timeout=None, protective=False, decode_item=True):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, self.get_pid()), method_name='getlast')
        object_tuple = self.fifo.getlast(timeout, protective)
        # retVal = json.loads(obj_serialized, object_hook=as_python_object)
        if object_tuple is None:
            retVal = None
        else:
            id, obj_serialized, score = object_tuple
            if decode_item:
                obj = self.decode(obj_serialized)
            else:
                obj = obj_serialized
            retVal = FifoObject(id, obj, score)
        mainLog.debug('called. protective={0}'.format(protective))
        return retVal

    # get tuple of the first object and its score without dequeuing
    def peek(self):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, self.get_pid()), method_name='peek')
        id, obj_serialized, score = self.fifo.peek()
        # retVal = (json.loads(obj_serialized, object_hook=as_python_object), score)
        if obj_serialized is None and score is None:
            retVal = FifoObject(None, None, None)
        else:
            if score is None:
                score = time.time()
            retVal = FifoObject(id, obj_serialized, score)
        mainLog.debug('score={0}'.format(score))
        return retVal

    # get tuple of the last object and its score without dequeuing
    def peeklast(self):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, self.get_pid()), method_name='peeklast')
        id, obj_serialized, score = self.fifo.peeklast()
        # retVal = (json.loads(obj_serialized, object_hook=as_python_object), score)
        if obj_serialized is None and score is None:
            retVal = FifoObject(None, None, None)
        else:
            if score is None:
                score = time.time()
            retVal = FifoObject(id, obj_serialized, score)
        mainLog.debug('score={0}'.format(score))
        return retVal

    # get tuple of the object by id without dequeuing
    def peekbyid(self, id, temporary=False):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, self.get_pid()), method_name='peekbyid')
        id_gotten, obj_serialized, score = self.fifo.peekbyid(id, temporary)
        # retVal = (json.loads(obj_serialized, object_hook=as_python_object), score)
        if obj_serialized is None and score is None:
            retVal = FifoObject(None, None, None)
        else:
            if score is None:
                score = time.time()
            retVal = FifoObject(id, obj_serialized, score)
        mainLog.debug('id={0} score={1} temporary={2}'.format(id, score, temporary))
        return retVal

    # remove objects by list of ids from temporary space
    def release(self, ids):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, self.get_pid()), method_name='release')
        retVal = self.fifo.delete(ids)
        mainLog.debug('released {0} objects in {1}'.format(retVal, ids))
        return retVal

    # restore objects by list of ids from temporary space to fifo; ids=None to restore all objects
    def restore(self, ids=None):
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, self.get_pid()), method_name='restore')
        retVal = self.fifo.restore(ids)
        if ids is None:
            mainLog.debug('restored all objects')
        else:
            mainLog.debug('restored objects in {0}'.format(ids))
        return retVal


# Special fifo base for non havester-agent
class SpecialFIFOBase(FIFOBase):
    # constructor
    def __init__(self, **kwarg):
        FIFOBase.__init__(self, **kwarg)
        self.fifoName = '{0}_fifo'.format(self.titleName)
        pluginConf = {}
        pluginConf.update( {'titleName': self.titleName} )
        pluginConf.update( {'module': harvester_config.fifo.fifoModule,
                            'name': harvester_config.fifo.fifoClass,} )
        pluginFactory = PluginFactory()
        self.fifo = pluginFactory.get_plugin(pluginConf)


# Benchmark fifo
class BenchmarkFIFO(SpecialFIFOBase):
    titleName = 'benchmark'


# monitor fifo
class MonitorFIFO(FIFOBase):
    titleName = 'monitor'

    # constructor
    def __init__(self, **kwarg):
        FIFOBase.__init__(self, **kwarg)
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
                and len(workspec_chunk) < fifoMaxWorkersPerChunk:
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
        mainLog = self.make_logger(_logger, 'id={0}-{1}'.format(self.fifoName, self.get_pid()), method_name='to_check_worker')
        retVal = False
        overhead_time = None
        timeNow_timestamp = time.time()
        peeked_tuple = self.peek()
        if peeked_tuple.item is not None:
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
        elif peeked_tuple.score is None:
            mainLog.debug('False. Got nothing in FIFO')
        else:
            mainLog.warning('False. Got a null object but with score in FIFO')
            try:
                obj_gotten = self.get(timeout=1, protective=False, decode_item=False)
                if obj_gotten is None:
                    mainLog.debug('Got nothing. Skipped')
                elif obj_gotten.item is None:
                    mainLog.info('Removed a null object')
                else:
                    self.put(obj_gotten.item, score=obj_gotten.score, encode_item=False)
                    mainLog.debug('Released an non-null object and put it back')
            except Exception as _e:
                mainLog.warning('Error when trying to remove a null object: {0} . Skipped'.format(_e))
        return retVal, overhead_time
