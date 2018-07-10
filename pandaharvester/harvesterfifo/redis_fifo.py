import os
import time
import re

import redis

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config


class RedisFifo(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        _redis_conn_opt_dict = {}
        if hasattr(self, 'redisHost'):
            _redis_conn_opt_dict['host'] = self.redisHost
        elif hasattr(harvester_config.fifo, 'redisHost'):
            _redis_conn_opt_dict['host'] = harvester_config.fifo.redisHost
        if hasattr(self, 'redisPort'):
            _redis_conn_opt_dict['port'] = self.redisPort
        elif hasattr(harvester_config.fifo, 'redisPort'):
            _redis_conn_opt_dict['port'] = harvester_config.fifo.redisPort
        if hasattr(self, 'redisDB'):
            _redis_conn_opt_dict['db'] = self.redisDB
        elif hasattr(harvester_config.fifo, 'redisDB'):
            _redis_conn_opt_dict['db'] = harvester_config.fifo.redisDB
        if hasattr(self, 'redisPassword'):
            _redis_conn_opt_dict['password'] = self.redisPassword
        elif hasattr(harvester_config.fifo, 'redisPassword'):
            _redis_conn_opt_dict['password'] = harvester_config.fifo.redisPassword
        self.qconn = redis.StrictRedis(**_redis_conn_opt_dict)
        self.qname = '{0}-fifo'.format(self.agentName)
        self.tname = '{0}-fifo_temp'.format(self.agentName)

    def __len__(self):
        return self.qconn.zcard(self.qname)

    def _peek(self, withscores=False):
        return self.qconn.zrange(self.qname, 0, 0, withscores=withscores)[0]

    def _peek_last(self, withscores=False):
        return self.qconn.zrevrange(self.qname, 0, 0, withscores=withscores)[0]

    def _pop(self, peek_method, timeout=None):
        keep_polling = True
        wait = 0.1
        max_wait = 2
        tries = 1
        last_attempt_timestamp = time.time()
        obj = None
        while keep_polling:
            try:
                obj = peek_method()
                ret_pop = self.qconn.zrem(self.qname, obj)
                if ret_pop == 1:
                    break
            except IndexError:
                time.sleep(wait)
                wait = min(max_wait, tries/10.0 + wait)
            tries += 1
            now_timestamp = time.time()
            if timeout is not None and (now_timestamp - last_attempt_timestamp) >= timeout:
                break
        return obj

    # number of objects in queue
    def size(self):
        return len(self)

    # enqueue with priority score
    def put(self, obj, score):
        return self.qconn.zadd(self.qname, score, obj)

    # dequeue the first object
    def get(self, timeout=None):
        return self._pop(self._peek, timeout)

    # dequeue the last object
    def getlast(self, timeout=None):
        return self._pop(self._peek_last, timeout)

    # copy object to temp and then dequeue; return object and ID of object in temp
    def conservative_get(self, timeout=None):
        return self._pop(self._peek, timeout)

    # get tuple of (obj, score) of the first object without dequeuing it
    def peek(self):
        try:
            return self._peek(withscores=True)
        except IndexError:
            return None, None

    # drop all objects in queue
    def clear(self):
        self.qconn.delete(self.qname)
