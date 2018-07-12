import os
import time
import re
import random

import redis

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config


def random_id():
    return random.randrange(2**30)


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
        self.idp = '{0}-fifo_id-pool'.format(self.agentName)
        self.titem = '{0}-fifo_temp-item'.format(self.agentName)
        self.tscore = '{0}-fifo_temp-score'.format(self.agentName)

    def __len__(self):
        return self.qconn.zcard(self.qname)

    def _peek(self):
        return self.qconn.zrange(self.qname, 0, 0, withscores=True)[0]

    def _peek_last(self):
        return self.qconn.zrevrange(self.qname, 0, 0, withscores=True)[0]

    def _pop(self, peek_method, timeout=None, protective=False):
        keep_polling = True
        wait = 0.1
        max_wait = 2
        tries = 1
        last_attempt_timestamp = time.time()
        obj = None
        while keep_polling:
            try:
                generate_id_attempt_timestamp = time.time()
                while True:
                    id = random_id()
                    resVal = self.sadd(self.idp, id)
                    if resVal:
                        break
                    elif time.time() > generate_id_attempt_timestamp + 60:
                        raise Exception('Cannot generate unique id')
                        return
                if protective:
                    item, score = self.qconn.zrange(self.qname, 0, 0, withscores=True)[0]
                    with self.qconn.pipeline() as pipeline:
                        pipeline.hset(self.titem, id, item)
                        pipeline.hset(self.tscore, id, score)
                        pipeline.zrem(self.qname, obj)
                        resVal = pipeline.execute()
                    ret_pop = resVal[-1]
                else:
                    item, score = peek_method()
                    ret_pop = self.qconn.zrem(self.qname, obj)
                if ret_pop == 1:
                    break
                elif protective:
                    with self.qconn.pipeline() as pipeline:
                        pipeline.hdel(self.titem, id)
                        pipeline.hdel(self.tscore, id)
                        resVal = pipeline.execute()
            except IndexError:
                time.sleep(wait)
                wait = min(max_wait, tries/10.0 + wait)
            tries += 1
            now_timestamp = time.time()
            if timeout is not None and (now_timestamp - last_attempt_timestamp) >= timeout:
                break
        return id, item, score

    # number of objects in queue
    def size(self):
        return len(self)

    # enqueue with priority score
    def put(self, obj, score):
        return self.qconn.zadd(self.qname, score, obj)

    # dequeue the first object
    def get(self, timeout=None, protective=False):
        return self._pop(self._peek, timeout)

    # dequeue the last object
    def getlast(self, timeout=None):
        return self._pop(self._peek_last, timeout)

    # get tuple of (item, score) of the first object without dequeuing it
    def peek(self):
        try:
            item, score = self._peek()
            return item, score
        except IndexError:
            return None, None

    # drop all objects in queue
    def clear(self):
        with self.qconn.pipeline() as pipeline:
            while True:
                try:
                    pipeline.watch(self.qname, self.idp, self.titem, self.tscore)
                    pipeline.multi()
                    pipeline.delete(self.qname)
                    pipeline.delete(self.idp)
                    pipeline.delete(self.titem)
                    pipeline.delete(self.tscore)
                    pipeline.execute()
                    break
                except redis.WatchError:
                    continue

    # delete an object by list of id
    def delete(self, ids):
        if isinstance(ids, (list, tuple)):
            with self.qconn.pipeline() as pipeline:
                while True:
                    try:
                        pipeline.watch(self.qname, self.idp, self.titem, self.tscore)
                        item_list = pipeline.hmget(self.titem, ids)
                        pipeline.multi()
                        pipeline.hdel(self.tscore, *ids)
                        pipeline.hdel(self.titem, *ids)
                        pipeline.zrem(self.qname, *item_list)
                        pipeline.srem(self.idp, *ids)
                        pipeline.execute()
                    except redis.WatchError:
                        continue
        else:
            raise TypeError('ids should be list or tuple')

    # Move all object in temporary space to the queue
    def restore(self):
        with self.qconn.pipeline() as pipeline:
            while True:
                now_timestamp = time.time()
                try:
                    pipeline.watch(self.qname, self.idp, self.titem, self.tscore)
                    pipeline.multi()
                    temp_item_dict = pipeline.hgetall(self.titem)
                    temp_score_dict = pipeline.hgetall(self.tscore)
                    item_score_dict = { item: temp_score_dict.get(id, now_timestamp)
                                        for id, item in temp_item_dict.items() }
                    pipeline.zadd(self.qname, **item_score_dict)
                    pipeline.delete(self.titem)
                    pipeline.delete(self.tscore)
                    # pipeline.delete(self.idp)
                    pipeline.execute()
                except redis.WatchError:
                    continue
