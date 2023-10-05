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
        if hasattr(self, "redisHost"):
            _redis_conn_opt_dict["host"] = self.redisHost
        elif hasattr(harvester_config.fifo, "redisHost"):
            _redis_conn_opt_dict["host"] = harvester_config.fifo.redisHost
        if hasattr(self, "redisPort"):
            _redis_conn_opt_dict["port"] = self.redisPort
        elif hasattr(harvester_config.fifo, "redisPort"):
            _redis_conn_opt_dict["port"] = harvester_config.fifo.redisPort
        if hasattr(self, "redisDB"):
            _redis_conn_opt_dict["db"] = self.redisDB
        elif hasattr(harvester_config.fifo, "redisDB"):
            _redis_conn_opt_dict["db"] = harvester_config.fifo.redisDB
        if hasattr(self, "redisPassword"):
            _redis_conn_opt_dict["password"] = self.redisPassword
        elif hasattr(harvester_config.fifo, "redisPassword"):
            _redis_conn_opt_dict["password"] = harvester_config.fifo.redisPassword
        self.qconn = redis.StrictRedis(**_redis_conn_opt_dict)
        self.id_score = "{0}-fifo_id-score".format(self.titleName)
        self.id_item = "{0}-fifo_id-item".format(self.titleName)
        self.id_temp = "{0}-fifo_id-temp".format(self.titleName)

    def __len__(self):
        return self.qconn.zcard(self.id_score)

    def _peek(self, mode="first", id=None, skip_item=False):
        if mode == "first":
            try:
                id_gotten, score = self.qconn.zrange(self.id_score, 0, 0, withscores=True)[0]
            except IndexError:
                return None
        elif mode == "last":
            try:
                id_gotten, score = self.qconn.zrevrange(self.id_score, 0, 0, withscores=True)[0]
            except IndexError:
                return None
        else:
            resVal = self.qconn.sismember(self.id_temp, id)
            if (mode == "id" and not resVal) or (mode == "idtemp" and resVal):
                id_gotten = id
                score = self.qconn.zscore(self.id_score, id)
            else:
                id_gotten, score = None, None
        if skip_item:
            item = None
        else:
            item = self.qconn.hget(self.id_item, id_gotten)
        if id_gotten is None:
            return None
        else:
            return (id_gotten, item, score)

    def _pop(self, timeout=None, protective=False, mode="first"):
        keep_polling = True
        wait = 0.1
        max_wait = 2
        tries = 1
        last_attempt_timestamp = time.time()
        id, item, score = None, None, None
        while keep_polling:
            peeked_tuple = self._peek(mode=mode)
            if peeked_tuple is None:
                time.sleep(wait)
                wait = min(max_wait, tries / 10.0 + wait)
            else:
                id, item, score = peeked_tuple
                while True:
                    try:
                        with self.qconn.pipeline() as pipeline:
                            pipeline.watch(self.id_score, self.id_item, self.id_temp)
                            pipeline.multi()
                            if protective:
                                pipeline.sadd(self.id_temp, id)
                                pipeline.zrem(self.id_score, id)
                            else:
                                pipeline.srem(self.id_temp, id)
                                pipeline.hdel(self.id_item, id)
                                pipeline.zrem(self.id_score, id)
                            resVal = pipeline.execute()
                    except redis.WatchError:
                        continue
                    else:
                        break
                if resVal[-2] == 1 and resVal[-1] == 1:
                    break
            tries += 1
            now_timestamp = time.time()
            if timeout is not None and (now_timestamp - last_attempt_timestamp) >= timeout:
                break
        return id, item, score

    # number of objects in queue
    def size(self):
        return len(self)

    # enqueue with priority score
    def put(self, item, score):
        generate_id_attempt_timestamp = time.time()
        while True:
            id = random_id()
            resVal = None
            with self.qconn.pipeline() as pipeline:
                while True:
                    try:
                        pipeline.watch(self.id_score, self.id_item)
                        pipeline.multi()
                        pipeline.execute_command("ZADD", self.id_score, "NX", score, id)
                        pipeline.hsetnx(self.id_item, id, item)
                        resVal = pipeline.execute()
                    except redis.WatchError:
                        continue
                    else:
                        break
            if resVal is not None:
                if resVal[-2] == 1 and resVal[-1] == 1:
                    return True
            if time.time() > generate_id_attempt_timestamp + 60:
                raise Exception("Cannot generate unique id")
                return False
            time.sleep(0.0001)
        return False

    # enqueue by id
    def putbyid(self, id, item, score):
        with self.qconn.pipeline() as pipeline:
            while True:
                try:
                    pipeline.watch(self.id_score, self.id_item)
                    pipeline.multi()
                    pipeline.execute_command("ZADD", self.id_score, "NX", score, id)
                    pipeline.hsetnx(self.id_item, id, item)
                    resVal = pipeline.execute()
                except redis.WatchError:
                    continue
                else:
                    break
        if resVal is not None:
            if resVal[-2] == 1 and resVal[-1] == 1:
                return True
        return False

    # dequeue the first object
    def get(self, timeout=None, protective=False):
        return self._pop(timeout=timeout, protective=protective, mode="first")

    # dequeue the last object
    def getlast(self, timeout=None, protective=False):
        return self._pop(timeout=timeout, protective=protective, mode="last")

    # get tuple of (id, item, score) of the first object without dequeuing it
    def peek(self, skip_item=False):
        return self._peek(skip_item=skip_item)

    # get tuple of (id, item, score) of the last object without dequeuing it
    def peeklast(self, skip_item=False):
        return self._peek(mode="last", skip_item=skip_item)

    # get tuple of (id, item, score) of object by id without dequeuing it
    def peekbyid(self, id, temporary=False, skip_item=False):
        if temporary:
            return self._peek(mode="idtemp", id=id, skip_item=skip_item)
        else:
            return self._peek(mode="id", id=id, skip_item=skip_item)

    # drop all objects in queue
    def clear(self):
        with self.qconn.pipeline() as pipeline:
            while True:
                try:
                    pipeline.watch(self.id_score, self.id_item, self.id_temp)
                    pipeline.multi()
                    pipeline.delete(self.id_score)
                    pipeline.delete(self.id_item)
                    pipeline.delete(self.id_temp)
                    pipeline.execute()
                except redis.WatchError:
                    continue
                else:
                    break

    # delete objects by list of id
    def delete(self, ids):
        if isinstance(ids, (list, tuple)):
            with self.qconn.pipeline() as pipeline:
                while True:
                    try:
                        pipeline.watch(self.id_score, self.id_item, self.id_temp)
                        pipeline.multi()
                        pipeline.srem(self.id_temp, *ids)
                        pipeline.hdel(self.id_item, *ids)
                        pipeline.zrem(self.id_score, *ids)
                        resVal = pipeline.execute()
                    except redis.WatchError:
                        continue
                    else:
                        n_row = resVal[-1]
                        return n_row
        else:
            raise TypeError("ids should be list or tuple")

    # Move objects in temporary space to the queue
    def restore(self, ids):
        with self.qconn.pipeline() as pipeline:
            while True:
                now_timestamp = time.time()
                try:
                    pipeline.watch(self.id_score, self.id_item, self.id_temp)
                    if ids is None:
                        pipeline.multi()
                        pipeline.delete(self.id_temp)
                        pipeline.execute()
                    elif isinstance(ids, (list, tuple)):
                        if len(ids) > 0:
                            pipeline.multi()
                            pipeline.srem(self.id_temp, *ids)
                            pipeline.execute()
                    else:
                        raise TypeError("ids should be list or tuple or None")
                except redis.WatchError:
                    continue
                else:
                    break
