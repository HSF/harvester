import os
import queue
import threading
from pandaharvester.harvesterconfig import harvester_config
from .db_proxy import DBProxy
from . import core_utils

# logger
_logger = core_utils.setup_logger("db_proxy_pool")


# method wrapper
class DBProxyMethod(object):
    # constructor
    def __init__(self, method_name, pool):
        self.methodName = method_name
        self.pool = pool

    # method emulation
    def __call__(self, *args, **kwargs):
        tmpLog = core_utils.make_logger(_logger, "method={0}".format(self.methodName), method_name="call")
        sw = core_utils.get_stopwatch()
        try:
            # get connection
            con = self.pool.get()
            tmpLog.debug("got lock. qsize={0} {1}".format(self.pool.qsize(), sw.get_elapsed_time()))
            sw.reset()
            # get function
            func = getattr(con, self.methodName)
            # exec
            return func(*args, **kwargs)
        finally:
            tmpLog.debug("release lock" + sw.get_elapsed_time())
            self.pool.put(con)


# connection class
class DBProxyPool(object):
    instance = None
    lock = threading.Lock()

    # constructor
    def __init__(self, read_only=False):
        pass

    # initialize
    def initialize(self, read_only=False):
        # install members
        object.__setattr__(self, "pool", None)
        # connection pool
        self.pool = queue.Queue(harvester_config.db.nConnections)
        currentThr = threading.current_thread()
        if currentThr is None:
            thrID = None
        else:
            thrID = currentThr.ident
        thrName = "{0}-{1}".format(os.getpid(), thrID)
        for i in range(harvester_config.db.nConnections):
            con = DBProxy(thr_name="{0}-{1}".format(thrName, i), read_only=read_only)
            self.pool.put(con)

    # override __new__ to have a singleton
    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            with cls.lock:
                if cls.instance is None:
                    if "read_only" in kwargs and kwargs["read_only"]:
                        read_only = True
                    else:
                        read_only = False
                    cls.instance = super(DBProxyPool, cls).__new__(cls, *args, **kwargs)
                    cls.instance.initialize(read_only=read_only)
        return cls.instance

    # override __getattribute__
    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except Exception:
            pass
        # method object
        tmpO = DBProxyMethod(name, self.pool)
        object.__setattr__(self, name, tmpO)
        return tmpO
