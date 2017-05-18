import Queue
import threading
from pandaharvester.harvesterconfig import harvester_config

from db_proxy import DBProxy


# method wrapper
class DBProxyMethod:
    # constructor
    def __init__(self, method_name, pool):
        self.methodName = method_name
        self.pool = pool

    # method emulation
    def __call__(self, *args, **kwargs):
        try:
            # get connection
            con = self.pool.get()
            # get function
            func = getattr(con, self.methodName)
            # exec
            return apply(func, args, kwargs)
        finally:
            self.pool.put(con)


# connection class
class DBProxyPool(object):
    instance = None
    lock = threading.Lock()

    # constructor
    def __init__(self):
        pass

    # initialize
    def initialize(self):
        # install members
        object.__setattr__(self, 'pool', None)
        # connection pool
        self.pool = Queue.Queue(harvester_config.db.nConnections)
        for i in range(harvester_config.db.nConnections):
            con = DBProxy()
            self.pool.put(con)

    # override __new__ to have a singleton
    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            with cls.lock:
                if cls.instance is None:
                    cls.instance = super(DBProxyPool, cls).__new__(cls, *args, **kwargs)
                    cls.instance.initialize()
        return cls.instance

    # override __getattribute__
    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except:
            pass
        # method object
        tmpO = DBProxyMethod(name, self.pool)
        object.__setattr__(self, name, tmpO)
        return tmpO
