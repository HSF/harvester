import queue

from pandaharvester.harvesterconfig import harvester_config
from .communicator import Communicator
from . import core_utils

# logger
_logger = core_utils.setup_logger('communicator_pool')


# method wrapper
class CommunicatorMethod:
    # constructor
    def __init__(self, method_name, pool):
        self.methodName = method_name
        self.pool = pool

    # method emulation
    def __call__(self, *args, **kwargs):
        tmpLog = core_utils.make_logger(_logger, 'method={0}'.format(self.methodName), method_name='call')
        sw = core_utils.get_stopwatch()
        try:
            # get connection
            con = self.pool.get()
            tmpLog.debug('got lock. qsize={0} {1}'.format(self.pool.qsize(), sw.get_elapsed_time()))
            sw.reset()
            # get function
            func = getattr(con, self.methodName)
            # exec
            return func(*args, **kwargs)
        finally:
            tmpLog.debug('release lock' + sw.get_elapsed_time())
            self.pool.put(con)


# connection class
class CommunicatorPool(object):
    # constructor
    def __init__(self):
        # install members
        object.__setattr__(self, 'pool', None)
        # connection pool
        self.pool = queue.Queue(harvester_config.pandacon.nConnections)
        for i in range(harvester_config.pandacon.nConnections):
            con = Communicator()
            self.pool.put(con)

    # override __getattribute__
    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except:
            pass
        # method object
        tmpO = CommunicatorMethod(name, self.pool)
        object.__setattr__(self, name, tmpO)
        return tmpO
