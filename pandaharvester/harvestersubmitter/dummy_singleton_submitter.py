import threading
from pandaharvester.harvestercore import core_utils

from .dummy_submitter import DummySubmitter

# setup base logger
baseLogger = core_utils.setup_logger("dummy_singleton_submitter")


# dummy submitter with singleton
class DummySingletonSubmitter(object):
    instances = dict()
    lock = threading.Lock()

    # constructor
    def __init__(self, **kwarg):
        key = kwarg["queueName"]
        cls = self.__class__
        with cls.lock:
            if key not in cls.instances:
                instance = DummySubmitter(**kwarg)
                cls.instances[key] = instance
        self.key = key

    def __getattr__(self, name):
        cls = self.__class__
        return getattr(cls.instances[self.key], name)
