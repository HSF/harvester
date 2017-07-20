import os
import threading
from pandaharvester.harvestercore import core_utils


# base class for agents
class AgentBase(threading.Thread):

    # constructor
    def __init__(self, single_mode):
        threading.Thread.__init__(self)
        self.singleMode = single_mode
        self.stopEvent = None
        self.os_pid = os.getpid()

    # set stop event
    def set_stop_event(self, stop_event):
        self.stopEvent = stop_event

    # check if going to be terminated
    def terminated(self, wait_interval, randomize=True):
        if self.singleMode:
            return True
        return core_utils.sleep(wait_interval, self.stopEvent, randomize)

    # get process identifier
    def get_pid(self):
        return '{0}-{1}'.format(self.os_pid, self.ident)
