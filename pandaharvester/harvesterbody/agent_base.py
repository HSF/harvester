import threading
from pandaharvester.harvestercore import core_utils


# base class for agents
class AgentBase(threading.Thread):

    # constructor
    def __init__(self, single_mode):
        threading.Thread.__init__(self)
        self.singleMode = single_mode
        self.stopEvent = None


    # set stop event
    def set_stop_event(self, stop_event):
        self.stopEvent = stop_event


    # check if going to be terminated
    def terminated(self, wait_interval):
        if self.singleMode:
            return True
        return core_utils.sleep(wait_interval, self.stopEvent)
