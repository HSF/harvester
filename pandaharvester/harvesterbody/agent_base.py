import os
import threading
import socket
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_interface import DBInterface


# base class for agents
class AgentBase(threading.Thread):
    # constructor
    def __init__(self, single_mode):
        threading.Thread.__init__(self)
        self.singleMode = single_mode
        self.stopEvent = None
        self.hostname = socket.gethostname()
        self.os_pid = os.getpid()
        self.dbInterface = DBInterface()

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
        thread_id = self.ident if self.ident else 0
        return "{0}_{1}-{2}".format(self.hostname, self.os_pid, format(thread_id, "x"))

    # make logger
    def make_logger(self, base_log, token=None, method_name=None, send_dialog=True):
        if send_dialog and hasattr(self, "dbInterface"):
            hook = self.dbInterface
        else:
            hook = None
        return core_utils.make_logger(base_log, token=token, method_name=method_name, hook=hook)
