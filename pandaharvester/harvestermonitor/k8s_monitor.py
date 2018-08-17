import os
import time
import datetime
import re

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.k8s_utils import k8s_Client


# logger
baseLogger = core_utils.setup_logger('k8s_monitor')


# monitor for K8S
class K8sMonitor (PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        try:
            self.nProcesses
        except AttributeError:
            self.nProcesses = 4
        try:
            self.cancelUnknown
        except AttributeError:
            self.cancelUnknown = False
        else:
            self.cancelUnknown = bool(self.cancelUnknown)

    # check workers
    def check_workers(self, workspec_list):
        tmpLog = self.make_logger(baseLogger, 'k8s query', method_name='check_workers')
        tmpLog.debug('start')

        #FIXME
        retList = list()

        tmpLog.debug('done')

        return True, retList
