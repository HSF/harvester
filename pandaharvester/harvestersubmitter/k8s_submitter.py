import os
import datetime
import re

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.k8s_utils import k8s_Client


# logger
baseLogger = core_utils.setup_logger('k8s_submitter')


# submitter for K8S
class K8sSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)
        # number of processes
        try:
            self.nProcesses
        except AttributeError:
            self.nProcesses = 1
        else:
            if (not self.nProcesses) or (self.nProcesses < 1):
                self.nProcesses = 1
        # x509 proxy
        try:
            self.x509UserProxy
        except AttributeError:
            self.x509UserProxy = os.getenv('X509_USER_PROXY')

    # submit workers
    def submit_workers(self, workspec_list):
        tmpLog = self.make_logger(baseLogger, method_name='submit_workers')

        nWorkers = len(workspec_list)
        tmpLog.debug('start, nWorkers={0}'.format(nWorkers))

        #FIXME
        retList = list()

        tmpLog.debug('done')

        return retList
