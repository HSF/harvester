import os

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.k8s_utils import k8s_Client


# logger
baseLogger = core_utils.setup_logger('k8s_sweeper')


# sweeper for K8S
class K8sSweeper(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)


    # kill a worker
    def kill_worker(self, workspec):
        tmpLog = self.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
                                  method_name='kill_worker')

        #FIXME

        return True, ''


    # cleanup for a worker
    def sweep_worker(self, workspec):
        ## Make logger
        tmpLog = self.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
                                  method_name='sweep_worker')

        # make sure job/pod is terminated
        self.kill_worker(workspec)

        #FIXME

        return True, ''
