import abc

from pandaharvester.harvestercore.plugin_base import PluginBase


# dummy plugin for sweeper
class BaseSweeper(PluginBase, metaclass=abc.ABCMeta):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # kill a worker
    # def kill_worker(self, workspec):
    #     retList = self.kill_workers([workspec])
    #     return retList[0]

    # kill workers
    # @abc.abstractmethod
    # def kill_workers(self, workspec_list):
    #     pass

    # cleanup for a worker
    @abc.abstractmethod
    def sweep_worker(self, workspec):
        pass
