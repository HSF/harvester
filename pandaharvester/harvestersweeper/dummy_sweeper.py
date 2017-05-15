from pandaharvester.harvestercore.plugin_base import PluginBase


# dummy plugin for sweeper
class DummySweeper(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # kill worker
    def kill_worker(self, workspec):
        """Kill a worker. If that is done synchronously in trigger_preparation
        this method should always return True.

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        return True, ''
