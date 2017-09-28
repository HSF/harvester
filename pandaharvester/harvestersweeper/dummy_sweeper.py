from pandaharvester.harvestercore.plugin_base import PluginBase


# dummy plugin for sweeper
class DummySweeper(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # kill a worker
    def kill_worker(self, workspec):
        """Kill a worker in a scheduling system like batch systems and computing elements.

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        return True, ''

    # cleanup for a worker
    def sweep_worker(self, workspec):
        """Perform cleanup procedures for a worker, such as deletion of work directory.
        The list of JobSpecs associated to the worker is available in workspec.get_jobspec_list().
        The list of input and output FileSpecs, which are not used by any active jobs and thus can
        safely be deleted, is available in JobSpec.get_files_to_delete().

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        return True, ''
