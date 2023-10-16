from pandaharvester.harvestersweeper.base_sweeper import BaseSweeper


# dummy plugin for sweeper
class DummySweeper(BaseSweeper):
    # constructor
    def __init__(self, **kwarg):
        BaseSweeper.__init__(self, **kwarg)

    # kill a worker (DEPRECATED)
    def kill_worker(self, workspec):
        """Kill a worker in a scheduling system like batch systems and computing elements.

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        return True, ""

    # kill workers
    def kill_workers(self, workspec_list):
        """Kill workers in a scheduling system like batch systems and computing elements.

        :param workspec_list: a list of workspec instances
        :return: A list of tuples of return code (True for success, False otherwise) and error dialog
        :rtype: [(bool, string), ...]
        """
        retList = []
        for workspec in workspec_list:
            retList.append((True, ""))
        return retList

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
        return True, ""
