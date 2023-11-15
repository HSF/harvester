import os.path
from concurrent.futures import ProcessPoolExecutor as Pool

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec

# logger
baseLogger = core_utils.setup_logger("dummy_mcore_monitor")


# check a worker
def check_a_worker(workspec):
    # make logger
    tmpLog = core_utils.make_logger(baseLogger, f"workerID={workspec.workerID}", method_name="check_a_worker")
    dummyFilePath = os.path.join(workspec.get_access_point(), "status.txt")
    tmpLog.debug(f"look for {dummyFilePath}")
    newStatus = WorkSpec.ST_finished
    try:
        with open(dummyFilePath) as dummyFile:
            newStatus = dummyFile.readline()
            newStatus = newStatus.strip()
    except BaseException:
        pass
    tmpLog.debug(f"newStatus={newStatus}")
    return (newStatus, "")


# dummy monitor with multi-cores
class DummyMcoreMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check workers
    def check_workers(self, workspec_list):
        # make logger
        tmpLog = self.make_logger(baseLogger, method_name="check_workers")
        tmpLog.debug(f"start nWorkers={len(workspec_list)}")
        with Pool() as pool:
            retList = pool.map(check_a_worker, workspec_list)
        tmpLog.debug("done")
        return True, retList
