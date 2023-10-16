import socket

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestersweeper.base_sweeper import BaseSweeper
from pandaharvester.harvestermisc.lancium_utils import LanciumClient

# logger
base_logger = core_utils.setup_logger("lancium_sweeper")


# sweeper for Lancium
class LanciumSweeper(BaseSweeper):
    # constructor
    def __init__(self, **kwarg):
        BaseSweeper.__init__(self, **kwarg)
        self.hostname = socket.getfqdn()
        self.lancium_client = LanciumClient(self.hostname, queue_name=self.queueName)

    # kill workers
    def kill_workers(self, workspec_list):
        tmp_log = self.make_logger(base_logger, method_name="kill_workers")
        tmp_log.debug("Start")
        ret_list = []
        for workspec in workspec_list:
            tmp_log.debug("Running kill_worker for {0}".format(workspec.workerID))
            tmp_ret_val = self.kill_worker(workspec)
            ret_list.append(tmp_ret_val)
        tmp_log.debug("Done")
        return ret_list

    def kill_worker(self, workspec):
        tmp_log = self.make_logger(base_logger, "workerID={0}".format(workspec.workerID), method_name="kill_worker")
        batch_id = workspec.batchID
        tmp_log.debug("Running kill_worker")
        if batch_id:  # sometimes there are missed workers that were not submitted
            try:
                self.lancium_client.delete_job(batch_id)
                tmp_log.debug("Deleted job {0}".format(batch_id))
                return True, ""
            except Exception as _e:
                err_str = "Failed to delete a job with id={0} ; {1}".format(batch_id, _e)
                tmp_log.error(err_str)
                return False, err_str

        else:  # the worker does not need be cleaned
            tmp_log.debug("No action necessary, since no batch ID")
            return True, ""

    def sweep_worker(self, workspec):
        # cleanup for a worker
        tmp_log = self.make_logger(base_logger, "workerID={0}".format(workspec.workerID), method_name="sweep_worker")
        tmp_log.debug("Returning kill_worker")
        return self.kill_worker(workspec)
