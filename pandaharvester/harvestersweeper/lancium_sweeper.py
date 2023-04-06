import socket

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestersweeper.base_sweeper import BaseSweeper
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestermisc.lancium_utils import LanciumClient

# logger
base_logger = core_utils.setup_logger('lancium_sweeper')


# sweeper for Lancium
class LanciumSweeper(BaseSweeper):
    # constructor
    def __init__(self, **kwarg):
        BaseSweeper.__init__(self, **kwarg)
        self.hostname = socket.getfqdn()
        self.lancium_client = LanciumClient(self.hostname, queue_name=self.queueName)

    # kill workers
    def kill_workers(self, work_spec_list):
        tmp_log = self.make_logger(base_logger, method_name='kill_workers')

        ret_list = []
        for work_spec in work_spec_list:
            batch_id = work_spec.batchID
            if batch_id:  # sometimes there are missed workers that were not submitted
                try:
                    self.lancium_client.delete_job(batch_id)
                    tmp_log.debug('Deleted job {0}'.format(batch_id))
                    tmp_ret_val = (True, '')
                except Exception as _e:
                    err_str = 'Failed to delete a job with id={0} ; {1}'.format(batch_id, _e)
                    tmp_log.error(err_str)
                    tmp_ret_val = (False, err_str)

            else:  # the worker does not need be cleaned
                tmp_ret_val = (True, '')

            ret_list.append(tmp_ret_val)

        return ret_list

    def sweep_worker(self, work_spec):
        # cleanup for a worker
        tmp_log = self.make_logger(base_logger, 'workerID={0}'.format(work_spec.workerID), method_name='sweep_worker')
        tmp_log.debug('Did nothing')

        return True, ''

