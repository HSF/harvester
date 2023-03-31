from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestersweeper.base_sweeper import BaseSweeper
from pandaharvester.harvestermisc.info_utils_k8s import PandaQueuesDict

# logger
base_logger = core_utils.setup_logger('lancium_sweeper')


# sweeper for Lancium
class LanciumSweeper(BaseSweeper):
    # constructor
    def __init__(self, **kwarg):
        BaseSweeper.__init__(self, **kwarg)
        self.panda_queues_dict = PandaQueuesDict()

    # kill workers
    def kill_workers(self, work_spec_list):
        tmp_log = self.make_logger(base_logger, method_name='kill_workers')

        ret_list = []
        for work_spec in work_spec_list:
            tmp_ret_val = (None, 'Nothing done')
            ret_list.append(tmp_ret_val)

        return ret_list

    def sweep_worker(self, work_spec):
        # cleanup for a worker
        tmp_log = self.make_logger(base_logger, 'workerID={0}'.format(work_spec.workerID), method_name='sweep_worker')

        return True, ''
