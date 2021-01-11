from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestersweeper.base_sweeper import BaseSweeper
from pandaharvester.harvestermisc.k8s_utils import k8s_Client

# logger
base_logger = core_utils.setup_logger('horovod_sweeper')


# sweeper for K8S
# sweeper for K8S
class HorovodSweeper(BaseSweeper):
    # constructor
    def __init__(self, **kwarg):
        BaseSweeper.__init__(self, **kwarg)

        self.k8s_client = k8s_Client(self.k8s_namespace, config_file=self.k8s_config_file)

        self._all_pods_list = []

    # kill workers
    def kill_workers(self, work_spec_list):
        ret_list = []
        for worker_spec in work_spec_list:
            tmp_ret_val = self.kill_worker(worker_spec)

        return ret_list

    def kill_worker(self, worker_spec):
        # cleanup for a worker
        tmp_log = self.make_logger(base_logger, 'workerID={0}'.format(worker_spec.workerID), method_name='sweep_worker')

        # retrieve and upload the logs to panda cache
        # batch_id = worker_spec.batchID
        # log_content = self.k8s_client.retrieve_pod_log(batch_id)

        tmp_log.debug('Going to sweep formation')
        tmp_ret_val = self.k8s_client.delete_horovod_formation(worker_spec)
        tmp_log.debug('Swept formation')

        return tmp_ret_val

    def sweep_worker(self, worker_spec):
        return self.kill_worker(worker_spec)