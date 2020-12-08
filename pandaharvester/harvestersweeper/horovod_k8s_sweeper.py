from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestersweeper.base_sweeper import BaseSweeper
from pandaharvester.harvestermisc.k8s_utils import k8s_Client

# logger
base_logger = core_utils.setup_logger('horovod_k8s_sweeper')


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
        for work_spec in work_spec_list:
            tmp_ret_val = self.sweep_worker(work_spec)

        return ret_list

    def sweep_worker(self, work_spec):
        # cleanup for a worker
        tmp_log = self.make_logger(base_logger, 'workerID={0}'.format(work_spec.workerID), method_name='sweep_worker')

        # retrieve and upload the logs to panda cache
        # batch_id = work_spec.batchID
        # log_content = self.k8s_client.retrieve_pod_log(batch_id)

        tmp_log.debug('Going to sweep deployment for worker_id: {0}'.format(work_spec.workerID))
        tmp_ret_val = self.k8s_client.delete_horovod_deployment(work_spec)
        tmp_log.debug('Swept deployment for worker_id: {0}'.format(work_spec.workerID, tmp_ret_val))

        return tmp_ret_val
