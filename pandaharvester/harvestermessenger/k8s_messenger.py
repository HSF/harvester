

from pandaharvester.harvestercore import core_utils
from .base_messenger import BaseMessenger
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc.k8s_utils import k8s_Client
# from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
# from pandaharvester.harvestercore.work_spec import WorkSpec


# Messenger for generic Kubernetes clusters
class K8sMessenger(BaseMessenger):

    def __init__(self, **kwargs):
        BaseMessenger.__init__(self, **kwarg)
        try:
            self.logDir
        except AttributeError:
            print('K8sMessenger: Missing attribute logDir')
            raise
        self.k8s_client = k8s_Client(namespace=self.k8s_namespace, config_file=self.k8s_config_file)
        self._all_pods_list = self.k8s_client.get_pods_info()

    def post_processing(self, workspec, jobspec_list, map_type):
        """
        Do the folloiwing in post_processing, i.e. when workers terminate (finished/failed/cancelled)
        - Fetch logs of the pod from k8s
        - Store or upload logs
        """
        # fetch and store logs
        job_id = workspec.batchID
        pods_list = self.k8s_client.filter_pods_info(self._all_pods_list, job_name=job_id)
        pod_name_list = [ pods_info['name'] for pods_info in pods_list ]
        outlog_filename = os.path(self.logDir, 'gridK8S.{0}.{1}.out'.format(workspec.workerID, workspec.batchID))
        with open(outlog_filename, 'w') as f:
            for pod_name in pod_name_list:
                current_log_str = self.k8s_client.get_pod_logs(pod_name)
                previous_log_str = self.k8s_client.get_pod_logs(pod_name, previous=True)
                f.write(previous_log_str)
                f.write('\n\n')
                f.write(current_log_str)
                f.write('\n\n\n')
        # upload logs
        pass
