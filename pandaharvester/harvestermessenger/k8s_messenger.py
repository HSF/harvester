import os

from pandaharvester.harvestercore import core_utils
from .base_messenger import BaseMessenger
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc.k8s_utils import k8s_Client
# from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
# from pandaharvester.harvestercore.work_spec import WorkSpec


# logger
_logger = core_utils.setup_logger('k8s_messenger')


# Messenger for generic Kubernetes clusters
class K8sMessenger(BaseMessenger):

    def __init__(self, **kwargs):
        BaseMessenger.__init__(self, **kwargs)
        try:
            self.logDir
        except AttributeError:
            print('K8sMessenger: Missing attribute logDir')
            raise
        self.k8s_client = k8s_Client(namespace=self.k8s_namespace, config_file=self.k8s_config_file)
        self._all_pods_list = self.k8s_client.get_pods_info()

    def post_processing(self, workspec, jobspec_list, map_type):
        """
        Do the following in post_processing, i.e. when workers terminate (finished/failed/cancelled)
        - Fetch logs of the pod from k8s
        - Store or upload logs
        """
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID),
                                        method_name='post_processing')
        tmpLog.debug('start')
        try:
            # fetch and store logs
            job_id = workspec.batchID
            pods_list = self.k8s_client.filter_pods_info(self._all_pods_list, job_name=job_id)
            pod_name_list = [ pods_info['name'] for pods_info in pods_list ]
            outlog_filename = os.path.join(self.logDir, 'gridK8S.{0}.{1}.out'.format(workspec.workerID, workspec.batchID))
            with open(outlog_filename, 'w') as f:
                for pod_name in pod_name_list:
                    current_log_str = self.k8s_client.get_pod_logs(pod_name)
                    f.write(current_log_str)
            # upload logs
            pass
            # return
            tmpLog.debug('done')
            return True
        except Exception:
            core_utils.dump_error_message(tmpLog)
            return None
