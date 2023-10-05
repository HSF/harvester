import os

from pandaharvester.harvestercore import core_utils
from .base_messenger import BaseMessenger
from pandaharvester.harvestermisc.k8s_utils import k8s_Client
from pandaharvester.harvestermisc.info_utils_k8s import PandaQueuesDictK8s

# logger
_logger = core_utils.setup_logger("k8s_messenger")


# Messenger for generic Kubernetes clusters
class K8sMessenger(BaseMessenger):
    def __init__(self, **kwargs):
        BaseMessenger.__init__(self, **kwargs)
        try:
            self.logDir
        except AttributeError:
            print("K8sMessenger: Missing attribute logDir")
            raise

        # retrieve the k8s namespace from CRIC
        self.panda_queues_dict = PandaQueuesDictK8s()
        namespace = self.panda_queues_dict.get_k8s_namespace(self.queueName)

        self.k8s_client = k8s_Client(namespace=namespace, queue_name=self.queueName, config_file=self.k8s_config_file)
        self._all_pods_list = self.k8s_client.get_pods_info()

    def post_processing(self, workspec, jobspec_list, map_type):
        """
        Do the following in post_processing, i.e. when workers terminate (finished/failed/cancelled)
        - Fetch logs of the pod from k8s
        - Store or upload logs
        """
        # get logger
        tmp_log = core_utils.make_logger(_logger, "queueName={0} workerID={1}".format(self.queueName, workspec.workerID), method_name="post_processing")
        tmp_log.debug("start")

        if self._all_pods_list is None:
            tmp_log.error("No pod information")
            tmp_log.debug("done")
            return None

        try:
            # fetch and store logs
            job_id = workspec.batchID
            pods_list = self.k8s_client.filter_pods_info(self._all_pods_list, job_name=job_id)
            pod_name_list = [pods_info["name"] for pods_info in pods_list]
            outlog_filename = os.path.join(self.logDir, "gridK8S.{0}.{1}.out".format(workspec.workerID, workspec.batchID))
            with open(outlog_filename, "w") as f:
                for pod_name in pod_name_list:
                    current_log_str = self.k8s_client.get_pod_logs(pod_name)
                    f.write(current_log_str)
            # upload logs
            pass
            # return
            tmp_log.debug("done")
            return True
        except Exception:
            core_utils.dump_error_message(tmp_log)
            return None
