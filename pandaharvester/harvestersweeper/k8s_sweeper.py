from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestersweeper.base_sweeper import BaseSweeper
from pandaharvester.harvestermisc.k8s_utils import k8s_Client
from pandaharvester.harvestermisc.info_utils_k8s import PandaQueuesDictK8s

# logger
base_logger = core_utils.setup_logger("k8s_sweeper")


# sweeper for K8S
class K8sSweeper(BaseSweeper):
    # constructor
    def __init__(self, **kwarg):
        BaseSweeper.__init__(self, **kwarg)

        # retrieve the k8s namespace from CRIC
        self.panda_queues_dict = PandaQueuesDictK8s()
        namespace = self.panda_queues_dict.get_k8s_namespace(self.queueName)
        self.k8s_client = k8s_Client(namespace, queue_name=self.queueName, config_file=self.k8s_config_file)

    # kill workers
    def kill_workers(self, work_spec_list):
        tmp_log = self.make_logger(base_logger, method_name="kill_workers")

        ret_list = []
        for work_spec in work_spec_list:
            tmp_ret_val = (None, "Nothing done")

            batch_id = work_spec.batchID
            worker_id = str(work_spec.workerID)
            if batch_id:  # sometimes there are missed workers that were not submitted
                # if push mode, delete the configmap
                if work_spec.mapType != "NoJob":
                    try:
                        self.k8s_client.delete_config_map(worker_id)
                        tmp_log.debug("Deleted configmap {0}".format(worker_id))
                    except Exception as _e:
                        err_str = "Failed to delete a CONFIGMAP with id={0} ; {1}".format(worker_id, _e)
                        tmp_log.error(err_str)
                        tmp_ret_val = (False, err_str)
                else:
                    tmp_log.debug("No pandajob/configmap associated to worker {0}".format(work_spec.workerID))

                # delete the job
                try:
                    self.k8s_client.delete_job(batch_id)
                    tmp_log.debug("Deleted JOB {0}".format(batch_id))
                except Exception as _e:
                    err_str = "Failed to delete a JOB with id={0} ; {1}".format(batch_id, _e)
                    tmp_log.error(err_str)
                    tmp_ret_val = (False, err_str)

            else:  # the worker does not need be cleaned
                tmp_ret_val = (True, "")

            ret_list.append(tmp_ret_val)

        return ret_list

    def sweep_worker(self, work_spec):
        # cleanup for a worker
        tmp_log = self.make_logger(base_logger, "workerID={0}".format(work_spec.workerID), method_name="sweep_worker")

        # retrieve and upload the logs to panda cache
        # batch_id = work_spec.batchID
        # log_content = self.k8s_client.retrieve_pod_log(batch_id)

        # nothing to do
        return True, ""
