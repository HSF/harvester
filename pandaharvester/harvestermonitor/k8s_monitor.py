import datetime
import traceback
from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.worker_errors import WorkerErrors
from pandaharvester.harvestermisc.info_utils_k8s import PandaQueuesDictK8s
from pandaharvester.harvestermisc.k8s_utils import k8s_Client

from pandaharvester.harvestermonitor.monitor_common import get_payload_errstr_from_ec

base_logger = core_utils.setup_logger("k8s_monitor")

BAD_CONTAINER_STATES = ["CreateContainerError", "CrashLoopBackOff", "FailedMount"]

# monitor for K8S


class K8sMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        self.panda_queues_dict = PandaQueuesDictK8s()

        # retrieve the k8s namespace from CRIC
        namespace = self.panda_queues_dict.get_k8s_namespace(self.queueName)

        self.k8s_client = k8s_Client(namespace=namespace, queue_name=self.queueName, config_file=self.k8s_config_file)

        try:
            self.nProcesses
        except AttributeError:
            self.nProcesses = 4
        try:
            self.cancelUnknown
        except AttributeError:
            self.cancelUnknown = False
        else:
            self.cancelUnknown = bool(self.cancelUnknown)
        try:
            self.podQueueTimeLimit
        except AttributeError:
            self.podQueueTimeLimit = 172800
        try:
            self.payloadType
        except AttributeError:
            self.payloadType = None

        self._all_workers_dict = []

    def check_pods_status(self, pods_status_list, pods_status_message_list, containers_state_list, containers_exit_code_list):
        sub_msg = ""
        exit_code = 0

        if "Unknown" in pods_status_list:
            if all(item == "Unknown" for item in pods_status_list):
                new_status = None
            elif "Running" in pods_status_list:
                new_status = WorkSpec.ST_running
            else:
                new_status = WorkSpec.ST_idle

        else:
            # Pod in Pending status
            if all(item == "Pending" for item in pods_status_list):
                new_status = WorkSpec.ST_submitted  # default is submitted, but consider certain cases
                for item in containers_state_list:
                    if item.waiting and item.waiting.reason in BAD_CONTAINER_STATES:
                        new_status = WorkSpec.ST_failed  # change state to failed

            # Pod in Succeeded status
            elif "Succeeded" in pods_status_list:
                if all((item.terminated is not None and item.terminated.reason == "Completed") for item in containers_state_list):
                    new_status = WorkSpec.ST_finished
                else:
                    sub_message_list = []
                    for item in containers_state_list:
                        msg_str = ""
                        if item.terminated is None:
                            state = "UNKNOWN"
                            if item.running is not None:
                                state = "running"
                            elif item.waiting is not None:
                                state = "waiting"
                            msg_str = f"container not terminated yet ({state}) while pod Succeeded"
                        elif item.terminated.reason != "Completed":
                            msg_str = f"container terminated by k8s for reason {item.terminated.reason}"
                        sub_message_list.append(msg_str)
                    sub_msg = ";".join(sub_message_list)
                    new_status = WorkSpec.ST_cancelled

            # Pod in Running status
            elif "Running" in pods_status_list:
                new_status = WorkSpec.ST_running

            # Pod in Failed status
            elif "Failed" in pods_status_list:
                new_status = WorkSpec.ST_failed

                for tmp_code in containers_exit_code_list:
                    if tmp_code != 0:
                        exit_code = tmp_code

                try:
                    sub_msg = ";".join(pods_status_message_list)
                except BaseException:
                    sub_msg = ""
            else:
                new_status = WorkSpec.ST_idle

        return new_status, exit_code, sub_msg

    def check_job_status(self, job_status, job_status_reason, job_status_message, n_pods_succeeded, n_pods_failed):
        new_status = None
        sub_msg = ""

        if n_pods_succeeded or job_status == "Complete":
            new_status = WorkSpec.ST_finished
            sub_msg = ""
        elif n_pods_failed or job_status == "Failed":
            new_status = WorkSpec.ST_failed
            sub_msg = job_status_message + job_status_reason
        # in principle the job information should only apply to final states, but consider other states in the future
        return new_status, sub_msg

    def check_a_worker(self, workspec):
        # set logger
        tmp_log = self.make_logger(
            base_logger, f"queueName={self.queueName} workerID={workspec.workerID} batchID={workspec.batchID}", method_name="check_a_worker"
        )

        # initialization
        job_id = workspec.batchID
        error_message = ""
        time_now = datetime.datetime.now(datetime.timezone.utc)
        pods_status_list = []
        pods_status_message_list = []
        pods_name_to_delete_list = []
        job_status = ""
        job_status_reason = ""
        job_status_message = ""
        n_pods_succeeded = 0
        n_pods_failed = 0
        try:
            containers_state_list = []
            containers_exit_code_list = []
            pods_sup_diag_list = []
            if job_id in self._all_workers_dict:
                worker_info = self._all_workers_dict[job_id]

                # make list of status of the pods belonging to our job
                if "pod_status" in worker_info and "containers_state" in worker_info and "pod_name" in worker_info:
                    pods_status_list.append(worker_info["pod_status"])
                    pods_status_message_list.append(worker_info["pod_status_message"])
                    containers_state_list.extend(worker_info["containers_state"])
                    containers_exit_code_list.extend(worker_info["containers_exit_code"])
                    pods_sup_diag_list.append(worker_info["pod_name"])

                # get backup info about the job
                if "job_status" in worker_info and "job_status_reason" in worker_info and "job_status_message" in worker_info:
                    job_status = worker_info["job_status"]
                    job_status_reason = worker_info["job_status_reason"]
                    job_status_message = worker_info["job_status_message"]
                    n_pods_succeeded = worker_info["n_pods_succeeded"]
                    n_pods_failed = worker_info["n_pods_failed"]

                # make a list of pods that should be removed
                # 1. pods being queued too long
                if (
                    "pod_status" in worker_info
                    and worker_info["pod_status"] in ["Pending", "Unknown"]
                    and worker_info["pod_start_time"]
                    and time_now - worker_info["pod_start_time"] > datetime.timedelta(seconds=self.podQueueTimeLimit)
                ):
                    pods_name_to_delete_list.append(worker_info["pod_name"])
                # 2. pods with containers in bad states
                if "pod_status" in worker_info and worker_info["pod_status"] in ["Pending", "Unknown"]:
                    for item in worker_info["containers_state"]:
                        if item.waiting and item.waiting.reason in BAD_CONTAINER_STATES:
                            pods_name_to_delete_list.append(worker_info["pod_name"])

        except Exception as _e:
            error_message = f"Failed to get status for id={job_id} ; {traceback.format_exc()}"
            tmp_log.error(error_message)
            new_status = None
        else:
            # we didn't find neither the pod nor the job for the worker
            if not pods_status_list and not job_status:
                # there were no pods found belonging to our job
                error_message = f"JOB id={job_id} not found"
                tmp_log.error(error_message)
                tmp_log.info("Force to cancel the worker due to JOB not found")
                new_status = WorkSpec.ST_cancelled
            # we found a pod for the worker, it has precedence over the job information
            elif pods_status_list:
                # we found pods belonging to our job. Obtain the final status
                tmp_log.debug(f"pods_status_list={pods_status_list}")
                new_status, exit_code, sub_msg = self.check_pods_status(pods_status_list, pods_status_message_list, containers_state_list, containers_exit_code_list)
                if sub_msg:
                    error_message += sub_msg
                tmp_log.debug(f"new_status={new_status}")
            # we didn't find the pod, but there was still a job for the worker
            else:
                new_status, sub_msg = self.check_job_status(job_status, job_status_reason, job_status_message, n_pods_succeeded, n_pods_failed)
                if sub_msg:
                    error_message += sub_msg
                tmp_log.debug(f"new_status={new_status}")

            # delete pods that have been queueing too long
            if pods_name_to_delete_list:
                tmp_log.debug("Deleting pods queuing too long")
                ret_list = self.k8s_client.delete_pods(pods_name_to_delete_list)
                deleted_pods_list = []
                for item in ret_list:
                    if item["errMsg"] == "":
                        deleted_pods_list.append(item["name"])
                tmp_log.debug(f"Deleted pods queuing too long: {','.join(deleted_pods_list)}")

            # exit code and diag message
            if exit_code != 0:
                sup_error_code = exit_code
                sup_error_diag = get_payload_errstr_from_ec(self.payloadType, exit_code)
            else:
                sup_error_code = WorkerErrors.error_codes.get("GENERAL_ERROR") if error_message else WorkerErrors.error_codes.get("SUCCEEDED")

                # supplemental diag messages
                if pods_sup_diag_list:
                    pods_sup_str = ",".join(pods_sup_diag_list)
                    sup_error_diag = f"PODs={pods_sup_str} ; {error_message}"
                else:
                    sup_error_diag = error_message

            workspec.set_supplemental_error(error_code=sup_error_code, error_diag=sup_error_diag)

        return new_status, error_message

    def check_workers(self, workspec_list):
        tmp_log = self.make_logger(base_logger, f"queueName={self.queueName}", method_name="check_workers")
        tmp_log.debug("start")

        ret_list = []
        if not workspec_list:
            error_message = "empty workspec_list"
            tmp_log.debug(error_message)
            ret_list.append(("", error_message))
            return False, ret_list

        workers_info = self.k8s_client.get_workers_info(workspec_list=workspec_list)
        if workers_info is None:  # there was a communication issue to the K8S cluster
            tmp_log.debug("done without answer")
            return False, ret_list

        self._all_workers_dict = workers_info

        # resolve status requested workers
        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            ret_iterator = thread_pool.map(self.check_a_worker, workspec_list)

        ret_list = list(ret_iterator)

        tmp_log.debug("done")
        return True, ret_list
