import os
import traceback

try:
    from urllib import unquote  # Python 2.X
except ImportError:
    from urllib.parse import unquote  # Python 3+

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.k8s_utils import k8s_Client
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc.info_utils_k8s import PandaQueuesDictK8s
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestersubmitter import submitter_common

# logger
base_logger = core_utils.setup_logger("k8s_submitter")

# submitter for K8S


class K8sSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)

        self.panda_queues_dict = PandaQueuesDictK8s()

        # retrieve the k8s namespace from CRIC
        namespace = self.panda_queues_dict.get_k8s_namespace(self.queueName)

        self.k8s_client = k8s_Client(namespace=namespace, queue_name=self.queueName, config_file=self.k8s_config_file)

        # update or create the pilot starter executable
        self.k8s_client.create_or_patch_configmap_starter()

        # allowed associated parameters from CRIC
        self._allowed_agis_attrs = ("pilot_url",)

        # number of processes
        try:
            self.nProcesses
        except AttributeError:
            self.nProcesses = 1
        else:
            if (not self.nProcesses) or (self.nProcesses < 1):
                self.nProcesses = 1

        # x509 proxy through k8s secrets: preferred way
        try:
            self.proxySecretPath
        except AttributeError:
            if os.getenv("PROXY_SECRET_PATH"):
                self.proxySecretPath = os.getenv("PROXY_SECRET_PATH")

        # analysis x509 proxy through k8s secrets: on GU queues
        try:
            self.proxySecretPathAnalysis
        except AttributeError:
            if os.getenv("PROXY_SECRET_PATH_ANAL"):
                self.proxySecretPath = os.getenv("PROXY_SECRET_PATH_ANAL")

    def _choose_proxy(self, workspec, is_grandly_unified_queue):
        """
        Choose the proxy based on the job type
        """
        cert = None
        job_type = workspec.jobType

        if is_grandly_unified_queue and job_type in ("user", "panda", "analysis"):
            if self.proxySecretPathAnalysis:
                cert = self.proxySecretPathAnalysis
            elif self.proxySecretPath:
                cert = self.proxySecretPath
        else:
            if self.proxySecretPath:
                cert = self.proxySecretPath

        return cert

    def submit_k8s_worker(self, work_spec):
        tmp_log = self.make_logger(base_logger, "queueName={0}".format(self.queueName), method_name="submit_k8s_worker")

        # get info from harvester queue config
        _queueConfigMapper = QueueConfigMapper()
        harvester_queue_config = _queueConfigMapper.get_queue(self.queueName)

        # set the stdout log file
        log_file_name = "{0}_{1}.out".format(harvester_config.master.harvester_id, work_spec.workerID)
        work_spec.set_log_file("stdout", "{0}/{1}".format(self.logBaseURL, log_file_name))
        # TODO: consider if we want to upload the yaml file to PanDA cache

        yaml_content = self.k8s_client.read_yaml_file(self.k8s_yaml_file)
        try:
            # choose the appropriate proxy
            this_panda_queue_dict = self.panda_queues_dict.get(self.queueName, dict())

            is_grandly_unified_queue = self.panda_queues_dict.is_grandly_unified_queue(self.queueName)
            cert = self._choose_proxy(work_spec, is_grandly_unified_queue)
            if not cert:
                err_str = "No proxy specified in proxySecretPath. Not submitted"
                tmp_return_value = (False, err_str)
                return tmp_return_value

            # get the walltime limit
            try:
                max_time = this_panda_queue_dict["maxtime"]
            except Exception as e:
                tmp_log.warning("Could not retrieve maxtime field for queue {0}".format(self.queueName))
                max_time = None

            associated_params_dict = {}
            for key, val in self.panda_queues_dict.get_harvester_params(self.queueName).items():
                if key in self._allowed_agis_attrs:
                    associated_params_dict[key] = val

            pilot_url = associated_params_dict.get("pilot_url")
            pilot_version = str(this_panda_queue_dict.get("pilot_version", "current"))
            python_version = str(this_panda_queue_dict.get("python_version", "2"))

            prod_source_label_tmp = harvester_queue_config.get_source_label(work_spec.jobType)
            pilot_opt_dict = submitter_common.get_complicated_pilot_options(work_spec.pilotType, pilot_url, pilot_version, prod_source_label_tmp)
            if pilot_opt_dict is None:
                prod_source_label = prod_source_label_tmp
                pilot_type = work_spec.pilotType
                pilot_url_str = "--piloturl {0}".format(pilot_url) if pilot_url else ""
            else:
                prod_source_label = pilot_opt_dict["prod_source_label"]
                pilot_type = pilot_opt_dict["pilot_type_opt"]
                pilot_url_str = pilot_opt_dict["pilot_url_str"]

            pilot_python_option = submitter_common.get_python_version_option(python_version, prod_source_label)
            host_image = self.panda_queues_dict.get_k8s_host_image(self.queueName)

            # submit the worker
            rsp, yaml_content_final = self.k8s_client.create_job_from_yaml(
                yaml_content, work_spec, prod_source_label, pilot_type, pilot_url_str, pilot_python_option, pilot_version, host_image, cert, max_time=max_time
            )
        except Exception as _e:
            tmp_log.error(traceback.format_exc())
            err_str = "Failed to create a JOB; {0}".format(_e)
            tmp_return_value = (False, err_str)
        else:
            work_spec.batchID = yaml_content["metadata"]["name"]
            tmp_log.debug("Created worker {0} with batchID={1}".format(work_spec.workerID, work_spec.batchID))
            tmp_return_value = (True, "")

        return tmp_return_value

    # submit workers
    def submit_workers(self, workspec_list):
        tmp_log = self.make_logger(base_logger, "queueName={0}".format(self.queueName), method_name="submit_workers")

        n_workers = len(workspec_list)
        tmp_log.debug("start, n_workers={0}".format(n_workers))

        ret_list = list()
        if not workspec_list:
            tmp_log.debug("empty workspec_list")
            return ret_list

        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            ret_val_list = thread_pool.map(self.submit_k8s_worker, workspec_list)
            tmp_log.debug("{0} workers submitted".format(n_workers))

        ret_list = list(ret_val_list)

        tmp_log.debug("done")

        return ret_list
