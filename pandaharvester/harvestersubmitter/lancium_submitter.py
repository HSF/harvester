import traceback
import socket
import os

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestersubmitter import submitter_common
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict

from pandaharvester.harvestermisc.lancium_utils import LanciumClient, SCRIPTS_PATH, get_job_name_from_workspec

base_logger = core_utils.setup_logger("lancium_submitter")

voms_lancium_path = "/voms/voms"
script_lancium_path = "/scripts/pilots_starter.py"
mount_path = "input_files"
full_mount_path_secrets = "/jobDir/input_files/voms"


class LanciumSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.hostname = socket.getfqdn()

        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)

        # retrieve the configurations for the panda queues
        self.panda_queues_dict = PandaQueuesDict()

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

        self.lancium_client = LanciumClient(self.hostname, queue_name=self.queueName)

        # update or create the pilot starter executable
        self.upload_pilots_starter()

    def upload_pilots_starter(self):
        tmp_log = self.make_logger(base_logger, method_name="upload_pilots_starter")
        tmp_log.debug("Start")
        try:
            base_name = "pilots_starter.py"
            dir_name = os.path.dirname(__file__)

            local_file = os.path.join(dir_name, "../harvestercloud/{0}".format(base_name))
            lancium_file = os.path.join(SCRIPTS_PATH, base_name)
            self.lancium_client.upload_file(local_file, lancium_file)
            tmp_log.debug("Done")
        except Exception:
            tmp_log.error("Problem uploading proxy {0}. {1}".format(local_file, traceback.format_exc()))

    def _choose_proxy(self, workspec):
        """
        Choose the proxy based on the job type
        """
        cert = None
        job_type = workspec.jobType
        is_grandly_unified_queue = self.panda_queues_dict.is_grandly_unified_queue(self.queueName)

        if is_grandly_unified_queue and job_type in ("user", "panda", "analysis"):
            if self.user_proxy:
                cert = self.user_proxy
            elif self.prod_proxy:
                cert = self.prod_proxy
        else:
            if self.prod_proxy:
                cert = self.prod_proxy

        return cert

    def _fill_params(
        self,
        workspec,
        container_image,
        cert,
        physical_cores,
        memory_gb,
        maxwdir_prorated_gib,
        max_time,
        pilot_type,
        pilot_url_str,
        pilot_version,
        prod_source_label,
        pilot_python_option,
        log_file_name,
    ):
        lancium_job_name = get_job_name_from_workspec(workspec)

        # submit the worker
        params = {
            "name": lancium_job_name,
            "command_line": "python input_files/scripts/pilots_starter.py",
            "image": container_image,  # 'harvester/centos7-singularity'
            "max_run_time": max_time,
            "resources": {"core_count": physical_cores, "memory": memory_gb, "scratch": int(maxwdir_prorated_gib)},
            "input_files": [
                {"source_type": "data", "data": voms_lancium_path, "name": mount_path},
                {"source_type": "data", "data": script_lancium_path, "name": mount_path},
            ],
            "environment": (
                {"variable": "pilotUrlOpt", "value": pilot_url_str},  # pilotUrlOpt, stdout_name
                {"variable": "stdout_name", "value": log_file_name},
                {"variable": "PILOT_NOKILL", "value": "True"},
                {"variable": "computingSite", "value": self.queueName},
                {"variable": "pandaQueueName", "value": self.queueName},
                {"variable": "resourceType", "value": workspec.resourceType},
                {"variable": "prodSourceLabel", "value": prod_source_label},
                {"variable": "pilotType", "value": pilot_type},
                # {'variable': 'pythonOption', 'value': pilot_python_option},
                {"variable": "pilotVersion", "value": pilot_version},
                {"variable": "jobType", "value": prod_source_label},
                {"variable": "proxySecretPath", "value": os.path.join(full_mount_path_secrets, cert)},
                {"variable": "workerID", "value": str(workspec.workerID)},
                {"variable": "pilotProxyCheck", "value": "False"},
                {"variable": "logs_frontend_w", "value": harvester_config.pandacon.pandaCacheURL_W},
                {"variable": "logs_frontend_r", "value": harvester_config.pandacon.pandaCacheURL_R},
                {"variable": "PANDA_JSID", "value": "harvester-" + harvester_config.master.harvester_id},
                {"variable": "HARVESTER_WORKER_ID", "value": str(workspec.workerID)},
                {"variable": "HARVESTER_ID", "value": harvester_config.master.harvester_id},
                {"variable": "submit_mode", "value": "PULL"},
                {"variable": "TMPDIR", "value": "/jobDir"},
                {"variable": "HOME", "value": "/jobDir"},
                # {'variable': 'K8S_JOB_ID', 'value': lancium_job_name},
            ),
        }
        return params

    def submit_lancium_worker(self, workspec):
        tmp_log = self.make_logger(base_logger, "queueName={0}".format(self.queueName), method_name="submit_lancium_worker")

        this_panda_queue_dict = self.panda_queues_dict.get(self.queueName, dict())

        try:
            # get info from harvester queue config
            _queueConfigMapper = QueueConfigMapper()
            harvester_queue_config = _queueConfigMapper.get_queue(self.queueName)

            # set the stdout log file
            log_file_name = "{0}_{1}.out".format(harvester_config.master.harvester_id, workspec.workerID)
            workspec.set_log_file("stdout", "{0}/{1}".format(self.logBaseURL, log_file_name))

            # choose the appropriate proxy
            cert = self._choose_proxy(workspec)
            if not cert:
                err_str = "No proxy specified in proxySecretPath. Not submitted"
                tmp_return_value = (False, err_str)
                return tmp_return_value

            # set the container image
            container_image = "harvester/centos7-singularity"  # harvester_queue_config.container_image
            physical_cores = workspec.nCore / 2  # lancium uses hyperthreading, but expects job size in physical cores
            memory_gb = workspec.minRamCount / 2 / 1000
            maxwdir_prorated_gib = self.panda_queues_dict.get_prorated_maxwdir_GiB(workspec.computingSite, workspec.nCore)
            max_time = this_panda_queue_dict.get("maxtime", None)

            associated_params_dict = {}
            for key, val in self.panda_queues_dict.get_harvester_params(self.queueName).items():
                if key in self._allowed_agis_attrs:
                    associated_params_dict[key] = val

            pilot_url = associated_params_dict.get("pilot_url")
            pilot_version = str(this_panda_queue_dict.get("pilot_version", "current"))
            python_version = str(this_panda_queue_dict.get("python_version", "3"))

            prod_source_label_tmp = harvester_queue_config.get_source_label(workspec.jobType)
            pilot_opt_dict = submitter_common.get_complicated_pilot_options(workspec.pilotType, pilot_url, pilot_version, prod_source_label_tmp)
            if pilot_opt_dict is None:
                prod_source_label = prod_source_label_tmp
                pilot_type = workspec.pilotType
                pilot_url_str = "--piloturl {0}".format(pilot_url) if pilot_url else ""
            else:
                prod_source_label = pilot_opt_dict["prod_source_label"]
                pilot_type = pilot_opt_dict["pilot_type_opt"]
                pilot_url_str = pilot_opt_dict["pilot_url_str"]

            pilot_python_option = submitter_common.get_python_version_option(python_version, prod_source_label)

            params = self._fill_params(
                workspec,
                container_image,
                cert,
                physical_cores,
                memory_gb,
                maxwdir_prorated_gib,
                max_time,
                pilot_type,
                pilot_url_str,
                pilot_version,
                prod_source_label,
                pilot_python_option,
                log_file_name,
            )

            return_code, return_str = self.lancium_client.submit_job(**params)
            if not return_code:
                return return_code, return_str

        except Exception as _e:
            tmp_log.error(traceback.format_exc())
            err_str = "Failed to create a worker; {0}".format(_e)
            tmp_return_value = (False, err_str)
        else:
            workspec.batchID = return_str
            tmp_log.debug("Created worker {0} with batchID={1}".format(workspec.workerID, workspec.batchID))
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
            ret_val_list = thread_pool.map(self.submit_lancium_worker, workspec_list)
            tmp_log.debug("{0} workers submitted".format(n_workers))

        ret_list = list(ret_val_list)

        tmp_log.debug("done")

        return ret_list
