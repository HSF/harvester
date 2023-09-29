"""
utilities routines associated with Kubernetes python client

"""
import os
import copy
import base64
import yaml

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc.info_utils_k8s import PandaQueuesDictK8s
from pandaharvester.harvestercore import core_utils

base_logger = core_utils.setup_logger("k8s_utils")

CONFIG_DIR = "/scratch/jobconfig"
EXEC_DIR = "/scratch/executables"
GiB_TO_GB = 2**30 / 10.0**9

# command and image defaults
DEF_COMMAND = ["/usr/bin/bash"]
DEF_ARGS = ["-c", "cd; python $EXEC_DIR/pilots_starter.py || true"]
DEF_IMAGE = "atlasadc/atlas-grid-centos7"


class k8s_Client(object):
    def __init__(self, namespace, config_file=None, queue_name=None):
        if not os.path.isfile(config_file):
            raise RuntimeError("Cannot find k8s config file: {0}".format(config_file))
        config.load_kube_config(config_file=config_file)
        self.corev1 = client.CoreV1Api()
        self.batchv1 = client.BatchV1Api()
        self.deletev1 = client.V1DeleteOptions(propagation_policy="Background")

        self.panda_queues_dict = PandaQueuesDictK8s()
        self.namespace = namespace
        self.queue_name = queue_name

    def read_yaml_file(self, yaml_file):
        with open(yaml_file) as f:
            yaml_content = yaml.load(f, Loader=yaml.FullLoader)

        return yaml_content

    def create_job_from_yaml(
        self, yaml_content, work_spec, prod_source_label, pilot_type, pilot_url_str, pilot_python_option, pilot_version, host_image, cert, max_time=None
    ):
        tmp_log = core_utils.make_logger(base_logger, "queue_name={0}".format(self.queue_name), method_name="create_job_from_yaml")

        # consider PULL mode as default, unless specified
        submit_mode = "PULL"

        # create the configmap in push mode
        worker_id = None
        if work_spec.mapType != "NoJob":
            submit_mode = "PUSH"
            worker_id = str(work_spec.workerID)
            res = self.create_configmap(work_spec)
            if not res:  # if the configmap creation failed, don't submit a job because the pod creation will hang
                return res, "Failed to create a configmap"

        # retrieve panda queue information
        queue_name = self.panda_queues_dict.get_panda_queue_name(work_spec.computingSite)

        # set the worker name
        worker_name = yaml_content["metadata"]["name"] + "-" + str(work_spec.workerID)  # this will be the batch id later on
        yaml_content["metadata"]["name"] = worker_name

        # set the resource type and other metadata to filter the pods
        yaml_content["spec"]["template"].setdefault("metadata", {})
        yaml_content["spec"]["template"]["metadata"].update(
            {"labels": {"resourceType": str(work_spec.resourceType), "prodSourceLabel": str(prod_source_label), "pq": str(work_spec.computingSite)}}
        )

        # this flag should be respected by the k8s autoscaler not relocate (kill) the job during a scale down
        safe_to_evict = self.panda_queues_dict.get_k8s_annotations(work_spec.computingSite)
        if safe_to_evict is False:
            yaml_content["spec"]["template"]["metadata"].update({"annotations": {"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"}})

        # fill the container details. we can only handle one container (take the first, delete the rest)
        yaml_containers = yaml_content["spec"]["template"]["spec"]["containers"]
        del yaml_containers[1 : len(yaml_containers)]

        container_env = yaml_containers[0]

        container_env.setdefault("resources", {})
        # set the container image
        if host_image:  # images defined in CRIC have absolute preference
            container_env["image"] = host_image
        elif "image" not in container_env:  # take default image only if not defined in yaml template
            container_env["image"] = DEF_IMAGE

        if "command" not in container_env:
            container_env["command"] = DEF_COMMAND
            container_env["args"] = DEF_ARGS

        # set the resources (CPU and memory) we need for the container
        # note that predefined values in the yaml template will NOT be overwritten
        # Be familiar with QoS classes: https://kubernetes.io/docs/tasks/configure-pod-container/quality-service-pod
        # The CPU & memory settings will affect the QoS for the pod
        container_env.setdefault("resources", {})
        resource_settings = self.panda_queues_dict.get_k8s_resource_settings(work_spec.computingSite)
        pilot_dir = self.panda_queues_dict.get_k8s_pilot_dir(work_spec.computingSite)

        # CPU resources
        cpu_scheduling_ratio = resource_settings["cpu_scheduling_ratio"]
        if work_spec.nCore > 0:
            # CPU requests
            container_env["resources"].setdefault("requests", {})
            if "cpu" not in container_env["resources"]["requests"]:
                container_env["resources"]["requests"]["cpu"] = str(work_spec.nCore * cpu_scheduling_ratio / 100.0)
            # CPU limits
            container_env["resources"].setdefault("limits", {})
            if "cpu" not in container_env["resources"]["limits"]:
                container_env["resources"]["limits"]["cpu"] = str(work_spec.nCore)

        # Memory resources
        use_memory_limit = resource_settings["use_memory_limit"]
        memory_limit_safety_factor = resource_settings["memory_limit_safety_factor"]
        memory_limit_min_offset = resource_settings["memory_limit_min_offset"]
        memory_scheduling_ratio = resource_settings["memory_scheduling_ratio"]

        if work_spec.minRamCount > 4:  # K8S minimum memory limit = 4 MB
            # memory requests
            container_env["resources"].setdefault("requests", {})
            if "memory" not in container_env["resources"]["requests"]:
                memory_request = str(work_spec.minRamCount * memory_scheduling_ratio / 100.0)
                container_env["resources"]["requests"]["memory"] = str(memory_request) + "Mi"
            # memory limits: kubernetes is very aggressive killing jobs due to memory, hence making this field optional
            # and adding configuration possibilities to add a safety factor
            if use_memory_limit:
                container_env["resources"].setdefault("limits", {})
                if "memory" not in container_env["resources"]["limits"]:
                    mem_limit = max(work_spec.minRamCount + memory_limit_min_offset, work_spec.minRamCount * memory_limit_safety_factor / 100.0)
                    container_env["resources"]["limits"]["memory"] = str(mem_limit) + "Mi"

        # Ephemeral storage resources
        use_ephemeral_storage = resource_settings["use_ephemeral_storage"]
        ephemeral_storage_offset_GiB = resource_settings["ephemeral_storage_offset"] / 1024
        ephemeral_storage_limit_safety_factor = resource_settings["ephemeral_storage_limit_safety_factor"]

        if use_ephemeral_storage:
            maxwdir_prorated_GiB = self.panda_queues_dict.get_prorated_maxwdir_GiB(work_spec.computingSite, work_spec.nCore)
            # ephemeral storage requests
            container_env["resources"].setdefault("requests", {})
            if "ephemeral-storage" not in container_env["resources"]["requests"]:
                eph_storage_request_GiB = maxwdir_prorated_GiB + ephemeral_storage_offset_GiB
                eph_storage_request_MiB = round(eph_storage_request_GiB * 1024, 2)
                container_env["resources"]["requests"]["ephemeral-storage"] = str(eph_storage_request_MiB) + "Mi"
            # ephemeral storage limits
            container_env["resources"].setdefault("limits", {})
            if "ephemeral-storage" not in container_env["resources"]["limits"]:
                eph_storage_limit_GiB = (maxwdir_prorated_GiB + ephemeral_storage_offset_GiB) * ephemeral_storage_limit_safety_factor / 100.0
                eph_storage_limit_MiB = round(eph_storage_limit_GiB * 1024, 2)
                container_env["resources"]["limits"]["ephemeral-storage"] = str(eph_storage_limit_MiB) + "Mi"

            # add the ephemeral storage and mount it on pilot_dir
            yaml_content["spec"]["template"]["spec"].setdefault("volumes", [])
            yaml_volumes = yaml_content["spec"]["template"]["spec"]["volumes"]
            exists = list(filter(lambda vol: vol["name"] == "pilot-dir", yaml_volumes))
            if not exists:
                yaml_volumes.append({"name": "pilot-dir", "emptyDir": {}})

            container_env.setdefault("volumeMounts", [])
            exists = list(filter(lambda vol_mount: vol_mount["name"] == "pilot-dir", container_env["volumeMounts"]))
            if not exists:
                container_env["volumeMounts"].append({"name": "pilot-dir", "mountPath": pilot_dir})

        container_env.setdefault("env", [])
        # try to retrieve the stdout log file name
        try:
            log_file_name = work_spec.workAttributes["stdout"]
        except (KeyError, AttributeError):
            tmp_log.debug("work_spec does not have stdout workAttribute, using default")
            log_file_name = ""

        # get the option to activate the pilot proxy check
        pilot_proxy_check = self.panda_queues_dict.get_k8s_pilot_proxy_check(work_spec.computingSite)

        container_env["env"].extend(
            [
                {"name": "computingSite", "value": work_spec.computingSite},
                {"name": "pandaQueueName", "value": queue_name},
                {"name": "resourceType", "value": work_spec.resourceType},
                {"name": "prodSourceLabel", "value": prod_source_label},
                {"name": "pilotType", "value": pilot_type},
                {"name": "pilotUrlOpt", "value": pilot_url_str},
                {"name": "pythonOption", "value": pilot_python_option},
                {"name": "pilotVersion", "value": pilot_version},
                {"name": "jobType", "value": work_spec.jobType},
                {"name": "proxySecretPath", "value": cert},
                {"name": "workerID", "value": str(work_spec.workerID)},
                {"name": "pilotProxyCheck", "value": str(pilot_proxy_check)},
                {"name": "logs_frontend_w", "value": harvester_config.pandacon.pandaCacheURL_W},
                {"name": "logs_frontend_r", "value": harvester_config.pandacon.pandaCacheURL_R},
                {"name": "stdout_name", "value": log_file_name},
                {"name": "PANDA_JSID", "value": "harvester-" + harvester_config.master.harvester_id},
                {"name": "HARVESTER_WORKER_ID", "value": str(work_spec.workerID)},
                {"name": "HARVESTER_ID", "value": harvester_config.master.harvester_id},
                {"name": "submit_mode", "value": submit_mode},
                {"name": "EXEC_DIR", "value": EXEC_DIR},
                {"name": "TMPDIR", "value": pilot_dir},
                {"name": "HOME", "value": pilot_dir},
                {"name": "PANDA_HOSTNAME", "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "spec.nodeName"}}},
                {"name": "K8S_JOB_ID", "value": worker_name},
            ]
        )

        # add the pilots starter configmap
        yaml_content["spec"]["template"]["spec"].setdefault("volumes", [])
        yaml_volumes = yaml_content["spec"]["template"]["spec"]["volumes"]
        yaml_volumes.append({"name": "pilots-starter", "configMap": {"name": "pilots-starter"}})
        # mount the volume to the filesystem
        container_env.setdefault("volumeMounts", [])
        container_env["volumeMounts"].append({"name": "pilots-starter", "mountPath": EXEC_DIR})

        # in push mode, add the configmap as a volume to the pod
        if submit_mode == "PUSH" and worker_id:
            yaml_content["spec"]["template"]["spec"].setdefault("volumes", [])
            yaml_volumes = yaml_content["spec"]["template"]["spec"]["volumes"]
            yaml_volumes.append({"name": "job-config", "configMap": {"name": worker_id}})
            # mount the volume to the filesystem
            container_env.setdefault("volumeMounts", [])
            container_env["volumeMounts"].append({"name": "job-config", "mountPath": CONFIG_DIR})

        # set the affinity
        scheduling_settings = self.panda_queues_dict.get_k8s_scheduler_settings(work_spec.computingSite)

        use_affinity = scheduling_settings["use_affinity"]
        use_anti_affinity = scheduling_settings["use_anti_affinity"]
        if (use_affinity or use_anti_affinity) and "affinity" not in yaml_content["spec"]["template"]["spec"]:
            yaml_content = self.set_affinity(yaml_content, use_affinity, use_anti_affinity)

        # set the priority classes. Specific priority classes have precedence over general priority classes
        priority_class = None

        priority_class_key = "priority_class_{0}".format(work_spec.resourceType.lower())
        priority_class_specific = scheduling_settings.get(priority_class_key, None)

        priority_class_key = "priority_class"
        priority_class_general = scheduling_settings.get(priority_class_key, None)

        if priority_class_specific:
            priority_class = priority_class_specific
        elif priority_class_general:
            priority_class = priority_class_general

        if priority_class and "priorityClassName" not in yaml_content["spec"]["template"]["spec"]:
            yaml_content["spec"]["template"]["spec"]["priorityClassName"] = priority_class

        # set max_time to avoid having a pod running forever
        use_active_deadline_seconds = resource_settings["use_active_deadline_seconds"]
        if "activeDeadlineSeconds" not in yaml_content["spec"]["template"]["spec"] and use_active_deadline_seconds:
            if not max_time:  # 4 days
                max_time = 4 * 24 * 23600
            yaml_content["spec"]["template"]["spec"]["activeDeadlineSeconds"] = max_time

        tmp_log.debug("creating job {0}".format(yaml_content))

        rsp = self.batchv1.create_namespaced_job(body=yaml_content, namespace=self.namespace)
        return rsp, yaml_content

    def generate_ls_from_wsl(self, workspec_list=[]):
        if workspec_list:
            batch_ids_list = [workspec.batchID for workspec in workspec_list if workspec.batchID]
            batch_ids_concat = ",".join(batch_ids_list)
            label_selector = "job-name in ({0})".format(batch_ids_concat)
        else:
            label_selector = ""

        return label_selector

    def get_workers_info(self, workspec_list=[]):
        tmp_log = core_utils.make_logger(base_logger, "queue_name={0}".format(self.queue_name), method_name="get_workers_info")
        tmp_log.debug("start")

        label_selector = self.generate_ls_from_wsl(workspec_list)

        # get detailed information for available pods
        pods_dict = self.get_pods_info(label_selector)
        if pods_dict is None:  # communication failure to the cluster
            tmp_log.error("Communication failure to cluster. Stopping")
            return None

        # complement pod information with coarse job information
        jobs_dict = self.get_jobs_info(label_selector)

        # combine the pod and job information
        workers_dict = {}
        for worker in workspec_list:
            worker_info = {}
            batch_id = worker.batchID  # batch ID is used as the job name
            if pods_dict and batch_id in pods_dict:
                worker_info.update(pods_dict[batch_id])
            if jobs_dict and batch_id in jobs_dict:
                worker_info.update(jobs_dict[batch_id])
            workers_dict[batch_id] = worker_info

        tmp_log.debug("done")
        return workers_dict

    def get_pods_info(self, label_selector):
        # Monitoring at pod level provides much more information than at job level
        # We use job information in case the pod has been deleted (e.g. in Google bulk exercises), because the job
        # should persist up the TTL.

        tmp_log = core_utils.make_logger(base_logger, "queue_name={0}".format(self.queue_name), method_name="get_pods_info")

        try:
            ret = self.corev1.list_namespaced_pod(namespace=self.namespace, label_selector=label_selector)
        except Exception as _e:
            tmp_log.error("Failed call to list_namespaced_pod with: {0}".format(_e))
            return None  # None needs to be treated differently than [] by the caller

        pods_dict = {}
        for i in ret.items:
            job_name = i.metadata.labels["job-name"] if i.metadata.labels and "job-name" in i.metadata.labels else None

            # pod information
            pod_info = {
                "pod_name": i.metadata.name,
                "pod_start_time": i.status.start_time.replace(tzinfo=None) if i.status.start_time else i.status.start_time,
                "pod_status": i.status.phase,
                "pod_status_conditions": i.status.conditions,
                "pod_status_message": i.status.message,
                "containers_state": [],
            }

            # sub-container information
            if i.status.container_statuses:
                for cs in i.status.container_statuses:
                    if cs.state:
                        pod_info["containers_state"].append(cs.state)

            pods_dict[job_name] = pod_info

        return pods_dict

    def filter_pods_info(self, pods_list, job_name=None):
        if job_name:
            pods_list = [i for i in pods_list if i["job_name"] == job_name]
        return pods_list

    def get_jobs_info(self, label_selector):
        tmp_log = core_utils.make_logger(base_logger, "queue_name={0}".format(self.queue_name), method_name="get_jobs_info")

        jobs_dict = {}

        try:
            ret = self.batchv1.list_namespaced_job(namespace=self.namespace, label_selector=label_selector)

            for i in ret.items:
                name = i.metadata.name
                status = None
                status_reason = None
                status_message = None
                n_pods_succeeded = 0
                n_pods_failed = 0
                if i.status.conditions:  # is only set when a pod started running
                    status = i.status.conditions[0].type
                    status_reason = i.status.conditions[0].reason
                    status_message = i.status.conditions[0].message
                    n_pods_succeeded = i.status.succeeded
                    n_pods_failed = i.status.failed

                job_info = {
                    "job_status": status,
                    "job_status_reason": status_reason,
                    "job_status_message": status_message,
                    "n_pods_succeeded": n_pods_succeeded,
                    "n_pods_failed": n_pods_failed,
                }
                jobs_dict[name] = job_info
        except Exception as _e:
            tmp_log.error("Failed call to list_namespaced_job with: {0}".format(_e))

        return jobs_dict

    def delete_pods(self, pod_name_list):
        tmp_log = core_utils.make_logger(base_logger, "queue_name={0}".format(self.queue_name), method_name="delete_pods")

        tmp_log.debug("Going to delete {0} PODs: {1}".format(len(pod_name_list), pod_name_list))

        ret_list = list()

        for pod_name in pod_name_list:
            rsp = {"name": pod_name}
            try:
                self.corev1.delete_namespaced_pod(name=pod_name, namespace=self.namespace, body=self.deletev1, grace_period_seconds=0)
            except ApiException as _e:
                rsp["errMsg"] = "" if _e.status == 404 else _e.reason
            except Exception as _e:
                rsp["errMsg"] = _e.reason
            else:
                rsp["errMsg"] = ""
            ret_list.append(rsp)

        tmp_log.debug("Done with: {0}".format(ret_list))
        return ret_list

    def delete_job(self, job_name):
        tmp_log = core_utils.make_logger(base_logger, "queue_name={0} job_name={1}".format(self.queue_name, job_name), method_name="delete_job")
        tmp_log.debug("Going to delete JOB {0}".format(job_name))
        try:
            self.batchv1.delete_namespaced_job(name=job_name, namespace=self.namespace, body=self.deletev1, grace_period_seconds=0)
            tmp_log.debug("Deleted JOB {0}".format(job_name))
        except Exception as _e:
            tmp_log.error("Failed to delete JOB {0} with: {1}".format(job_name, _e))

    def delete_config_map(self, config_map_name):
        self.corev1.delete_namespaced_config_map(name=config_map_name, namespace=self.namespace, body=self.deletev1, grace_period_seconds=0)

    def set_affinity(self, yaml_content, use_affinity, use_anti_affinity):
        if not use_affinity and not use_anti_affinity:
            # we are not supposed to use any affinity setting for this queue
            return yaml_content

        yaml_content["spec"]["template"]["spec"]["affinity"] = {}
        yaml_affinity = yaml_content["spec"]["template"]["spec"]["affinity"]

        scores = ["SCORE", "SCORE_HIMEM"]
        mcores = ["MCORE", "MCORE_HIMEM"]

        anti_affinity_matrix = {"SCORE": mcores, "SCORE_HIMEM": mcores, "MCORE": scores, "MCORE_HIMEM": scores}

        affinity_spec = {
            "preferredDuringSchedulingIgnoredDuringExecution": [
                {
                    "weight": 100,
                    "podAffinityTerm": {
                        "labelSelector": {
                            "matchExpressions": [{"key": "resourceType", "operator": "In", "values": ["SCORE", "SCORE_HIMEM", "MCORE", "MCORE_HIMEM"]}]
                        },
                        "topologyKey": "kubernetes.io/hostname",
                    },
                }
            ]
        }

        resource_type = yaml_content["spec"]["template"]["metadata"]["labels"]["resourceType"]

        if use_affinity and resource_type in scores:
            # resource type SCORE* should attract each other instead of spreading across the nodes
            yaml_affinity["podAffinity"] = copy.deepcopy(affinity_spec)

        if use_anti_affinity:
            # SCORE* will repel MCORE* and viceversa. The main reasoning was to keep nodes for MCORE
            # This setting depends on the size of the node vs the MCORE job
            yaml_affinity["podAntiAffinity"] = copy.deepcopy(affinity_spec)
            yaml_affinity["podAntiAffinity"]["preferredDuringSchedulingIgnoredDuringExecution"][0]["podAffinityTerm"]["labelSelector"]["matchExpressions"][0][
                "values"
            ] = anti_affinity_matrix[resource_type]

        return yaml_content

    def create_or_patch_secret(self, file_list, secret_name):
        # api_version = 'v1'
        # kind = 'Secret'
        # type='kubernetes.io/tls'
        rsp = None
        tmp_log = core_utils.make_logger(base_logger, "queue_name={0}".format(self.queue_name), method_name="create_or_patch_secret")

        metadata = {"name": secret_name, "namespace": self.namespace}
        data = {}
        for file_name in file_list:
            filename = os.path.basename(file_name)
            with open(file_name, "rb") as f:
                content = f.read()
            data[filename] = base64.b64encode(content).decode()
        body = client.V1Secret(data=data, metadata=metadata)
        try:
            try:
                rsp = self.corev1.patch_namespaced_secret(name=secret_name, body=body, namespace=self.namespace)
                tmp_log.debug("Patched secret")
            except ApiException as e:
                tmp_log.debug("Exception when patching secret: {0} . Try to create secret instead...".format(e))
                rsp = self.corev1.create_namespaced_secret(body=body, namespace=self.namespace)
                tmp_log.debug("Created secret")
        except Exception as e:
            tmp_log.error("Exception when patching or creating secret: {0}.".format(e))
        return rsp

    def create_configmap(self, work_spec):
        # useful guide: https://matthewpalmer.net/kubernetes-app-developer/articles/ultimate-configmap-guide-kubernetes.html

        tmp_log = core_utils.make_logger(base_logger, "queue_name={0}".format(self.queue_name), method_name="create_configmap")

        try:
            worker_id = str(work_spec.workerID)

            # Get the access point. The messenger should have dropped the input files for the pilot here
            access_point = work_spec.get_access_point()
            pjd = "pandaJobData.out"
            job_data_file = os.path.join(access_point, pjd)
            with open(job_data_file) as f:
                job_data_contents = f.read()

            pfc = "PoolFileCatalog_H.xml"
            pool_file_catalog_file = os.path.join(access_point, pfc)
            with open(pool_file_catalog_file) as f:
                pool_file_catalog_contents = f.read()

            # put the job data and PFC into a dictionary
            data = {pjd: job_data_contents, pfc: pool_file_catalog_contents}

            # instantiate the configmap object
            metadata = {"name": worker_id, "namespace": self.namespace}
            config_map = client.V1ConfigMap(api_version="v1", kind="ConfigMap", data=data, metadata=metadata)

            # create the configmap object in K8s
            api_response = self.corev1.create_namespaced_config_map(namespace=self.namespace, body=config_map)
            tmp_log.debug("Created configmap for worker id: {0}".format(worker_id))
            return True

        except Exception as e:
            tmp_log.error("Could not create configmap with: {0}".format(e))
            return False

    def create_or_patch_configmap_starter(self):
        # useful guide: https://matthewpalmer.net/kubernetes-app-developer/articles/ultimate-configmap-guide-kubernetes.html

        tmp_log = core_utils.make_logger(base_logger, "queue_name={0}".format(self.queue_name), method_name="create_or_patch_configmap_starter")

        try:
            fn = "pilots_starter.py"
            dirname = os.path.dirname(__file__)
            pilots_starter_file = os.path.join(dirname, "../harvestercloud/{0}".format(fn))
            with open(pilots_starter_file) as f:
                pilots_starter_contents = f.read()

            data = {fn: pilots_starter_contents}
            name = "pilots-starter"

            # instantiate the configmap object
            metadata = {"name": name, "namespace": self.namespace}
            config_map = client.V1ConfigMap(api_version="v1", kind="ConfigMap", data=data, metadata=metadata)

            try:
                api_response = self.corev1.patch_namespaced_config_map(name=name, body=config_map, namespace=self.namespace)
                tmp_log.debug("Patched pilots-starter config_map")
            except ApiException as e:
                tmp_log.debug("Exception when patching pilots-starter config_map: {0} . Try to create it instead...".format(e))
                api_response = self.corev1.create_namespaced_config_map(namespace=self.namespace, body=config_map)
                tmp_log.debug("Created pilots-starter config_map")
            return True

        except Exception as e:
            tmp_log.error("Could not create configmap with: {0}".format(e))
            return False

    def get_pod_logs(self, pod_name, previous=False):
        tmp_log = core_utils.make_logger(base_logger, "queue_name={0}".format(self.queue_name), method_name="get_pod_logs")
        try:
            rsp = self.corev1.read_namespaced_pod_log(name=pod_name, namespace=self.namespace, previous=previous)
            tmp_log.debug("Log file retrieved for {0}".format(pod_name))
        except Exception as e:
            tmp_log.debug("Exception when getting logs for pod {0} : {1}. Skipped".format(pod_name, e))
            raise
        else:
            return rsp
