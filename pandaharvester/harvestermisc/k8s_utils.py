"""
utilities routines associated with Kubernetes python client
"""
import os
import copy
import base64
import yaml

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict

base_logger = core_utils.setup_logger('k8s_utils')

CONFIG_DIR = '/scratch/jobconfig'
DEF_COMMAND = ["/usr/bin/bash"]
DEF_ARGS = ["-c", "cd; wget https://raw.githubusercontent.com/HSF/harvester/k8s_analysis/pandaharvester/harvestercloud/pilots_starter.py; chmod 755 pilots_starter.py; ./pilots_starter.py; sleep 300 || true"]

class k8s_Client(object):

    def __init__(self, namespace, config_file=None):
        config.load_kube_config(config_file=config_file)
        self.namespace = namespace if namespace else 'default'
        self.corev1 = client.CoreV1Api()
        self.batchv1 = client.BatchV1Api()
        self.deletev1 = client.V1DeleteOptions(propagation_policy='Background')

    def read_yaml_file(self, yaml_file):
        with open(yaml_file) as f:
            yaml_content = yaml.load(f, Loader=yaml.FullLoader)

        return yaml_content

    def create_job_from_yaml(self, yaml_content, work_spec, container_image, cert, cert_in_secret=True,
                             cpu_adjust_ratio=100, memory_adjust_ratio=100, executable=None):

        tmp_log = core_utils.make_logger(base_logger, method_name='create_job_from_yaml')

        submit_mode = 'PULL'

        # create the configmap in push mode
        worker_id = None
        if work_spec.mapType != 'NoJob':
            submit_mode = 'PUSH'
            worker_id = str(work_spec.workerID)
            res = self.create_configmap(work_spec)
            if not res:  # if the configmap creation failed, don't submit a job because the pod creation will hang
                return res, 'Failed to create a configmap'

        # retrieve panda queue information
        panda_queues_dict = PandaQueuesDict()
        queue_name = panda_queues_dict.get_panda_queue_name(work_spec.computingSite)

        # set the worker name
        yaml_content['metadata']['name'] = yaml_content['metadata']['name'] + "-" + str(work_spec.workerID)

        # set the resource type and other metadata to filter the pods
        yaml_content['spec']['template'].setdefault('metadata', {})
        yaml_content['spec']['template']['metadata'].update({'labels':
                                                                 {'resourceType': str(work_spec.resourceType)}
                                                             })

        # fill the container details. we can only handle one container (take the first, delete the rest)
        yaml_containers = yaml_content['spec']['template']['spec']['containers']
        del(yaml_containers[1:len(yaml_containers)])
        container_env = yaml_containers[0]

        # set the container image
        if 'image' not in container_env:
            container_env['image'] = container_image

        if executable:
            command = executable
            args = ''
        else:
            command = DEF_COMMAND
            args = DEF_ARGS

        if 'command' not in container_env:
            container_env['command'] = command
            container_env['args'] = args

        # set the resources (CPU and memory) we need for the container
        # note that predefined values in the yaml template will NOT be overwritten
        container_env.setdefault('resources', {})
        if work_spec.nCore > 0:

            # CPU limits
            container_env['resources'].setdefault('limits', {})
            if 'cpu' not in container_env['resources']['limits']:
                container_env['resources']['limits']['cpu'] = str(work_spec.nCore)
            # CPU requests
            container_env['resources'].setdefault('requests', {})
            if 'cpu' not in container_env['resources']['requests']:
                container_env['resources']['requests']['cpu'] = str(work_spec.nCore * cpu_adjust_ratio / 100.0)

        if work_spec.minRamCount > 4: # K8S minimum memory limit = 4 MB
            # memory limits
            container_env['resources'].setdefault('limits', {})
            if 'memory' not in container_env['resources']['limits']:
                container_env['resources']['limits']['memory'] = str(work_spec.minRamCount) + 'M'
            # memory requests
            container_env['resources'].setdefault('requests', {})
            if 'memory' not in container_env['resources']['requests']:
                container_env['resources']['requests']['memory'] = str(work_spec.minRamCount * memory_adjust_ratio / 100.0) + 'M'

        # try to retrieve the stdout log file name
        try:
            log_file_name = work_spec.workAttributes['stdout']
        except (KeyError, AttributeError):
            tmp_log.debug('work_spec does not have workAttributes field: {0}'.format(work_spec))
            log_file_name = ''

        # set the environment variables
        container_env.setdefault('env', [])
        container_env['env'].extend([
            {'name': 'computingSite', 'value': work_spec.computingSite},
            {'name': 'pandaQueueName', 'value': queue_name},
            {'name': 'resourceType', 'value': work_spec.resourceType},
            {'name': 'proxySecretPath', 'value': cert if cert_in_secret else None},
            {'name': 'proxyContent', 'value': None if cert_in_secret else self.set_proxy(cert)},
            {'name': 'workerID', 'value': str(work_spec.workerID)},
            {'name': 'logs_frontend_w', 'value': harvester_config.pandacon.pandaCacheURL_W},
            {'name': 'logs_frontend_r', 'value': harvester_config.pandacon.pandaCacheURL_R},
            {'name': 'stdout_name', 'value': log_file_name},
            {'name': 'PANDA_JSID', 'value': 'harvester-' + harvester_config.master.harvester_id},
            {'name': 'HARVESTER_WORKER_ID', 'value': str(work_spec.workerID)},
            {'name': 'HARVESTER_ID', 'value': harvester_config.master.harvester_id},
            {'name': 'submit_mode', 'value': submit_mode}
            ])

        # in push mode, add the configmap as a volume to the pod
        if submit_mode == 'PUSH' and worker_id:
            yaml_content['spec']['template']['spec'].setdefault('volumes', [])
            yaml_volumes = yaml_content['spec']['template']['spec']['volumes']
            yaml_volumes.append({'name': 'job-config', 'configMap': {'name': worker_id}})
            # mount the volume to the filesystem
            container_env.setdefault('volumeMounts', [])
            container_env['volumeMounts'].append({'name': 'job-config', 'mountPath': CONFIG_DIR})

        # set the affinity
        if 'affinity' not in yaml_content['spec']['template']['spec']:
            yaml_content = self.set_affinity(yaml_content)

        rsp = self.batchv1.create_namespaced_job(body=yaml_content, namespace=self.namespace)
        return rsp, yaml_content

    def get_pods_info(self):
        pods_list = list()

        ret = self.corev1.list_namespaced_pod(namespace=self.namespace)

        for i in ret.items:
            pod_info = {
                'name': i.metadata.name,
                'start_time': i.status.start_time.replace(tzinfo=None),
                'status': i.status.phase,
                'status_reason': i.status.conditions[0].reason if i.status.conditions else None,
                'status_message': i.status.conditions[0].message if i.status.conditions else None,
                'job_name': i.metadata.labels['job-name'] if i.metadata.labels and 'job-name' in i.metadata.labels else None
            }
            pods_list.append(pod_info)

        return pods_list

    def filter_pods_info(self, pods_list, job_name=None):
        if job_name:
            pods_list = [ i for i in pods_list if i['job_name'] == job_name]
        return pods_list

    def get_jobs_info(self, job_name=None):
        jobs_list = list()

        field_selector = 'metadata.name=' + job_name if job_name else ''
        ret = self.batchv1.list_namespaced_job(namespace=self.namespace, field_selector=field_selector)

        for i in ret.items:
            job_info = {
                'name': i.metadata.name,
                'status': i.status.conditions[0].type,
                'status_reason': i.status.conditions[0].reason,
                'status_message': i.status.conditions[0].message
            }
            jobs_list.append(job_info)
        return jobs_list

    def delete_pods(self, pod_name_list):
        ret_list = list()

        for pod_name in pod_name_list:
            rsp = {}
            rsp['name'] = pod_name
            try:
                self.corev1.delete_namespaced_pod(name=pod_name, namespace=self.namespace, body=self.deletev1, grace_period_seconds=0)
            except ApiException as _e:
                rsp['errMsg'] = '' if _e.status == 404 else _e.reason
            else:
                rsp['errMsg'] = ''
            ret_list.append(rsp)

        return ret_list

    def delete_job(self, job_name):
        self.batchv1.delete_namespaced_job(name=job_name, namespace=self.namespace, body=self.deletev1,
                                           grace_period_seconds=0)

    def delete_config_map(self, config_map_name):
        self.corev1.delete_namespaced_config_map(name=config_map_name, namespace=self.namespace, body=self.deletev1,
                                                  grace_period_seconds=0)

    def set_proxy(self, proxy_path):
        with open(proxy_path) as f:
            content = f.read()
        content = content.replace("\n", ",")
        return content

    def set_affinity(self, yaml_content):
        yaml_content['spec']['template']['spec']['affinity'] = {}
        yaml_affinity = yaml_content['spec']['template']['spec']['affinity']
        res_element = {'SCORE', 'MCORE'}
        affinity_spec = {
            'preferredDuringSchedulingIgnoredDuringExecution': [
                {'weight': 100, 'podAffinityTerm': {
                    'labelSelector': {'matchExpressions': [
                        {'key': 'resourceType', 'operator': 'In', 'values': ['SCORE']}]},
                    'topologyKey': 'kubernetes.io/hostname'}
                }]}

        resourceType = yaml_content['spec']['template']['metadata']['labels']['resourceType']

        if resourceType == 'SCORE':
            yaml_affinity['podAffinity'] = copy.deepcopy(affinity_spec)
            yaml_affinity['podAffinity']['preferredDuringSchedulingIgnoredDuringExecution'][0]['podAffinityTerm']['labelSelector']['matchExpressions'][0]['values'][0] = resourceType

        yaml_affinity['podAntiAffinity'] = copy.deepcopy(affinity_spec)
        yaml_affinity['podAntiAffinity']['preferredDuringSchedulingIgnoredDuringExecution'][0]['podAffinityTerm']['labelSelector']['matchExpressions'][0]['values'][0] = res_element.difference({resourceType}).pop()

        return yaml_content

    def create_or_patch_secret(self, file_list, secret_name):
        # api_version = 'v1'
        # kind = 'Secret'
        # type='kubernetes.io/tls'

        tmp_log = core_utils.make_logger(base_logger, method_name='create_or_patch_secret')

        metadata = {'name': secret_name, 'namespace': self.namespace}
        data = {}
        for file_name in file_list:
            filename = os.path.basename(file_name)
            with open(file_name, 'rb') as f:
                content = f.read()
            data[filename] = base64.b64encode(content).decode()
        body = client.V1Secret(data=data, metadata=metadata)
        try:
            rsp = self.corev1.patch_namespaced_secret(name=secret_name, body=body, namespace=self.namespace)
        except ApiException as e:
            tmp_log.debug('Exception when patching secret: {0} . Try to create secret instead...'.format(e))
            rsp = self.corev1.create_namespaced_secret(body=body, namespace=self.namespace)
        return rsp

    def retrieve_pod_log(self, job_id):

        tmp_log = core_utils.make_logger(base_logger, method_name='retrieve_pod_log')

        try:
            log_content = self.corev1.read_namespaced_pod_log(name=job_id, namespace=self.namespace)
            # TODO: figure out what to do with the log. Ideally store it in file and let communicator take care of it
            return log_content
        except ApiException as e:
            tmp_log.debug('Exception retrieving logs for pod {0}: {1}'.format(job_id, e))
            return None

    def create_configmap(self, work_spec):
        # useful guide: https://matthewpalmer.net/kubernetes-app-developer/articles/ultimate-configmap-guide-kubernetes.html

        tmp_log = core_utils.make_logger(base_logger, method_name='create_configmap')

        try:
            worker_id = str(work_spec.workerID)

            # Get the access point. The messenger should have dropped the input files for the pilot here
            access_point = work_spec.get_access_point()
            pjd = 'pandaJobData.out'
            job_data_file = os.path.join(access_point, pjd)
            with open(job_data_file) as f:
                job_data_contents = f.read()

            pfc = 'PoolFileCatalog_H.xml'
            pool_file_catalog_file = os.path.join(access_point, pfc)
            with open(pool_file_catalog_file) as f:
                pool_file_catalog_contents = f.read()

            # put the job data and PFC into a dictionary
            data = {pjd: job_data_contents, pfc: pool_file_catalog_contents}

            # instantiate the configmap object
            metadata = {'name': worker_id, 'namespace': self.namespace}
            config_map = client.V1ConfigMap(api_version="v1", kind="ConfigMap", data=data, metadata=metadata)

            # create the configmap object in K8s
            api_response = self.corev1.create_namespaced_config_map(namespace=self.namespace, body=config_map)
            tmp_log.debug('Created configmap for worker id: {0}'.format(worker_id))
            return True

        except (ApiException, TypeError) as e:
            tmp_log.error('Could not create configmap with: {0}'.format(e))
            return False

