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
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestercore import core_utils

base_logger = core_utils.setup_logger('k8s_utils')


class k8s_Client(object):

    def __init__(self, namespace, config_file=None):
        if not os.path.isfile(config_file):
            raise RuntimeError('Cannot find k8s config file: {0}'.format(config_file))
        config.load_kube_config(config_file=config_file)
        self.namespace = namespace if namespace else 'default'
        self.corev1 = client.CoreV1Api()
        self.batchv1 = client.BatchV1Api()
        self.deletev1 = client.V1DeleteOptions(propagation_policy='Background')

    def read_yaml_file(self, yaml_file):
        with open(yaml_file) as f:
            yaml_content = yaml.load(f, Loader=yaml.FullLoader)

        return yaml_content

    def create_job_from_yaml(self, yaml_content, work_spec, cert, cert_in_secret=True, cpuadjustratio=100,
                             memoryadjustratio=100):

        # tmp_log = core_utils.make_logger(base_logger, method_name='create_job_from_yaml')

        panda_queues_dict = PandaQueuesDict()
        queue_name = panda_queues_dict.get_panda_queue_name(work_spec.computingSite)

        yaml_content['metadata']['name'] = yaml_content['metadata']['name'] + "-" + str(work_spec.workerID)

        yaml_content['spec']['template'].setdefault('metadata', {})
        yaml_content['spec']['template']['metadata'].update({
            'labels': {'resourceType': str(work_spec.resourceType)}})

        yaml_containers = yaml_content['spec']['template']['spec']['containers']
        del (yaml_containers[1:len(yaml_containers)])

        container_env = yaml_containers[0]

        container_env.setdefault('resources', {})

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
                container_env['resources']['requests']['cpu'] = str(work_spec.nCore * cpuadjustratio / 100.0)

        if work_spec.minRamCount > 4:  # K8S minimum memory limit = 4 MB
            # memory limits
            container_env['resources'].setdefault('limits', {})
            if 'memory' not in container_env['resources']['limits']:
                container_env['resources']['limits']['memory'] = str(work_spec.minRamCount) + 'M'
            # memory requests
            container_env['resources'].setdefault('requests', {})
            if 'memory' not in container_env['resources']['requests']:
                container_env['resources']['requests']['memory'] = str(
                    work_spec.minRamCount * memoryadjustratio / 100.0) + 'M'

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
            {'name': 'PANDA_JSID', 'value': 'harvester-' + harvester_config.master.harvester_id},
            {'name': 'HARVESTER_WORKER_ID', 'value': str(work_spec.workerID)},
            {'name': 'HARVESTER_ID', 'value': harvester_config.master.harvester_id}
        ])

        if 'affinity' not in yaml_content['spec']['template']['spec']:
            yaml_content = self.set_affinity(yaml_content)

        # tmp_log.debug('creating job {0}'.format(yaml_content))

        rsp = self.batchv1.create_namespaced_job(body=yaml_content, namespace=self.namespace)
        return rsp

    def get_pods_info(self):

        tmp_log = core_utils.make_logger(base_logger, method_name='get_pods_info')

        pods_list = list()

        try:
            ret = self.corev1.list_namespaced_pod(namespace=self.namespace)
        except Exception as _e:
            tmp_log.error('Failed call to list_namespaced_pod with: {0}'.format(_e))

        for i in ret.items:
            pod_info = {}
            pod_info['name'] = i.metadata.name
            pod_info['start_time'] = i.status.start_time.replace(
                tzinfo=None) if i.status.start_time else i.status.start_time
            pod_info['status'] = i.status.phase
            pod_info['status_conditions'] = i.status.conditions
            pod_info['job_name'] = i.metadata.labels[
                'job-name'] if i.metadata.labels and 'job-name' in i.metadata.labels else None
            pod_info['containers_state'] = []
            if i.status.container_statuses:
                for cs in i.status.container_statuses:
                    if cs.state:
                        pod_info['containers_state'].append(cs.state)
            pods_list.append(pod_info)

        return pods_list

    def filter_pods_info(self, pods_list, job_name=None):
        if job_name:
            pods_list = [i for i in pods_list if i['job_name'] == job_name]
        return pods_list

    def get_jobs_info(self, job_name=None):

        tmp_log = core_utils.make_logger(base_logger, 'job_name={0}'.format(job_name), method_name='get_jobs_info')

        jobs_list = list()

        field_selector = 'metadata.name=' + job_name if job_name else ''
        try:
            ret = self.batchv1.list_namespaced_job(namespace=self.namespace, field_selector=field_selector)

            for i in ret.items:
                job_info = {'name': i.metadata.name, 'status': i.status.conditions[0].type,
                            'status_reason': i.status.conditions[0].reason,
                            'status_message': i.status.conditions[0].message}
                jobs_list.append(job_info)
        except Exception as _e:
            tmp_log.error('Failed call to list_namespaced_job with: {0}'.format(_e))

        return jobs_list

    def delete_pods(self, pod_name_list):
        ret_list = list()

        for pod_name in pod_name_list:
            rsp = {}
            rsp['name'] = pod_name
            try:
                self.corev1.delete_namespaced_pod(name=pod_name, namespace=self.namespace, body=self.deletev1,
                                                  grace_period_seconds=0)
            except ApiException as _e:
                rsp['errMsg'] = '' if _e.status == 404 else _e.reason
            except Exception as _e:
                rsp['errMsg'] = _e.reason
            else:
                rsp['errMsg'] = ''
            ret_list.append(rsp)

        return ret_list

    def delete_job(self, job_name):
        tmp_log = core_utils.make_logger(base_logger, 'job_name={0}'.format(job_name), method_name='delete_job')
        try:
            self.batchv1.delete_namespaced_job(name=job_name, namespace=self.namespace, body=self.deletev1,
                                               grace_period_seconds=0)
        except Exception as _e:
            tmp_log.error('Failed call to delete_namespaced_job with: {0}'.format(_e))

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

        resource_type = yaml_content['spec']['template']['metadata']['labels']['resourceType']

        if resource_type == 'SCORE':
            yaml_affinity['podAffinity'] = copy.deepcopy(affinity_spec)
            yaml_affinity['podAffinity']['preferredDuringSchedulingIgnoredDuringExecution'][0]['podAffinityTerm'][
                'labelSelector']['matchExpressions'][0]['values'][0] = resource_type

        yaml_affinity['podAntiAffinity'] = copy.deepcopy(affinity_spec)
        yaml_affinity['podAntiAffinity']['preferredDuringSchedulingIgnoredDuringExecution'][0]['podAffinityTerm'][
            'labelSelector']['matchExpressions'][0]['values'][0] = res_element.difference({resource_type}).pop()

        return yaml_content

    def create_or_patch_secret(self, file_list, secret_name):
        # api_version = 'v1'
        # kind = 'Secret'
        # type='kubernetes.io/tls'

        tmp_log = core_utils.make_logger(base_logger, method_name='create_or_patch_secret')

        metadata = {'name': secret_name, 'namespace': self.namespace}
        data = {}
        for file in file_list:
            filename = os.path.basename(file)
            with open(file, 'rb') as f:
                aux = f.read()
            data[filename] = base64.b64encode(aux).decode()
        body = client.V1Secret(data=data, metadata=metadata)
        try:
            try:
                rsp = self.corev1.patch_namespaced_secret(name=secret_name, body=body, namespace=self.namespace)
            except ApiException as e:
                tmp_log.debug('Exception when patch secret: {0} . Try to create secret instead...'.format(e))
                rsp = self.corev1.create_namespaced_secret(body=body, namespace=self.namespace)
        except Exception as e:
            tmp_log.error('Exception when patch or creating secret: {0}.'.format(e))
        return rsp

    def get_pod_logs(self, pod_name, previous=False):
        tmp_log = core_utils.make_logger(base_logger, method_name='get_pod_logs')
        try:
            rsp = self.corev1.read_namespaced_pod_log(name=pod_name, namespace=self.namespace, previous=previous)
        except Exception as e:
            tmp_log.debug('Exception when getting logs for pod {0} : {1}. Skipped'.format(pod_name, e))
            raise
        else:
            return rsp
