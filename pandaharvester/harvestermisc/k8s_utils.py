"""
utilities routines associated with Kubernetes python client

"""
import os
import six
import yaml

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.core_utils import SingletonWithID
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict


class k8s_Client(six.with_metaclass(SingletonWithID, object)):

    def __init__(self, namespace, config_file=None):
        config.load_kube_config(config_file=config_file)
        self.namespace = namespace if namespace else 'default'
        self.corev1 = client.CoreV1Api()
        self.batchv1 = client.BatchV1Api()
        self.deletev1 = client.V1DeleteOptions(propagation_policy='Background')

    def read_yaml_file(self, yaml_file):
        with open(yaml_file) as f:
            yaml_content = yaml.load(f)

        return yaml_content

    def create_job_from_yaml(self, yaml_content, work_spec, cert):
        panda_queues_dict = PandaQueuesDict()
        queue_name = panda_queues_dict.get_panda_queue_name(work_spec.computingSite)
        # queue_dict = panda_queues_dict.get(queue_name, {})

        yaml_content['metadata']['name'] = yaml_content['metadata']['name'] + "-" + str(work_spec.workerID)

        yaml_containers = yaml_content['spec']['template']['spec']['containers']
        del(yaml_containers[1:len(yaml_containers)])

        container_env = yaml_containers[0]
        container_env.setdefault('resources', {})

        #if 'limits' not in container_env['resources']:
        #    container_env['resources']['limits'] = {'memory': str(queue_dict.get('maxrss', '')) + 'M', 'cpu': str(queue_dict.get('corecount', 1)) \
        #        if queue_dict.get('corecount', 1) else '1'}
        #if 'requests' not in container_env['resources']:
        #    container_env['resources']['requests'] = {'memory': str(work_spec.minRamCount) + 'M', 'cpu': str(work_spec.nCore)}

        container_env['resources']['limits'] = {'memory': str(work_spec.minRamCount) + 'M',
                                                'cpu': str(work_spec.nCore)}

        container_env['resources']['requests'] = {'memory': str(work_spec.minRamCount) + 'M',
                                                  'cpu': str(work_spec.nCore)}

        container_env.setdefault('env', [])
        container_env['env'].extend([
            {'name': 'computingSite', 'value': work_spec.computingSite},
            {'name': 'pandaQueueName', 'value': queue_name},
            {'name': 'resourceType', 'value': work_spec.resourceType},
            {'name': 'proxyContent', 'value': self.set_proxy(cert)},
            {'name': 'workerID', 'value': str(work_spec.workerID)},
            {'name': 'logs_frontend_w', 'value': harvester_config.pandacon.pandaCacheURL_W},
            {'name': 'logs_frontend_r', 'value': harvester_config.pandacon.pandaCacheURL_R},
            {'name': 'PANDA_JSID', 'value': 'harvester-' + harvester_config.master.harvester_id},
            ])

        rsp = self.batchv1.create_namespaced_job(body=yaml_content, namespace=self.namespace)

    def get_pods_info(self, job_name=None):
        pods_list = list()

        ret = self.corev1.list_namespaced_pod(namespace=self.namespace)

        for i in ret.items:
            pod_info = {}
            pod_info['name'] = i.metadata.name
            pod_info['status'] = i.status.phase
            pod_info['status_reason'] = i.status.conditions[0].reason if i.status.conditions else None
            pod_info['status_message'] = i.status.conditions[0].message if i.status.conditions else None
            pod_info['job_name'] = i.metadata.labels['job-name'] if i.metadata.labels and 'job-name' in i.metadata.labels else None
            pods_list.append(pod_info)
        if job_name:
            tmp_list = [ i for i in pods_list if i['job_name'] == job_name]
            del pods_list[:]
            pods_list = tmp_list
        return pods_list

    def get_jobs_info(self, job_name=None):
        jobs_list = list()

        field_selector = 'metadata.name=' + job_name if job_name else ''
        ret = self.batchv1.list_namespaced_job(namespace=self.namespace, field_selector=field_selector)

        for i in ret.items:
            job_info = {}
            job_info['name'] = i.metadata.name
            job_info['status'] = i.status.conditions[0].type
            job_info['status_reason'] = i.status.conditions[0].reason
            job_info['status_message'] = i.status.conditions[0].message
            jobs_list.append(job_info)
        return jobs_list

    def delete_pod(self, pod_name_list):
        retList = list()

        for pod_name in pod_name_list:
            rsp = {}
            rsp['name'] = pod_name
            try:
                self.corev1.delete_namespaced_pod(name=pod_name, namespace=self.namespace, body=self.deletev1, grace_period_seconds=0)
            except ApiException as _e:
                rsp['errMsg'] = '' if _e.status == 404 else _e.reason
            else:
                rsp['errMsg'] = ''
            retList.append(rsp)

        return retList

    def delete_job(self, job_name):
        self.batchv1.delete_namespaced_job(name=job_name, namespace=self.namespace, body=self.deletev1, grace_period_seconds=0)

    def set_proxy(self, proxy_path):
        with open(proxy_path) as f:
            content = f.read()
        content = content.replace("\n", ",")
        return content
