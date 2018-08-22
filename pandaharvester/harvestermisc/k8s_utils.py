"""
utilities routines associated with Kubernetes python client

"""
import os
import six
import yaml
from pandaharvester.harvestercore.core_utils import SingletonWithID
from kubernetes import client, config
from kubernetes.client.rest import ApiException

NAMESPACE = "default"

class k8s_Client(six.with_metaclass(SingletonWithID, object)):

    def __init__(self):
        config.load_kube_config()
        self.corev1 = client.CoreV1Api()
        self.batchv1 = client.BatchV1Api()
        self.deletev1 = client.V1DeleteOptions()

    def create_job_from_yaml(self, yaml_file, workerID, cert):
        with open(os.path.join(os.path.dirname(__file__), yaml_file)) as f:
            job = yaml.load(f)
        job['metadata']['name'] = job['metadata']['name'] + "-" + workerID
        job_name = job['metadata']['name']

        for i in range(len(job['spec']['template']['spec']['containers'])):
            proxy_var = job['spec']['template']['spec']['containers'][i]['env']
            for item in proxy_var:
                if item['name'] == 'proxyContent':
                    item['value'] = self.set_proxy(cert)
                if item['name'] == 'workerID':
                    item['value'] = workerID

        rsp = self.batchv1.create_namespaced_job(body=job, namespace=NAMESPACE)
        return job_name

    def get_pod_info_from_job(self, job_name):
        pod_info = {}
        ret = self.corev1.list_namespaced_pod(namespace=NAMESPACE)
        for i in ret.items:
            if i.metadata.labels['job-name'] == job_name:
                pod_info['name'] = i.metadata.name
                pod_info['status'] = i.status.phase
                pod_info['status_reason'] = i.status.conditions[0].reason
                pod_info['status_message'] = i.status.conditions[0].message
        return pod_info

    def get_job_info(self, job_name):
        job_info = {}
        field_selector = 'metadata.name=' + job_name
        ret = self.batchv1.list_namespaced_job(namespace=NAMESPACE, field_selector=field_selector)
        for i in ret.items:
            job_info['name'] = i.metadata.name
            job_info['status'] = i.status.conditions[0].type
            job_info['status_reason'] = i.status.conditions[0].reason
            job_info['status_message'] = i.status.conditions[0].message
        return job_info

    def list_pods_info(self):
        pods_list = []
        pods = {}
        ret = self.corev1.list_namespaced_pod(namespace=NAMESPACE)
        for i in ret.items:
            pods['name'] = i.metadata.name
            pods['status'] = i.status.phase
            pods['status_reason'] = i.status.conditions[0].reason
            pods['status_message'] = i.status.conditions[0].message
            pods_list.append(pods)
        return pods_list

    def list_jobs_name(self):
        jobs_name = []
        ret = self.batchv1.list_namespaced_job(namespace=NAMESPACE)
        for i in ret.items:
            jobs_name.append(i.metadata.name)
        return jobs_name

    def delete_pod(self, pod_name):
         self.corev1.delete_namespaced_pod(name=pod_name, namespace=NAMESPACE, body=self.deletev1, grace_period_seconds=0)

    def delete_job(self, job_name):
         self.batchv1.delete_namespaced_job(name=job_name, namespace=NAMESPACE, body=self.deletev1, grace_period_seconds=0)

    def set_proxy(self, proxy_path):
        with open(proxy_path) as f:
            content = f.read()
        content = content.replace("\n", ",")
        return content
