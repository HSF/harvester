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
        self.deletev1 = client.V1DeleteOptions(propagation_policy='Background')

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

    def get_pods_info(self, job_name=None):
        pods_list = list()

        ret = self.corev1.list_namespaced_pod(namespace=NAMESPACE)

        for i in ret.items:
            pod_info = {}
            pod_info['name'] = i.metadata.name
            pod_info['status'] = i.status.phase
            pod_info['status_reason'] = i.status.conditions[0].reason if i.status.conditions else None
            pod_info['status_message'] = i.status.conditions[0].message if i.status.conditions else None
            pod_info['job_name'] = i.metadata.labels['job-name'] if 'job-name' in i.metadata.labels else None
            pods_list.append(pod_info)
        if job_name:
            tmp_list = [ i for i in pods_list if i['job_name'] == job_name]
            del pods_list[:]
            pods_list = tmp_list
        return pods_list

    def get_jobs_info(self, job_name=None):
        jobs_list = list()

        field_selector = 'metadata.name=' + job_name if job_name else ''
        ret = self.batchv1.list_namespaced_job(namespace=NAMESPACE, field_selector=field_selector)

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
                self.corev1.delete_namespaced_pod(name=pod_name, namespace=NAMESPACE, body=self.deletev1, grace_period_seconds=0)
            except ApiException as _e:
                rsp['errMsg'] = '' if _e.status == 404 else _e.reason
            else:
                rsp['errMsg'] = ''
            retList.append(rsp)

        return retList

    def delete_job(self, job_name):
        self.batchv1.delete_namespaced_job(name=job_name, namespace=NAMESPACE, body=self.deletev1, grace_period_seconds=0)

    def set_proxy(self, proxy_path):
        with open(proxy_path) as f:
            content = f.read()
        content = content.replace("\n", ",")
        return content
