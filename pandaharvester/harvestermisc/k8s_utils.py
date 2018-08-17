"""
utilities routines associated with Kubernetes python client

"""
import os
import yaml
from datetime import datetime
from kubernetes import client, config
from kubernetes.client.rest import ApiException

NAMESPACE = "default"

class k8s_Client:

    def __init__(self):
        config.load_kube_config()
        self.corev1 = client.CoreV1Api()
        self.batchv1 = client.BatchV1Api()

    def create_job_from_yaml(self, yaml_file, workerID, cert):
        with open(os.path.join(os.path.dirname(__file__), yaml_file)) as f:
            job = yaml.load(f)
        currenttime = datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')
        job['metadata']['name'] = job['metadata']['name'] + "-" + currenttime
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

    def delete_job(self, job_name):
        try:
            self.batchv1.delete_namespaced_job(name=job_name, namespace=NAMESPACE, body=client.V1DeleteOptions(), grace_period_seconds=0)
        except ApiException as _e:
            print("%s" % _e)

    def set_proxy(self, proxy_path):
        with open(proxy_path) as f:
            content = f.read()
        content = content.replace("\n", ",")
        return content
