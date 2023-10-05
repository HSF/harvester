from kubernetes import client, config
import datetime
from subprocess import Popen, PIPE

config.load_kube_config(config_file="PATH TO YOUR CONFIG")
namespace = "default"

nodes = []
current_time = datetime.datetime.now().astimezone()

corev1 = client.CoreV1Api()
aux = corev1.list_namespaced_pod(namespace=namespace, field_selector="status.phase=Pending")
for item in aux.items:
    try:
        if item.status.container_statuses[
            0
        ].state.waiting.reason == "ContainerCreating" and current_time - item.metadata.creation_timestamp > datetime.timedelta(minutes=30):
            if item.spec.node_name not in nodes:
                nodes.append(item.spec.node_name)
    except Exception:
        continue

# delete the node
command_desc = "/bin/gcloud compute instances describe --format=value[](metadata.items.created-by) {0} --zone={1}"
command_del = "/bin/gcloud compute instance-groups managed delete-instances --instances={0} {1} --zone={2}"

zones = ["europe-west1-b", "europe-west1-c", "europe-west1-d"]

for node in nodes:
    for zone in zones:
        command_with_node = command_desc.format(node, zone)
        command_list = command_with_node.split(" ")
        p = Popen(command_list, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        output, err = p.communicate()
        if output:
            output_str = output[:-1].decode()
            command_del_with_vars = command_del.format(node, output_str, zone)
            command_del_list = command_del_with_vars.split(" ")
            p = Popen(command_del_list, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            output, err = p.communicate()
            print(command_del_with_vars)
            print(output)
            print(err)
            print("--------------------")
