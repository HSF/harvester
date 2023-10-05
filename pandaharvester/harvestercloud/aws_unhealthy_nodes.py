# Detect and delete nodes that are stuck

from kubernetes import client, config
from subprocess import Popen, PIPE

config.load_kube_config(config_file="YOUR KUBECONFIG FILE")
apis_api = client.CoreV1Api()

# get running nodes
running_nodes = {}
nodes = apis_api.list_node()
for node in nodes.items:
    running_nodes[node.metadata.name] = node.spec.provider_id.split("/")[-1]

# get events with FailedMounts and filter them by known error message
failed_mount_events = apis_api.list_namespaced_event(namespace="default", field_selector="reason=FailedMount")

unhealthy_node_ids = set()
for event in failed_mount_events.items:
    node_name = event.source.host
    if "Argument list too long" in event.message and node_name in running_nodes:
        unhealthy_node_ids.add(running_nodes[node_name])

# set the node as unhealthy using the AWS CLI
command = "/usr/local/bin/aws autoscaling set-instance-health --instance-id {0} --health-status Unhealthy"
for id in unhealthy_node_ids:
    command_with_id = command.format(id)
    command_list = command_with_id.split(" ")
    p = Popen(command_list, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()
    print("------------------------------------")
    print(command_with_id)
    print("return code: {0}".format(p.returncode))
    print("output: {0}".format(output))
    print("err: {0}".format(err))
    print("------------------------------------")
