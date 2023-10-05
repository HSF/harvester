from kubernetes import client, config
from kubernetes.client.rest import ApiException

config.load_kube_config(config_file="/opt/harvester_k8s/kubeconf")
namespace = ""  # namespace needs to be set or read from queue configuration

corev1 = client.CoreV1Api()
batchv1 = client.BatchV1Api()


# unavailable nodes
nodes_unav = []
nodes_av = {}

# get nodes and make a dictionary with the available ones
nodes = corev1.list_node()
for node in nodes.items:
    print("Processing {0}".format(node.metadata.name))
    for condition in node.status.conditions:
        print("Condition: {0}, status: {1}".format(condition.type, type(condition.status)))
        if condition.type == "Ready" and condition.status == "Unknown":
            nodes_unav.append(node.metadata.name)
        elif condition.type == "Ready" and condition.status == "True":
            nodes_av.setdefault(node.metadata.name, [])

print("Unavailable nodes: {0}".format(len(nodes_unav)))
print("Available nodes: {0}".format(len(nodes_av.keys())))

# get pods and pack them into the available nodes
pods = corev1.list_namespaced_pod(namespace=namespace)
for pod in pods.items:
    print(pod.metadata.name)
    cpus = 0
    if pod.status.phase == "Running" and pod.spec.containers:
        for container in pod.spec.containers:
            print(container.resources.limits)
            if container.resources.limits and "cpu" in container.resources.limits:
                cpus += int(container.resources.limits["cpu"])
        try:
            nodes_av[pod.spec.node_name].append(cpus)
        except KeyError:
            pass

for node in nodes_av:
    if not sum(nodes_av[node]):
        print("{0}: occupied cpus {1}".format(node, nodes_av[node]))
