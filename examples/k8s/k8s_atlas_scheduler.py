#!/usr/bin/env python

import os
import time
import random
import json
import re

from kubernetes import client, config, watch

config.load_kube_config(config_file=os.environ.get("KUBECONFIG"))
corev1 = client.CoreV1Api()
scheduler_name = "atlas_scheduler"


def node_allocatable_map(node_status_allocatable):
    cpu_str = node_status_allocatable["cpu"]
    memory_str = node_status_allocatable["memory"]
    mCpu = int(cpu_str) * 1000
    _m = re.match("^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$", memory_str)
    if _m is None:
        print("No memory allocatable in node")
        memoryKB = 0
    elif "M" in _m.group(2):
        memoryKB = int(float(_m.group(1)) * 2**10)
    elif "G" in _m.group(2):
        memoryKB = int(float(_m.group(1)) * 2**20)
    else:
        memoryKB = int(float(_m.group(1)))
    return {"mCpu": mCpu, "memoryKB": memoryKB}


def get_mcpu(containers):
    mcpu_req = 0
    for c in containers:
        if hasattr(c.resources, "requests"):
            mcpu_req_str = c.resources.requests["cpu"]
        elif hasattr(c.resources, "limits"):
            mcpu_req_str = c.resources.limits["cpu"]
        else:
            mcpu_req_str = ""
        _m = re.match("^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$", mcpu_req_str)
        if _m is None:
            print("No cpu reources requests or limits specified")
            mcpu_req += 999
        elif _m.group(2) == "":
            mcpu_req += int(float(_m.group(1)) * 1000)
        elif _m.group(2) == "m":
            mcpu_req += int(float(_m.group(1)))
        else:
            print("Invalid cpu reources requests or limits specified")
            mcpu_req += 999
    return mcpu_req


def get_allocated_resources(namespace="default"):
    node_allocated_resources_map = {}
    ret = corev1.list_namespaced_pod(namespace=namespace, field_selector="status.phase!=Succeeded,status.phase!=Failed")
    for i in ret.items:
        # pod_info = {}
        # pod_info['name'] = i.metadata.name
        # pod_info['status'] = i.status.phase
        # pod_info['status_reason'] = i.status.conditions[0].reason if i.status.conditions else None
        # pod_info['status_message'] = i.status.conditions[0].message if i.status.conditions else None
        nodeName = getattr(i.spec, "node_name", None)
        if nodeName is None:
            continue
        node_allocated_resources_map.setdefault(nodeName, {})
        node_allocated_resources_map[nodeName].setdefault("mCpu", 0)
        node_allocated_resources_map[nodeName]["mCpu"] += get_mcpu(i.spec.containers)
    return node_allocated_resources_map


def nodes_available():
    allocated_resources_map = get_allocated_resources()
    ready_nodes = []
    for node in corev1.list_node().items:
        node_name = node.metadata.name
        for status in node.status.conditions:
            if status.status == "True" and status.type == "Ready":
                node_allocatable_dict = node_allocatable_map(node.status.allocatable)
                mcpu_available = node_allocatable_dict["mCpu"] - allocated_resources_map.get(node_name, {"mCpu": 0})["mCpu"]
                ready_nodes.append({"name": node_name, "mCpu": mcpu_available})
    ready_nodes = sorted(ready_nodes, key=(lambda x: x["mCpu"]))
    return ready_nodes


def scheduler(name, node, namespace="default"):
    target = client.V1ObjectReference()
    target.kind = "Node"
    target.apiVersion = "corev1"
    target.name = node
    print("target", target)
    meta = client.V1ObjectMeta()
    meta.name = name
    body = client.V1Binding(metadata=meta, target=target)
    return corev1.create_namespaced_binding(namespace=namespace, body=body)


def main():
    w = watch.Watch()
    while True:
        for event in w.stream(corev1.list_namespaced_pod, "default", timeout_seconds=30):
            pod = event["object"]
            if pod.status.phase == "Pending" and not pod.spec.node_name and pod.spec.scheduler_name == scheduler_name:
                for node_info in nodes_available():
                    pod_mcpu_req = get_mcpu(pod.spec.containers)
                    node_mcpu_free = node_info["mCpu"]
                    to_bind = pod_mcpu_req <= node_mcpu_free
                    print("Node {0} has {1} mcpu ; pod requests {2} mcpu ; to_bind: {3}".format(node_info["name"], node_mcpu_free, pod_mcpu_req, to_bind))
                    if to_bind:
                        try:
                            print("Scheduling " + pod.metadata.name)
                            res = scheduler(pod.metadata.name, node_info["name"])
                        except ValueError as e:
                            print("ValueError (maybe harmless):", e)
                        except client.rest.ApiException as e:
                            print(json.loads(e.body)["message"])
                        finally:
                            break
            time.sleep(2**-4)


if __name__ == "__main__":
    main()
