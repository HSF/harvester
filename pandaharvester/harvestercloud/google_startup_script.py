#!/usr/bin/env python

"""
This script will be executed at the VM startup time.
- It will download the proxy and panda queue from Google instance metadata
- It will download the pilot wrapper from github and execute it
- It will upload the pilot logs to panda cache
"""

import requests
import subprocess
import os
from threading import Timer

METADATA_URL = "http://metadata.google.internal/computeMetadata/v1/instance/attributes/{0}"


def contact_harvester(harvester_frontend, data):
    try:
        resp = requests.post(harvester_frontend, json=data, headers={'Content-Type': 'application/json'})
    except:
        # message could not be sent
        pass

def heartbeat(harvester_frontend, worker_id):
    data = {'methodName': 'heartbeat', 'workerID': worker_id, 'data': None}
    return contact_harvester(harvester_frontend, data)

def suicide(harvester_frontend, worker_id):
    data = {'methodName': 'killWorker', 'workerID': worker_id, 'data': None}
    return contact_harvester(harvester_frontend, data)

def get_url(url, headers=None):
    """
    get content from specified URL
    """

    reply = requests.get(url, headers=headers)
    if reply.status_code != 200:
        print '[get_attribute] Failed to open {0}'.format(url)
        return None
    else:
        return reply.content


# get the proxy certificate and save it
proxy_path = "/tmp/x509up"
proxy_url = METADATA_URL.format("proxy")
proxy_string = get_url(proxy_url, headers={"Metadata-Flavor": "Google"})
with open(proxy_path, "w") as proxy_file:
    proxy_file.write(proxy_string)

os.environ['X509_USER_PROXY'] = proxy_path

# get the panda queue name
pq_url = METADATA_URL.format("panda_queue")
panda_queue = get_url(pq_url, headers={"Metadata-Flavor": "Google"})

harvester_frontend_url = METADATA_URL.format("harvester_frontend")
harvester_frontend = get_url(harvester_frontend_url, headers={"Metadata-Flavor": "Google"})

worker_id_url = METADATA_URL.format("worker_id")
worker_id = get_url(worker_id_url, headers={"Metadata-Flavor": "Google"})

# start a thread that will send a heartbeat to harvester every 5 minutes
heartbeat_thread = Timer(300, heartbeat, [harvester_frontend, worker_id])
heartbeat_thread.start()

# get the pilot wrapper
wrapper_path = "/tmp/runpilot3-wrapper.sh"
wrapper_url = "https://raw.githubusercontent.com/fbarreir/adc/master/runpilot3-wrapper.sh"
wrapper_string = get_url(wrapper_url)
with open(wrapper_path, "w") as wrapper_file:
    wrapper_file.write(wrapper_string)
os.chmod(wrapper_path, 0544) # make pilot wrapper executable

# execute the pilot wrapper
command = "/tmp/runpilot3-wrapper.sh -s {0} -h {0} -p 25443 -w https://pandaserver.cern.ch >& /tmp/wrapper-wid.log".format(panda_queue)
subprocess.call(command, shell=True)

# ask harvester to kill the VM and stop the heartbeat
suicide(harvester_frontend, worker_id)
heartbeat_thread.cancel()


# TODO: upload logs to panda cache or harvester


