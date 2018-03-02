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

METADATA_URL = "http://metadata.google.internal/computeMetadata/v1/instance/attributes/{0}"


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

# TODO: upload logs to panda cache


