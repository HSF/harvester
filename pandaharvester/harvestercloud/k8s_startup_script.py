#!/usr/bin/env python

"""
This script will be executed at the VM startup time.
- It will download the proxy and panda queue from Google instance metadata
- It will download the pilot wrapper from github and execute it
- It will upload the pilot logs to panda cache
"""

import requests
try:
    import subprocess32 as subprocess
except:
    import subprocess
import os
import sys
import logging
import time
import traceback
import zlib
from threading import Thread

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s',
                    filename='/tmp/vm_script.log', filemode='w')

global loop
loop = True


def upload_logs(url, log_file_name, destination_name, proxy_path):
    try:

        # open and compress the content of the file
        with open(log_file_name, 'rb') as log_file_object:
            files = {'file': (destination_name, log_file_object.read())}

        cert = [proxy_path, proxy_path]
        # verify = '/etc/grid-security/certificates' # not supported in CernVM - requests.exceptions.SSLError: [Errno 21] Is a directory

        logging.debug('[upload_logs] start')
        res = requests.post(url + '/putFile', files=files, timeout=180, verify=False, cert=cert)
        logging.debug('[upload_logs] finished with code={0} msg={1}'.format(res.status_code, res.text))
        if res.status_code == 200:
            return True
    except:
        err_type, err_value = sys.exc_info()[:2]
        err_messsage = "failed to put with {0}:{1} ".format(err_type, err_value)
        err_messsage += traceback.format_exc()
        logging.debug('[upload_logs] excepted with:\n {0}'.format(err_messsage))

    return False


def contact_harvester(harvester_frontend, data, auth_token, proxy_path):
    try:
        headers = {'Content-Type': 'application/json',
                   'Authorization': 'Bearer {0}'.format(auth_token)}
        cert = [proxy_path, proxy_path]
        #verify = '/etc/grid-security/certificates' # not supported in CernVM - requests.exceptions.SSLError: [Errno 21] Is a directory
        verify = False
        resp = requests.post(harvester_frontend, json=data, headers=headers, cert=cert, verify=verify)
        logging.debug('[contact_harvester] harvester returned: {0}'.format(resp.text))
    except Exception as e:
        # message could not be sent
        logging.debug('[contact_harvester] failed to send message to harvester: {0}'.format(e))
        pass


def heartbeat(harvester_frontend, worker_id, auth_token, proxy_path):
    data = {'methodName': 'heartbeat', 'workerID': worker_id, 'data': None}
    logging.debug('[heartbeat] sending heartbeat to harvester: {0}'.format(data))
    return contact_harvester(harvester_frontend, data, auth_token, proxy_path)


def suicide(harvester_frontend, worker_id, auth_token, proxy_path):
    data = {'methodName': 'killWorker', 'workerID': worker_id, 'data': None}
    logging.debug('[suicide] sending suicide message to harvester: {0}'.format(data))
    return contact_harvester(harvester_frontend, data, auth_token, proxy_path)


def heartbeat_loop(harvester_frontend, worker_id, auth_token, proxy_path):
    while loop:
        heartbeat(harvester_frontend, worker_id, auth_token, proxy_path)
        time.sleep(300)


def get_url(url, headers=None):
    """
    get content from specified URL
    """

    reply = requests.get(url, headers=headers)
    if reply.status_code != 200:
        logging.debug('[get_attribute] Failed to open {0}'.format(url))
        return None
    else:
        return reply.content


def get_configuration():

    # get the proxy certificate and save it
    proxy_path = "/tmp/x509up"
    proxy_string = os.environ.get('proxyContent').replace(",", "\n")
    with open(proxy_path, "w") as proxy_file:
        proxy_file.write(proxy_string)
    os.chmod(proxy_path, 0o600)
    os.environ['X509_USER_PROXY'] = proxy_path
    logging.debug('[main] initialized proxy')

    # get the panda site name
    panda_site = os.environ.get('computingSite')
    logging.debug('[main] got panda site: {0}'.format(panda_site))

    # get the panda queue name
    panda_queue = os.environ.get('pandaQueueName')
    logging.debug('[main] got panda queue: {0}'.format(panda_queue))

    # get the harvester frontend URL, where we'll send heartbeats
#    harvester_frontend_url = METADATA_URL.format("harvester_frontend")
    harvester_frontend = None
#    logging.debug('[main] got harvester frontend: {0}'.format(harvester_frontend))

    # get the worker id
    worker_id = os.environ.get('workerID')
    logging.debug('[main] got worker id: {0}'.format(worker_id))

    # get the authentication token
#    auth_token_url = METADATA_URL.format("auth_token")
    auth_token = None
#    logging.debug('[main] got authentication token')

    # get the URL (e.g. panda cache) to upload logs
    logs_frontend_w = os.environ.get('logs_frontend_w')
    logging.debug('[main] got url to upload logs')

    # get the URL (e.g. panda cache) where the logs can be downloaded afterwards
    logs_frontend_r = os.environ.get('logs_frontend_r')
    logging.debug('[main] got url to download logs')

    return proxy_path, panda_site, panda_queue, harvester_frontend, worker_id, auth_token, logs_frontend_w, logs_frontend_r


if __name__ == "__main__":

    # get all the configuration from the GCE metadata server
    proxy_path, panda_site, panda_queue, harvester_frontend, worker_id, auth_token, logs_frontend_w, logs_frontend_r = get_configuration()

    # start a separate thread that will send a heartbeat to harvester every 5 minutes
#    heartbeat_thread = Thread(target=heartbeat_loop, args=(harvester_frontend, worker_id, auth_token, proxy_path))
#    heartbeat_thread.start()

    # the pilot should propagate the download link via the pilotId field in the job table
    destination_name = '{0}.log'.format(worker_id)
    log_download_url = '{0}/{1}'.format(logs_frontend_r, destination_name)
    os.environ['GTAG'] = log_download_url # GTAG env variable is read by pilot

    # get the pilot wrapper
    wrapper_path = "/tmp/runpilot3-wrapper.sh"
    wrapper_url = "https://raw.githubusercontent.com/fbarreir/adc/master/runpilot3-wrapper.sh"
    wrapper_string = get_url(wrapper_url)
    with open(wrapper_path, "w") as wrapper_file:
        wrapper_file.write(wrapper_string)
    os.chmod(wrapper_path, 0o544) # make pilot wrapper executable
    logging.debug('[main] downloaded pilot wrapper')

    # execute the pilot wrapper
    logging.debug('[main] starting pilot wrapper...')
    wrapper_params = '-s {0} -h {1}'.format(panda_site, panda_queue)
    if 'ANALY' in panda_queue:
        wrapper_params = '{0} -u user'.format(wrapper_params)
    else:
        wrapper_params = '{0} -u managed'.format(wrapper_params)
    command = "/tmp/runpilot3-wrapper.sh {0} -p 25443 -w https://pandaserver.cern.ch >& /tmp/wrapper-wid.log".\
        format(wrapper_params, worker_id)
    subprocess.call(command, shell=True)
    logging.debug('[main] pilot wrapper done...')

    # upload logs to e.g. panda cache or similar
    upload_logs(logs_frontend_w, '/tmp/wrapper-wid.log', destination_name, proxy_path)

    # ask harvester to kill the VM and stop the heartbeat
#    suicide(harvester_frontend, worker_id, auth_token, proxy_path)
    loop = False
#    heartbeat_thread.join()
