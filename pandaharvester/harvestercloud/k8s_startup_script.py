#!/usr/bin/env python


########################################################
#         OBSOLETE!!! USE PILOTS_STARTER.PY
########################################################

"""
This script will be executed at container startup
- It will retrieve the proxy and panda queue from the environment
- It will download the pilot wrapper from github and execute it
- It will upload the pilot logs to panda cache at the end

post-multipart code was taken from: https://github.com/haiwen/webapi-examples/blob/master/python/upload-file.py
"""

try:
    import subprocess32 as subprocess
except Exception:
    import subprocess
import os
import sys
import logging
import traceback
import httplib
import mimetypes
import ssl
import urlparse
import urllib2

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s',
                    filename='/tmp/vm_script.log', filemode='w')


def post_multipart(host, port, selector, files, proxy_cert):
    """
    Post files to an http host as multipart/form-data.
    files is a sequence of (name, filename, value) elements for data to be uploaded as files
    Return the server's response page.
    """
    content_type, body = encode_multipart_formdata(files)

    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=proxy_cert, keyfile=proxy_cert)

    h = httplib.HTTPSConnection(host, port, context=context, timeout=180)

    h.putrequest('POST', selector)
    h.putheader('content-type', content_type)
    h.putheader('content-length', str(len(body)))
    h.endheaders()
    h.send(body)
    response = h.getresponse()
    return response.status, response.reason


def encode_multipart_formdata(files):
    """
    files is a sequence of (name, filename, value) elements for data to be uploaded as files
    Return (content_type, body) ready for httplib.HTTP instance
    """
    BOUNDARY = '----------ThIs_Is_tHe_bouNdaRY_$'
    CRLF = '\r\n'
    L = []
    for (key, filename, value) in files:
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key, filename))
        L.append('Content-Type: %s' % get_content_type(filename))
        L.append('')
        L.append(value)
    L.append('--' + BOUNDARY + '--')
    L.append('')
    body = CRLF.join(L)
    content_type = 'multipart/form-data; boundary=%s' % BOUNDARY
    return content_type, body


def get_content_type(filename):
    return mimetypes.guess_type(filename)[0] or 'application/octet-stream'


def upload_logs(url, log_file_name, destination_name, proxy_cert):
    try:
        full_url = url + '/putFile'
        urlparts = urlparse.urlsplit(full_url)

        logging.debug('[upload_logs] start')
        files = [('file', destination_name, open(log_file_name).read())]
        status, reason = post_multipart(urlparts.hostname, urlparts.port, urlparts.path, files, proxy_cert)
        logging.debug('[upload_logs] finished with code={0} msg={1}'.format(status, reason))
        if status == 200:
            return True
    except Exception:
        err_type, err_value = sys.exc_info()[:2]
        err_messsage = "failed to put with {0}:{1} ".format(err_type, err_value)
        err_messsage += traceback.format_exc()
        logging.debug('[upload_logs] excepted with:\n {0}'.format(err_messsage))

    return False


def get_url(url, headers=None):
    """
    get content from specified URL
    TODO: error handling
    """
    response = urllib2.urlopen(wrapper_url)
    content = response.read()
    return content

def get_configuration():

    # get the proxy certificate and save it
    if os.environ.get('proxySecretPath'):
        # os.symlink(os.environ.get('proxySecretPath'), proxy_path)
        proxy_path = os.environ.get('proxySecretPath')
    elif os.environ.get('proxyContent'):
        proxy_path = "/tmp/x509up"
        proxy_string = os.environ.get('proxyContent').replace(",", "\n")
        with open(proxy_path, "w") as proxy_file:
            proxy_file.write(proxy_string)
        del os.environ['proxyContent']
        os.chmod(proxy_path, 0o600)
    else:
        logging.debug('[main] no proxy specified in env var $proxySecretPath nor $proxyContent')
        raise Exception('Found no voms proxy specified')
    os.environ['X509_USER_PROXY'] = proxy_path
    logging.debug('[main] initialized proxy')

    # get the panda site name
    panda_site = os.environ.get('computingSite')
    logging.debug('[main] got panda site: {0}'.format(panda_site))

    # get the panda queue name
    panda_queue = os.environ.get('pandaQueueName')
    logging.debug('[main] got panda queue: {0}'.format(panda_queue))

    # get the resource type of the worker
    resource_type = os.environ.get('resourceType')
    logging.debug('[main] got resource type: {0}'.format(resource_type))

    # get the worker id
    worker_id = os.environ.get('workerID')
    logging.debug('[main] got worker id: {0}'.format(worker_id))

    # get the URL (e.g. panda cache) to upload logs
    logs_frontend_w = os.environ.get('logs_frontend_w')
    logging.debug('[main] got url to upload logs')

    # get the URL (e.g. panda cache) where the logs can be downloaded afterwards
    logs_frontend_r = os.environ.get('logs_frontend_r')
    logging.debug('[main] got url to download logs')

    return proxy_path, panda_site, panda_queue, resource_type, worker_id, logs_frontend_w, logs_frontend_r


if __name__ == "__main__":

    # get all the configuration from the environment
    proxy_path, panda_site, panda_queue, resource_type, worker_id, logs_frontend_w, logs_frontend_r = get_configuration()

    # the pilot should propagate the download link via the pilotId field in the job table
    destination_name = '{0}.out'.format(worker_id)
    log_download_url = '{0}/{1}'.format(logs_frontend_r, destination_name)
    os.environ['GTAG'] = log_download_url # GTAG env variable is read by pilot

    # get the pilot wrapper
    wrapper_path = "/tmp/runpilot2-wrapper.sh"
    wrapper_url = "https://raw.githubusercontent.com/PanDAWMS/pilot-wrapper/master/runpilot2-wrapper.sh"
    wrapper_string = get_url(wrapper_url)
    with open(wrapper_path, "w") as wrapper_file:
        wrapper_file.write(wrapper_string)
    os.chmod(wrapper_path, 0o544) # make pilot wrapper executable
    logging.debug('[main] downloaded pilot wrapper')

    # execute the pilot wrapper
    logging.debug('[main] starting pilot wrapper...')
    resource_type_option = ''
    if resource_type:
        resource_type_option = '--resource-type {0}'.format(resource_type)
    wrapper_params = '-s {0} -r {1} -q {2} {3}'.format(panda_site, panda_queue, panda_queue, resource_type_option)
    if 'ANALY' in panda_queue:
        wrapper_params = '{0} -j user'.format(wrapper_params)
    else:
        wrapper_params = '{0} -j managed'.format(wrapper_params)
    command = "/tmp/runpilot2-wrapper.sh {0} -i PR -w generic --pilot-user=ATLAS --url=https://pandaserver.cern.ch -d --harvester-submit-mode=PULL --allow-same-user=False >& /tmp/wrapper-wid.log".\
        format(wrapper_params, worker_id)
    subprocess.call(command, shell=True)
    logging.debug('[main] pilot wrapper done...')

    # upload logs to e.g. panda cache or similar
    upload_logs(logs_frontend_w, '/tmp/wrapper-wid.log', destination_name, proxy_path)
