#!/usr/bin/env python

"""
This script will be executed at container startup
- It will retrieve the proxy and panda queue from the environment
- It will download the pilot wrapper from github and execute it
- It will upload the pilot logs to panda cache at the end

post-multipart code was taken from: https://github.com/haiwen/webapi-examples/blob/master/python/upload-file.py
"""

import http.client as httplib  # for python 3
import logging
import mimetypes
import os
import shutil
import ssl
import subprocess
import sys
import traceback
import urllib.parse as urlparse  # for python 3

WORK_DIR = "/scratch"
CONFIG_DIR = "/scratch/jobconfig"
PJD = "pandaJobData.out"
PFC = "PoolFileCatalog_H.xml"
CONFIG_FILES = [PJD, PFC]

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)

# This is necessary in Lancium, otherwise the wrapper breaks
os.unsetenv("SINGULARITY_ENVIRONMENT")
os.unsetenv("SINGULARITY_BIND")


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

    h.putrequest("POST", selector)
    h.putheader("content-type", content_type)
    h.putheader("content-length", str(len(body)))
    h.endheaders()
    h.send(body.encode())
    response = h.getresponse()
    return response.status, response.reason


def encode_multipart_formdata(files):
    """
    files is a sequence of (name, filename, value) elements for data to be uploaded as files
    Return (content_type, body) ready for httplib.HTTP instance
    """
    BOUNDARY = "----------ThIs_Is_tHe_bouNdaRY_$"
    CRLF = "\r\n"
    L = []
    for key, filename, value in files:
        L.append("--" + BOUNDARY)
        L.append(f'Content-Disposition: form-data; name="{key}"; filename="{filename}"')
        L.append(f"Content-Type: {get_content_type(filename)}")
        L.append("")
        L.append(value)
    L.append("--" + BOUNDARY + "--")
    L.append("")
    body = CRLF.join(L)
    content_type = f"multipart/form-data; boundary={BOUNDARY}"
    return content_type, body


def get_content_type(filename):
    return mimetypes.guess_type(filename)[0] or "application/octet-stream"


def upload_logs(url, log_file_name, destination_name, proxy_cert):
    try:
        full_url = url + "/putFile"
        urlparts = urlparse.urlsplit(full_url)

        logging.debug("[upload_logs] start")
        files = [("file", destination_name, open(log_file_name).read())]
        status, reason = post_multipart(urlparts.hostname, urlparts.port, urlparts.path, files, proxy_cert)
        logging.debug(f"[upload_logs] finished with code={status} msg={reason}")
        if status == 200:
            return True
    except Exception:
        err_type, err_value = sys.exc_info()[:2]
        err_messsage = f"failed to put with {err_type}:{err_value} "
        err_messsage += traceback.format_exc()
        logging.debug(f"[upload_logs] excepted with:\n {err_messsage}")

    return False


def copy_files_in_dir(src_dir, dst_dir):
    # src_files = os.listdir(src_dir)
    for file_name in CONFIG_FILES:
        full_file_name = os.path.join(src_dir, file_name)
        shutil.copy(full_file_name, dst_dir)


def str_to_bool(input_str, default=False):
    output_str = default
    try:
        if input_str.upper() == "FALSE":
            output_str = False
        elif input_str.upper() == "TRUE":
            output_str = True
    except BaseException:
        pass
    return output_str


def get_configuration():
    # get the proxy certificate and save it
    if os.environ.get("proxySecretPath"):
        proxy_path = os.environ.get("proxySecretPath")
    else:
        logging.debug("[main] no proxy specified in env var $proxySecretPath")
        raise Exception("Found no voms proxy specified")
    os.environ["X509_USER_PROXY"] = proxy_path
    logging.debug("[main] initialized proxy")

    # get the panda site name
    panda_site = os.environ.get("computingSite")
    logging.debug(f"[main] got panda site: {panda_site}")

    # get the panda queue name
    panda_queue = os.environ.get("pandaQueueName")
    logging.debug(f"[main] got panda queue: {panda_queue}")

    # get the resource type of the worker
    resource_type = os.environ.get("resourceType")
    logging.debug(f"[main] got resource type: {resource_type}")

    prodSourceLabel = os.environ.get("prodSourceLabel")
    logging.debug(f"[main] got prodSourceLabel: {prodSourceLabel}")

    job_type = os.environ.get("jobType")
    logging.debug(f"[main] got job type: {job_type}")

    pilot_type = os.environ.get("pilotType", "")
    logging.debug(f"[main] got pilotType: {pilot_type}")

    pilot_url_option = os.environ.get("pilotUrlOpt", "")
    logging.debug(f"[main] got pilotUrlOpt: {pilot_url_option}")

    python_option = os.environ.get("pythonOption", "")
    logging.debug(f"[main] got pythonOption: {python_option}")

    pilot_version = os.environ.get("pilotVersion", "")
    logging.debug(f"[main] got pilotVersion: {pilot_version}")

    pilot_proxy_check_tmp = os.environ.get("pilotProxyCheck", "False")
    pilot_proxy_check = str_to_bool(pilot_proxy_check_tmp)
    logging.debug(f"[main] got pilotProxyCheck: {pilot_proxy_check}")

    # get the Harvester ID
    harvester_id = os.environ.get("HARVESTER_ID")
    logging.debug(f"[main] got Harvester ID: {harvester_id}")

    # get the worker id
    worker_id = os.environ.get("workerID")
    logging.debug(f"[main] got worker ID: {worker_id}")

    # get the URL (e.g. panda cache) to upload logs
    logs_frontend_w = os.environ.get("logs_frontend_w")
    logging.debug("[main] got url to upload logs")

    # get the URL (e.g. panda cache) where the logs can be downloaded afterwards
    logs_frontend_r = os.environ.get("logs_frontend_r")
    logging.debug("[main] got url to download logs")

    # get the filename to use for the stdout log
    stdout_name = os.environ.get("stdout_name")
    if not stdout_name:
        stdout_name = f"{harvester_id}_{worker_id}.out"

    logging.debug("[main] got filename for the stdout log")

    # get the submission mode (push/pull) for the pilot
    submit_mode = os.environ.get("submit_mode")
    if not submit_mode:
        submit_mode = "PULL"

    # see if there is a work directory specified
    tmpdir = os.environ.get("TMPDIR")
    if tmpdir:
        global WORK_DIR
        WORK_DIR = tmpdir

    return (
        proxy_path,
        panda_site,
        panda_queue,
        resource_type,
        prodSourceLabel,
        job_type,
        pilot_type,
        pilot_url_option,
        python_option,
        pilot_proxy_check,
        pilot_version,
        harvester_id,
        worker_id,
        logs_frontend_w,
        logs_frontend_r,
        stdout_name,
        submit_mode,
    )


if __name__ == "__main__":
    # get all the configuration from environment
    (
        proxy_path,
        panda_site,
        panda_queue,
        resource_type,
        prodSourceLabel,
        job_type,
        pilot_type,
        pilot_url_opt,
        python_option,
        pilot_proxy_check,
        pilot_version,
        harvester_id,
        worker_id,
        logs_frontend_w,
        logs_frontend_r,
        destination_name,
        submit_mode,
    ) = get_configuration()

    # the pilot should propagate the download link via the pilotId field in the job table
    log_download_url = f"{logs_frontend_r}/{destination_name}"
    os.environ["GTAG"] = log_download_url  # GTAG env variable is read by pilot

    # execute the pilot wrapper
    logging.debug("[main] starting pilot wrapper...")
    resource_type_option = ""
    if resource_type:
        resource_type_option = f"--resource-type {resource_type}"

    if prodSourceLabel:
        psl_option = f"-j {prodSourceLabel}"
    else:
        psl_option = "-j managed"

    job_type_option = ""
    if job_type:
        job_type_option = f"--job-type {job_type}"

    pilot_type_option = "-i PR"
    if pilot_type:
        pilot_type_option = f"-i {pilot_type}"

    pilot_proxy_check_option = "-t"  # This disables the proxy check
    if pilot_proxy_check:
        pilot_proxy_check_option = ""  # Empty enables the proxy check (default pilot behaviour)

    pilot_version_option = "--pilotversion 2"
    if pilot_version:
        pilot_version_option = f"--pilotversion {pilot_version}"

    wrapper_params = "-q {0} -r {1} -s {2} -a {3} {4} {5} {6} {7} {8} {9} {10} {11}".format(
        panda_queue,
        panda_queue,
        panda_site,
        WORK_DIR,
        resource_type_option,
        psl_option,
        pilot_type_option,
        job_type_option,
        pilot_url_opt,
        python_option,
        pilot_version_option,
        pilot_proxy_check_option,
    )

    if submit_mode == "PUSH":
        # job configuration files need to be copied, because k8s configmap mounts as read-only file system
        # and therefore the pilot cannot execute in the same directory
        copy_files_in_dir(CONFIG_DIR, WORK_DIR)

    wrapper_executable = "/cvmfs/atlas.cern.ch/repo/sw/PandaPilotWrapper/latest/runpilot2-wrapper.sh"
    command = "/bin/bash {0} {1} -w generic --pilot-user=ATLAS --url=https://pandaserver.cern.ch -d --harvester-submit-mode={2} --allow-same-user=False | tee /tmp/wrapper-wid.log".format(
        wrapper_executable, wrapper_params, submit_mode
    )

    try:
        return_code = subprocess.call(command, shell=True)
    except BaseException:
        logging.error(traceback.format_exc())
        return_code = 1

    logging.debug(f"[main] pilot wrapper done with return code {return_code} ...")

    # upload logs to e.g. panda cache or similar
    upload_logs(logs_frontend_w, "/tmp/wrapper-wid.log", destination_name, proxy_path)
    logging.debug("[main] FINISHED")

    # Exit with the same exit code as the pilot wrapper
    sys.exit(return_code)
