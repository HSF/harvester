"""
Connection to the PanDA server

"""

import ssl

try:
    # disable SNI for TLSV1_UNRECOGNIZED_NAME before importing requests
    ssl.HAS_SNI = False
except Exception:
    pass
import datetime
import json
import os
import sys
import traceback
import uuid
import zlib
from urllib.parse import urlparse

# TO BE REMOVED for python2.7
import requests.packages.urllib3

try:
    requests.packages.urllib3.disable_warnings()
except Exception:
    pass

from pandacommon.pandautils.net_utils import get_http_adapter_with_random_dns_resolution

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc import idds_utils

from .base_communicator import BaseCommunicator


# connection class
class PandaCommunicator(BaseCommunicator):
    # constructor
    def __init__(self):
        BaseCommunicator.__init__(self)
        self.useInspect = False
        if hasattr(harvester_config.pandacon, "verbose") and harvester_config.pandacon.verbose:
            self.verbose = True
            if hasattr(harvester_config.pandacon, "useInspect") and harvester_config.pandacon.useInspect is True:
                self.useInspect = True
        else:
            self.verbose = False
        if hasattr(harvester_config.pandacon, "auth_type"):
            self.auth_type = harvester_config.pandacon.auth_type
        else:
            self.auth_type = "x509"
        self.auth_token = None
        self.auth_token_last_update = None
        if hasattr(harvester_config.pandacon, "cert_file"):
            self.cert_file = harvester_config.pandacon.cert_file
        else:
            self.cert_file = None
        if hasattr(harvester_config.pandacon, "key_file"):
            self.key_file = harvester_config.pandacon.key_file
        else:
            self.key_file = None
        if hasattr(harvester_config.pandacon, "ca_cert"):
            self.ca_cert = harvester_config.pandacon.ca_cert
        else:
            self.ca_cert = False

        # multihost auth configuration
        self.multihost_auth_config = {}
        if hasattr(harvester_config.pandacon, "multihost_auth_config") and harvester_config.pandacon.multihost_auth_config:
            try:
                with open(harvester_config.pandacon.multihost_auth_config) as f:
                    self.multihost_auth_config = json.load(f)
            except Exception:
                pass

        # mapping between base URL and host
        self.base_url_host_map = {}
        # renew token
        try:
            self.renew_token()
        except Exception:
            pass

    # force token renewal
    def force_credential_renewal(self):
        """
        Unset timestamp to trigger token renewal
        """
        self.auth_token_last_update = None

    # renew token
    def renew_token(self):
        if self.auth_token_last_update is not None and core_utils.naive_utcnow() - self.auth_token_last_update < datetime.timedelta(minutes=10):
            return
        self.auth_token_last_update = core_utils.naive_utcnow()
        if hasattr(harvester_config.pandacon, "auth_token"):
            if harvester_config.pandacon.auth_token.startswith("file:"):
                with open(harvester_config.pandacon.auth_token.split(":")[-1]) as f:
                    self.auth_token = f.read()
            else:
                self.auth_token = harvester_config.pandacon.auth_token
        for config_map in self.multihost_auth_config.values():
            if "auth_token" in config_map:
                if config_map["auth_token"].startswith("file:"):
                    with open(config_map["auth_token"].split(":")[-1]) as f:
                        config_map["auth_token_str"] = f.read()
                else:
                    config_map["auth_token_str"] = config_map["auth_token"]

    # def get host-specific auth config for a base URL
    def get_host_specific_auth_config(self, base_url: str) -> tuple:
        """
        Get host-specific auth configuration for a base URL

        Args:
            base_url: base URL

        Returns:
            list: host-specific configuration: auth_type, cert_file, key_file, ca_cert, auth_token. Return the default configuration if the host is not in the multihost configuration.
        """
        # check if the base URL is already in the cache
        if base_url not in self.base_url_host_map:
            parsed_uri = urlparse(base_url)
            self.base_url_host_map[base_url] = parsed_uri.netloc
        host = self.base_url_host_map[base_url]
        if host in self.multihost_auth_config:
            return (
                self.multihost_auth_config[host].get("auth_type", self.auth_type),
                self.multihost_auth_config[host].get("cert_file", self.cert_file),
                self.multihost_auth_config[host].get("key_file", self.key_file),
                self.multihost_auth_config[host].get("ca_cert", self.ca_cert),
                self.multihost_auth_config[host].get("auth_token_str", self.auth_token),
            )
        else:
            return self.auth_type, self.cert_file, self.key_file, self.ca_cert, self.auth_token

    def request_ssl(self, method: str, path: str, data: dict = None, files: dict = None, cert: tuple[str, str] = None, base_url: str = None):
        """
        Generic HTTPS request function for GET, POST, and file uploads.

        :param method: HTTP method ("GET", "POST", or "UPLOAD"). The method defines how the data is sent.
        :param path: URL path
        :param data: Query data for GET/POST.
        :param files: Files to upload (for "UPLOAD" method).
        :param cert: SSL certificate tuple (cert_file, key_file). Defaults to None (use system default).
        :param base_url: Base URL. Defaults to None (use the default base URL).

        :return: Tuple (status, response or message).
        """

        if method not in ("GET", "POST", "UPLOAD"):
            raise ValueError(f"Unsupported method: {method}")

        try:
            tmp_log = None
            if self.verbose:
                tmp_log = self.make_logger(method_name=f"{method.lower()}_ssl")
                tmp_exec = str(uuid.uuid4())

            if base_url is None:
                # Most operations go to PanDA server, except for file uploads that go to PanDA cache
                base_url = harvester_config.pandacon.server_api_url_ssl if method != "UPLOAD" else harvester_config.pandacon.cache_api_url_ssl
            url = f"{base_url}/{path}"

            # Get authentication config
            auth_type, cert_file, key_file, ca_cert, auth_token = self.get_host_specific_auth_config(base_url)

            if self.verbose:
                tmp_log.debug(f"exec={tmp_exec} URL={url} data={data} files={files}")

            headers = {"Accept": "application/json", "Connection": "close", "Content-Type": "application/json"}
            if auth_type == "oidc":
                self.renew_token()
                cert = None
                headers["Authorization"] = f"Bearer {self.auth_token}"
                headers["Origin"] = harvester_config.pandacon.auth_origin
            else:
                if cert is None:
                    cert = (cert_file, key_file)

            session = get_http_adapter_with_random_dns_resolution()
            sw = core_utils.get_stopwatch()

            # Determine request type
            if method == "GET":
                # URL encoding
                response = session.request(method, url, data=data, headers=headers, timeout=harvester_config.pandacon.timeout, verify=ca_cert, cert=cert)
            elif method == "POST":
                # JSON encoding in body
                response = session.request(method, url, json=data, headers=headers, timeout=harvester_config.pandacon.timeout, verify=ca_cert, cert=cert)
            if method == "UPLOAD":
                # Upload files
                response = session.post(url, files=files, headers=headers, timeout=harvester_config.pandacon.timeout, verify=ca_cert, cert=cert)

            if self.verbose:
                tmp_log.debug(f"exec={tmp_exec} code={response.status_code} {sw.get_elapsed_time()}. return={response.text}")

            if response.status_code == 200:
                return True, response.json()
            else:
                err_msg = f"StatusCode={response.status_code} {response.text}"

        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            err_msg = f"failed to {method} with {err_type}:{err_value} "
            err_msg += traceback.format_exc()

        return False, err_msg

    # check server, this method is only used for testing the connectivity
    def check_panda(self):
        tmp_status, tmp_response = self.request_ssl("GET", "system/is_alive", {})
        return tmp_status, tmp_response

    # send heartbeat of harvester instance
    def is_alive(self, metadata_dictionary):
        tmp_log = self.make_logger(method_name="is_alive")
        tmp_log.debug("Start")

        # convert datetime
        for tmp_key, tmp_val in metadata_dictionary.items():
            if isinstance(tmp_val, datetime.datetime):
                tmp_val = "datetime/" + tmp_val.strftime("%Y-%m-%d %H:%M:%S.%f")
                metadata_dictionary[tmp_key] = tmp_val

        # send data
        data = {
            "harvester_id": harvester_config.master.harvester_id,
            "data": metadata_dictionary,
        }
        tmp_status, tmp_response = self.request_ssl("POST", "harvester/heartbeat", data)

        # Communication issue
        if tmp_status is False:
            tmp_str = core_utils.dump_error_message(tmp_log, tmp_response)
            tmp_log.debug(f"Done with {tmp_status} : {tmp_str}")
            return tmp_status, tmp_str

        # We see what PanDA server replied
        tmp_status = tmp_response["success"]
        tmp_str = ""
        if not tmp_status:
            tmp_str = tmp_response["message"]

        tmp_log.debug(f"Done with {tmp_status} : {tmp_str}")
        return tmp_status, tmp_str

    # get jobs
    def get_jobs(self, site_name, node_name, prod_source_label, computing_element, n_jobs, additional_criteria):
        tmp_log = self.make_logger(f"siteName={site_name}", method_name="get_jobs")
        tmp_log.debug(f"try to get {n_jobs} jobs")

        data = {
            "siteName": site_name,
            "node": node_name,
            "prodSourceLabel": prod_source_label,
            "computingElement": computing_element,
            "nJobs": n_jobs,
            "schedulerID": f"harvester-{harvester_config.master.harvester_id}",
        }
        if additional_criteria:
            for tmp_key, tmp_val in additional_criteria.items():
                data[tmp_key] = tmp_val

        # Get the jobs from PanDA server and measure the time
        sw = core_utils.get_stopwatch()
        tmp_status, tmp_response = self.request_ssl("POST", "pilot/acquire_jobs", data)
        tmp_log.debug(f"get_jobs for {n_jobs} jobs {sw.get_elapsed_time()}")

        # Communication issue
        if tmp_status is False:
            err_string = core_utils.dump_error_message(tmp_log, tmp_response)
            return [], err_string

        # Parse the response
        err_string = "OK"
        try:
            tmp_dict = tmp_response["data"]
            tmp_log.debug(f"StatusCode={tmp_dict['StatusCode']}")
            if tmp_dict["StatusCode"] == 0:
                tmp_log.debug(f"got {len(tmp_dict['jobs'])} jobs")
                return tmp_dict["jobs"], err_string

            if "errorDialog" in tmp_dict:
                err_string = tmp_dict["errorDialog"]
            else:
                err_string = f"StatusCode={tmp_dict['StatusCode']}"
            return [], err_string
        except Exception:
            err_string = core_utils.dump_error_message(tmp_log, tmp_response["message"])

        return [], err_string

    # update jobs
    def update_jobs(self, jobspec_list, id):
        sw = core_utils.get_stopwatch()
        tmp_logger = self.make_logger(f"id={id}", method_name="update_jobs")
        tmp_logger.debug(f"update {len(jobspec_list)} jobs")
        ret_list = []

        # TODO: check these work OK after the API migration
        # upload checkpoints
        for jobSpec in jobspec_list:
            if jobSpec.outFiles:
                tmp_logger.debug(f"upload {len(jobSpec.outFiles)} checkpoint files for PandaID={jobSpec.PandaID}")
            for fileSpec in jobSpec.outFiles:
                if "sourceURL" in jobSpec.jobParams:
                    tmp_status = self.upload_checkpoint(jobSpec.jobParams["sourceURL"], jobSpec.taskID, jobSpec.PandaID, fileSpec.lfn, fileSpec.path)
                    if tmp_status:
                        fileSpec.status = "done"

        # TODO: check these work OK after the API migration
        # update events
        for jobSpec in jobspec_list:
            event_ranges, event_specs = jobSpec.to_event_data(max_events=10000)
            if event_ranges != []:
                tmp_logger.debug(f"update {len(event_specs)} events for PandaID={jobSpec.PandaID}")
                tmp_ret = self.update_event_ranges(event_ranges, tmp_logger)
                if tmp_ret["StatusCode"] == 0:
                    for event_spec, ret_value in zip(event_specs, tmp_ret["Returns"]):
                        if ret_value in [True, False] and event_spec.is_final_status():
                            event_spec.subStatus = "done"

        # update jobs in bulk
        n_lookup = 100
        i_lookup = 0
        while i_lookup < len(jobspec_list):

            # break down the jobspec_list into chunks of n_lookup
            job_list = []
            jobspec_shard = jobspec_list[i_lookup : i_lookup + n_lookup]
            for job_spec in jobspec_shard:

                # pre-fill job data
                # TODO: this function needs to be reviewed, since now we changed the names
                job_dict = job_spec.get_job_attributes_for_panda()

                # basic fields
                job_dict["job_id"] = job_spec.PandaID
                job_dict["site_name"] = job_spec.computingSite
                job_dict["job_status"] = job_spec.get_status()
                job_dict["job_sub_status"] = job_spec.subStatus
                job_dict["attempt_nr"] = job_spec.attemptNr

                # change cancelled/missed to failed to be accepted by panda server
                if job_dict["job_status"] in ["cancelled", "missed"]:
                    if job_spec.is_pilot_closed():
                        job_dict["job_sub_status"] = "pilot_closed"
                    else:
                        job_dict["job_sub_status"] = job_dict["job_status"]
                    job_dict["job_status"] = "failed"

                if job_spec.startTime is not None and "startTime" not in job_dict:
                    job_dict["start_time"] = job_spec.startTime.strftime("%Y-%m-%d %H:%M:%S")
                if job_spec.endTime is not None and "endTime" not in job_dict:
                    job_dict["end_time"] = job_spec.endTime.strftime("%Y-%m-%d %H:%M:%S")

                if "core_count" not in job_dict and job_spec.nCore is not None:
                    job_dict["core_count"] = job_spec.nCore

                # for jobs in final status we upload metadata and the job output report
                if job_spec.is_final_status() and job_spec.status == job_spec.get_status():
                    if job_spec.metaData is not None:
                        job_dict["meta_data"] = json.dumps(job_spec.metaData)
                    if job_spec.outputFilesToReport is not None:
                        job_dict["job_output_report"] = job_spec.outputFilesToReport

                job_list.append(job_dict)

            # data dictionary to be sent to PanDA server
            data = {"job_list": job_list}

            tmp_status, tmp_response = self.request_ssl("POST", "pilot/update_jobs_bulk", data)
            ret_maps = None

            # Communication issue
            error_message = ""
            if tmp_status is False:
                error_message = core_utils.dump_error_message(tmp_logger, tmp_response)
            else:
                try:
                    tmp_status, ret_message, ret_maps = tmp_response["success"], tmp_response["message"], tmp_response["data"]
                    if tmp_status is False:
                        tmp_logger.error(f"updateJobsInBulk failed with {ret_message} {ret_maps}")
                        ret_maps = None
                        error_message = ret_message
                except Exception:
                    error_message = core_utils.dump_error_message(tmp_logger)

            # make a default map when the jobs could not be updated
            if ret_maps is None:
                ret_map = {"content": {}}
                ret_map["content"]["StatusCode"] = 999
                ret_map["content"]["ErrorDiag"] = error_message
                ret_maps = [json.dumps(ret_map)] * len(jobspec_shard)

            # iterate the results
            for job_spec, ret_map, job_dict in zip(jobspec_shard, ret_maps, job_list):
                tmp_log = self.make_logger(f"id={id} PandaID={job_spec.PandaID}", method_name="update_jobs")
                tmp_log.debug(f"job_dict={job_dict}")
                tmp_log.debug(f"Done with {ret_map}")
                ret_list.append(ret_map)

            i_lookup += n_lookup

        tmp_logger.debug(f"Done. Took {sw.get_elapsed_time()} seconds")
        return ret_list

    # get events
    def get_event_ranges(self, data_map, scattered, base_path):
        ret_status = False
        ret_value = dict()

        # define the chunk size
        try:
            default_chunk_size = harvester_config.pandacon.getEventsChunkSize
        except Exception:
            default_chunk_size = 5120

        for panda_id, data in data_map.items():
            # job-specific logger
            tmp_log = self.make_logger(f"PandaID={data['pandaID']}", method_name="get_event_ranges")
            if "nRanges" in data:
                n_ranges = data["nRanges"]
            else:
                n_ranges = 1

            if scattered:
                data["scattered"] = True

            if "isHPO" in data:
                is_hpo = data["isHPO"]
                del data["isHPO"]
            else:
                is_hpo = False

            if "sourceURL" in data:
                source_url = data["sourceURL"]
                del data["sourceURL"]
            else:
                source_url = None

            tmp_log.debug(f"Start n_ranges={n_ranges}")

            while n_ranges > 0:
                # use a small chunk size to avoid timeout
                chunk_size = min(default_chunk_size, n_ranges)
                data["nRanges"] = chunk_size
                tmp_status, tmp_response = self.request_ssl("POST", "event/acquire_event_ranges", data)

                # decrease the number of ranges
                n_ranges -= chunk_size

                # communication error
                if tmp_status is False:
                    core_utils.dump_error_message(tmp_log, tmp_response)
                    continue

                # communication with PanDA server was OK
                try:
                    tmp_dict = tmp_response["data"]
                    if tmp_dict["StatusCode"] == 0:
                        ret_status = True
                        ret_value.setdefault(data["pandaID"], [])

                        if not is_hpo:
                            ret_value[data["pandaID"]] += tmp_dict["eventRanges"]
                        else:
                            for event in tmp_dict["eventRanges"]:
                                event_id = event["eventRangeID"]
                                task_id = event_id.split("-")[0]
                                point_id = event_id.split("-")[3]

                                # get HP point
                                tmp_si, tmp_oi = idds_utils.get_hp_point(harvester_config.pandacon.iddsURL, task_id, point_id, tmp_log, self.verbose)
                                if tmp_si:
                                    event["hp_point"] = tmp_oi

                                    # get checkpoint
                                    if source_url:
                                        tmp_so, tmp_oo = self.download_checkpoint(source_url, task_id, data["pandaID"], point_id, base_path)
                                        if tmp_so:
                                            event["checkpoint"] = tmp_oo
                                    ret_value[data["pandaID"]].append(event)
                                else:
                                    core_utils.dump_error_message(tmp_log, tmp_oi)
                        # got empty
                        if len(tmp_dict["eventRanges"]) == 0:
                            break
                except Exception:
                    core_utils.dump_error_message(tmp_log)
                    break

            tmp_log.debug(f"Done with {ret_value}")

        return ret_status, ret_value

    # update events
    def update_event_ranges(self, event_ranges, tmp_log):
        # We are already receiving a tagged logger, no need to do a new one
        tmp_log.debug("Start update_event_ranges")

        # loop over for HPO
        for item in event_ranges:
            new_event_ranges = []
            for event in item["eventRanges"]:

                # report loss to idds
                if "loss" in event:
                    event_id = event["eventRangeID"]
                    task_id = event_id.split("-")[0]
                    point_id = event_id.split("-")[3]
                    tmp_si, tmp_oi = idds_utils.update_hp_point(harvester_config.pandacon.iddsURL, task_id, point_id, event["loss"], tmp_log, self.verbose)
                    if not tmp_si:
                        core_utils.dump_error_message(tmp_log, tmp_oi)
                        tmp_log.error(f"skip {event_id} since cannot update iDDS")
                        continue
                    else:
                        # clear checkpoint
                        if "sourceURL" in item:
                            tmp_sc, tmp_oc = self.clear_checkpoint(item["sourceURL"], task_id, point_id)
                            if not tmp_sc:
                                core_utils.dump_error_message(tmp_log, tmp_oc)
                    del event["loss"]
                new_event_ranges.append(event)
            item["eventRanges"] = new_event_ranges

        # update in panda
        data = {"event_ranges": json.dumps(event_ranges), "version": 1}
        tmp_log.debug(f"data={data}")
        tmp_status, tmp_response = self.request_ssl("POST", "event/update_event_ranges", data)
        ret_map = None

        if tmp_status is False:
            core_utils.dump_error_message(tmp_log, tmp_response)
            return {"StatusCode": 999}

        try:
            ret_map = tmp_response["data"]
        except Exception:
            core_utils.dump_error_message(tmp_log)

        if ret_map is None:
            ret_map = {"StatusCode": 999}

        tmp_log.debug(f"Done updateEventRanges with {ret_map}")
        return ret_map

    # get commands
    def get_commands(self, n_commands):
        tmp_log = self.make_logger(method_name="get_commands")
        tmp_log.debug(f"Start retrieving {n_commands} commands")

        data = {"harvester_id": harvester_config.master.harvester_id, "n_commands": n_commands}
        tmp_status, tmp_response = self.request_ssl("POST", "harvester/acquire_commands", data)

        # Communication issue
        if tmp_status is False:
            core_utils.dump_error_message(tmp_log, tmp_response)
            return []

        # Parse the response
        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")

        # Some issue on the server side
        if not tmp_success:
            core_utils.dump_error_message(tmp_log, tmp_message)
            return []

        commands = tmp_response.get("data", [])
        tmp_log.debug(f"Done.")
        return commands

    # send ACKs
    def ack_commands(self, command_ids):
        tmp_log = self.make_logger(method_name="ack_commands")
        tmp_log.debug(f"Start acknowledging {len(command_ids)} commands (command_ids={command_ids})")

        data = {"command_ids": command_ids}
        tmp_status, tmp_response = self.request_ssl("POST", "harvester/acknowledge_commands", data)

        # Communication issue
        if tmp_status is False:
            core_utils.dump_error_message(tmp_log, tmp_response)
            return False

        # Parse the response
        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")

        # Check the success flag
        if not tmp_success:
            core_utils.dump_error_message(tmp_log, tmp_message)
            return False

        return True

    # get proxy
    def get_proxy(self, voms_role, cert=None):
        ret_value = None
        ret_message = ""

        tmp_log = self.make_logger(method_name="get_proxy")
        tmp_log.debug("Start")

        data = {"role": voms_role}
        tmp_status, tmp_response = self.request_ssl("GET", "credential/get_proxy", data, cert)

        # Communication issue
        if tmp_status is False:
            core_utils.dump_error_message(tmp_log, tmp_response)
            return ret_value, tmp_response

        # Parse the response
        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")
        tmp_data = tmp_response.get("data")

        if not tmp_success:
            core_utils.dump_error_message(tmp_log, tmp_message)
            return ret_value, tmp_message

        ret_value = tmp_data["userProxy"]
        tmp_log.debug(f"Done with {ret_value}")

        return ret_value, ret_message

    # get token key
    def get_token_key(self, client_name: str) -> tuple[bool, str]:
        """
        Get a token key

        :param client_name: client name

        :return: a tuple of (status, token key or error message)
        """
        ret_value = None
        ret_message = ""

        tmp_log = self.make_logger(method_name="get_token_key")
        tmp_log.debug("Start")

        data = {"client_name": client_name}
        tmp_status, tmp_response = self.request_ssl("GET", "credential/get_token_key", data)

        # Communication issue
        if tmp_status is False:
            core_utils.dump_error_message(tmp_log, tmp_response)
            return ret_value, ret_message

        # Parse the response
        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")
        tmp_data = tmp_response.get("data")

        if not tmp_success:
            core_utils.dump_error_message(tmp_log, tmp_message)
            return ret_value, tmp_message

        ret_value = tmp_data["tokenKey"]
        tmp_log.debug(f"Done with {ret_value}")

        return ret_value, ret_message

    # get resource types
    def get_resource_types(self):
        tmp_log = self.make_logger(method_name="get_resource_types")
        tmp_log.debug("Start retrieving resource types")

        data = {}
        ret_message = ""
        ret_value = None
        tmp_status, tmp_response = self.request_ssl("GET", "metaconfig/get_resource_types", data)
        if tmp_status is False:
            core_utils.dump_error_message(tmp_log, tmp_response)
            return ret_value, ret_message

        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")
        tmp_data = tmp_response.get("data")

        if not tmp_success:
            ret_message = tmp_message
            core_utils.dump_error_message(tmp_log, ret_message)
            return ret_value, ret_message

        ret_value = tmp_data["ResourceTypes"]
        tmp_log.debug(f"Resource types: {ret_value}")

        return ret_value, ret_message

    # get job statistics
    def get_job_stats(self):
        tmp_log = self.make_logger(method_name="get_job_stats")
        tmp_log.debug("Start")

        tmp_status, tmp_response = self.request_ssl("GET", "statistics/active_job_stats_by_site", {})
        stats = {}
        ret_message = "FAILED"

        # Communication issue
        if tmp_status is False:
            core_utils.dump_error_message(tmp_log, tmp_response)
            return stats, ret_message

        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")
        stats = tmp_response.get("data")

        if not tmp_success:
            ret_message = tmp_message
            core_utils.dump_error_message(tmp_log, ret_message)
            return stats, ret_message

        return stats, "OK"

    # update workers
    def update_workers(self, workspec_list):
        tmp_log = self.make_logger(method_name="update_workers")
        tmp_log.debug("Start")
        data_list = []
        for workSpec in workspec_list:
            data_list.append(workSpec.convert_to_propagate())

        data = {
            "harvester_id": harvester_config.master.harvester_id,
            "workers": json.dumps(data_list),
        }

        tmp_log.debug(f"Update {len(data_list)} workers")
        tmp_status, tmp_response = self.request_ssl("POST", "harvester/update_workers", data)

        ret_list = None
        ret_message = "OK"

        # Communication issue
        if tmp_status is False:
            ret_message = core_utils.dump_error_message(tmp_log, tmp_response)
            return ret_list, ret_message

        # Parse the response
        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")
        tmp_data = tmp_response.get("data")

        # Update was not done correctly
        if not tmp_success:
            ret_message = core_utils.dump_error_message(tmp_log, tmp_message)
            return ret_list, ret_message

        ret_list = tmp_data
        tmp_log.debug(f"Done with {ret_message}")

        return ret_list, ret_message

    # update worker stats
    def update_worker_stats(self, site_name, stats):
        tmp_log = self.make_logger(method_name="update_worker_stats")
        tmp_log.debug("Start")

        data = {
            "harvesterID": harvester_config.master.harvester_id,
            "siteName": site_name,
            "paramsList": json.dumps(stats),
        }
        tmp_log.debug(f"update stats for {site_name}, stats: {stats}")
        tmp_status, tmp_response = self.request_ssl("POST", "harvester/report_worker_statistics", data)

        ret_status = True
        ret_message = "OK"

        # Communication issue
        if tmp_status is False:
            ret_status = tmp_status
            ret_message = core_utils.dump_error_message(tmp_log, tmp_response)
            return ret_status, ret_message

        # Parse the response
        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")
        # No need to parse the data field
        # tmp_data = tmp_response.get("data")

        # Update was not done correctly
        if not tmp_success:
            ret_status = tmp_success
            ret_message = core_utils.dump_error_message(tmp_log, tmp_message)
            return ret_status, ret_message

        tmp_log.debug(f"Done with {ret_status}:{ret_message}")

        return ret_status, ret_message

    # check jobs
    def check_jobs(self, jobspec_list):
        tmp_log = self.make_logger(method_name="check_jobs")
        tmp_log.debug("Start")

        ret_list = []

        # Chunk size for job lookup
        n_lookup = 100
        i_lookup = 0

        while i_lookup < len(jobspec_list):
            # Create job shards for lookup
            job_ids = []
            for jobSpec in jobspec_list[i_lookup : i_lookup + n_lookup]:
                job_ids.append(jobSpec.PandaID)

            i_lookup += n_lookup

            data = {"job_ids": job_ids}
            tmp_status, tmp_response = self.request_ssl("GET", "job/get_status", data)

            err_string = "OK"
            job_statuses = []

            # Communication issue
            if tmp_status is False:
                err_string = core_utils.dump_error_message(tmp_log, tmp_response)
            else:
                # Parse the response
                tmp_success = tmp_response.get("success", False)
                tmp_message = tmp_response.get("message")

                # Exception or rejection from PanDA server
                if not tmp_success:
                    err_string = core_utils.dump_error_message(tmp_log, tmp_message)

                # No need to parse the data field
                job_statuses = tmp_response.get("data")

            for idx, job_id in enumerate(job_ids):
                # We requested more jobs than we got back
                if not job_statuses or idx >= len(job_statuses):
                    ret_map = {"StatusCode": 999, "ErrorDiag": err_string}
                else:
                    ret_map = job_statuses[idx]
                    ret_map["StatusCode"] = 0
                    ret_map["ErrorDiag"] = err_string
                ret_list.append(ret_map)
                tmp_log.debug(f"Received {ret_map} for PandaID={job_id}")

        tmp_log.debug("Done")
        return ret_list

    # get key pair
    def get_key_pair(self, public_key_name, private_key_name):
        tmp_log = self.make_logger(method_name="get_key_pair")
        tmp_log.debug(f"Start for {public_key_name}:{private_key_name}")

        data = {
            "public_key_name": public_key_name,
            "private_key_name": private_key_name,
        }

        tmp_status, tmp_response = self.request_ssl("GET", "credential/get_key_pair", data)

        key_pair = None
        err_string = None

        # Communication issue
        if tmp_status is False:
            err_string = core_utils.dump_error_message(tmp_log, tmp_response)
            return key_pair, err_string

        # Parse the response
        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")

        # Issue/condition on server side
        if not tmp_success:
            err_string = core_utils.dump_error_message(tmp_log, tmp_message)
            return key_pair, err_string

        # Set the key-pair
        key_pair = tmp_response.get("data")
        tmp_log.debug(f"Got key_pair: {key_pair} err_string: {err_string}")

        return key_pair, err_string

    # upload file
    def upload_file(self, file_name, file_object, offset, read_bytes):
        tmp_log = self.make_logger(method_name="upload_file")
        tmp_log.debug(f"Start for {file_name} {offset}:{read_bytes}")
        file_object.seek(offset)
        files = {"file": (file_name, zlib.compress(file_object.read(read_bytes)))}
        tmp_status, tmp_response = self.request_ssl("UPLOAD", "file/update_jedi_log", files)

        # Communication issue
        if tmp_status is False:
            err_string = core_utils.dump_error_message(tmp_log, tmp_response)
            return tmp_status, err_string

        # Parse the response
        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")

        (tmp_log.error if not tmp_success else tmp_log.debug)(f"Done with {tmp_success}:{tmp_message}")
        return tmp_success, tmp_message

    # check event availability
    def check_event_availability(self, jobspec):
        tmp_log = self.make_logger(f"PandaID={jobspec.PandaID}", method_name="check_event_availability")
        tmp_log.debug("Start")

        data = {
            "job_id": jobspec.PandaID,
            "jobset_id": jobspec.jobsetID or jobspec.jobParams["jobsetID"],
            "task_id": jobspec.taskID,
        }

        tmp_status, tmp_response = self.request_ssl("GET", "event/get_available_event_range_count", data)

        # Communication issue
        if tmp_status is False:
            ret_message = core_utils.dump_error_message(tmp_log, tmp_response)
            return tmp_status, ret_message

        # Parse the response
        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")
        n_event_ranges = tmp_response.get("data", None)

        # Issue/condition on server side
        if not tmp_success:
            err_string = core_utils.dump_error_message(tmp_log, tmp_message)
            return tmp_success, err_string

        tmp_log.debug(f"Done with {n_event_ranges}")
        return tmp_success, n_event_ranges

    # send dialog messages
    def send_dialog_messages(self, dialog_list):
        tmp_log = self.make_logger(method_name="send_dialog_messages")
        tmp_log.debug("Start")

        data_list = []
        for diagSpec in dialog_list:
            data_list.append(diagSpec.convert_to_propagate())

        data = {"harvester_id": harvester_config.master.harvester_id, "dialogs": data_list}

        tmp_log.debug(f"Sending {len(data_list)} messages")
        tmp_status, tmp_response = self.request_ssl("POST", "harvester/add_dialogs", data)

        # Communication issue
        if tmp_status is False:
            ret_message = core_utils.dump_error_message(tmp_log, tmp_response)
            return tmp_status, ret_message

        # Parse the response
        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")

        (tmp_log.error if not tmp_success else tmp_log.debug)(f"Done with {tmp_success}:{tmp_message}")

        return tmp_success, tmp_message

    # update service metrics
    def update_service_metrics(self, service_metrics_list):
        tmp_log = self.make_logger(method_name="update_service_metrics")
        tmp_log.debug("Start")

        data = {"harvester_id": harvester_config.master.harvester_id, "metrics": service_metrics_list}

        tmp_log.debug("Updating metrics")
        tmp_status, tmp_response = self.request_ssl("POST", "harvester/update_service_metrics", data)

        # Communication issue
        if tmp_status is False:
            ret_message = core_utils.dump_error_message(tmp_log, tmp_response)
            return tmp_status, ret_message

        # Parse the response
        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")

        (tmp_log.error if not tmp_success else tmp_log.debug)(f"Done with {tmp_success}:{tmp_message}")

        return tmp_success, tmp_message

    # upload checkpoint
    def upload_checkpoint(self, base_url, task_id, panda_id, file_name, file_path):
        tmp_log = self.make_logger(f"taskID={task_id} pandaID={panda_id}", method_name="upload_checkpoint")
        tmp_log.debug(f"Start for {file_name}")

        ret_status = False

        try:
            files = {"file": (file_name, open(file_path).read())}
            tmp_status, tmp_response = self.request_ssl("UPLOAD", "file/upload_hpo_checkpoint", files, base_url=base_url)

            if tmp_status is False:  # Communication issue
                core_utils.dump_error_message(tmp_log, tmp_response)
                ret_status = tmp_status
            else:  # Parse the response
                tmp_success = tmp_response.get("success", False)
                tmp_message = tmp_response.get("message")
                (tmp_log.error if not tmp_success else tmp_log.debug)(f"Done with {tmp_success}:{tmp_message}")
                ret_status = tmp_success

            return ret_status

        except Exception:
            core_utils.dump_error_message(tmp_log)
            return ret_status

    # download checkpoint
    def download_checkpoint(self, base_url, task_id, panda_id, point_id, base_path):
        tmp_log = self.make_logger(f"taskID={task_id} pandaID={panda_id}", method_name="download_checkpoint")
        tmp_log.debug(f"Start for ID={point_id}")
        try:
            # This method doesn't go through the API. The file is downloaded directly from Apache file server
            path = f"cache/hpo_cp_{task_id}_{point_id}"
            tmp_status, tmp_response = self.request_ssl("POST", path, {}, base_url=base_url)
            file_name = None

            # Communication issue
            if tmp_status is False:
                core_utils.dump_error_message(tmp_log, tmp_response)
                return tmp_status, file_name

            # Generate a random file name
            file_name = os.path.join(base_path, str(uuid.uuid4()))
            with open(file_name, "w") as f:
                f.write(tmp_response.content)

            tmp_log.debug(f"Downloaded {file_name}")
            return tmp_status, file_name

        except Exception:
            core_utils.dump_error_message(tmp_log)
            return False, None

    # clear checkpoint
    def clear_checkpoint(self, base_url, task_id, point_id):
        tmp_log = self.make_logger(f"taskID={task_id} pointID={point_id}", method_name="clear_checkpoints")
        tmp_log.debug("Start")

        data = {"task_id": task_id, "sub_id": point_id}

        tmp_status, tmp_response = self.request_ssl("POST", "file/delete_hpo_checkpoint", data, base_url=base_url)

        # Communication issue
        if tmp_status is False:
            ret_message = core_utils.dump_error_message(tmp_log, tmp_response)
            return tmp_status, ret_message

        # Parse the response
        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")

        (tmp_log.error if not tmp_success else tmp_log.debug)(f"Done with {tmp_success}:{tmp_message}")

        return tmp_success, tmp_message

    # get the current/max worker id
    def get_max_worker_id(self):
        tmp_log = self.make_logger(method_name="get_max_worker_id")
        tmp_log.debug("Start")

        data = {"harvester_id": harvester_config.master.harvester_id}

        tmp_status, tmp_response = self.request_ssl("GET", "harvester/get_current_worker_id", data)

        # Communication issue
        if tmp_status is False:
            core_utils.dump_error_message(tmp_log, tmp_response)
            return tmp_status, None

        # Parse the response
        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")
        tmp_data = tmp_response.get("data")

        # If there was a max worker id, return it. Otherwise return the error message
        ret_value = tmp_data if tmp_data is not None else tmp_message

        (tmp_log.error if not tmp_success else tmp_log.debug)(f"Done with {tmp_success}:{tmp_message}")
        return tmp_success, ret_value

    # get worker stats from PanDA
    def get_worker_stats_from_panda(self):
        tmp_log = self.make_logger(method_name="get_worker_stats_from_panda")
        tmp_log.debug("Start")

        tmp_status, tmp_response = self.request_ssl("GET", "harvester/get_worker_statistics", {})

        # Communication issue
        if tmp_status is False:
            ret_message = core_utils.dump_error_message(tmp_log, tmp_response)
            return {}, ret_message

        # Parse the response
        tmp_success = tmp_response.get("success", False)
        tmp_message = tmp_response.get("message")
        stats = tmp_response.get("data", {})

        if not tmp_success:
            ret_message = core_utils.dump_error_message(tmp_log, tmp_message)
            return {}, ret_message

        tmp_log.debug(f"Done with {stats}")
        return stats, "OK"
