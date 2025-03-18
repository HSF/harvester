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
import inspect
import json
import os
import pickle
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

    # POST with http
    def post(self, path, data):
        try:
            tmp_log = None
            url = f"{harvester_config.pandacon.pandaURL}/{path}"

            if self.verbose:
                tmp_log = self.make_logger(method_name="post")
                if self.useInspect:
                    tmp_exec = inspect.stack()[1][3]
                    tmp_exec += "/"
                tmp_exec = str(uuid.uuid4())
                tmp_log.debug(f"exec={tmp_exec} URL={url} data={str(data)}")

            session = get_http_adapter_with_random_dns_resolution()
            res = session.post(url, data=data, headers={"Accept": "application/json", "Connection": "close"}, timeout=harvester_config.pandacon.timeout)

            if self.verbose:
                tmp_log.debug(f"exec={tmp_exec} code={res.status_code} return={res.text}")

            if res.status_code == 200:
                return True, res
            else:
                err_msg = f"StatusCode={res.status_code} {res.text}"
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            err_msg = f"failed to post with {err_type}:{err_value} "
            err_msg += traceback.format_exc()
        return False, err_msg

    # POST with https
    def post_ssl(self, path: str, data: dict, cert: tuple[str, str] = None, base_url: str = None, raw_mode: bool = False):
        """
        POST with https

        :param path: URL path
        :param data: query data
        :param cert: a tuple of (certificate, key) for SSL. If None, use the default certificate and key.
        :param base_url: base URL. If None, use the default base URL.
        :param raw_mode: return the raw response if True

        :return: a tuple of (status, response or message) if raw_mode is False. Otherwise, return the raw response.
        """
        try:
            tmp_log = None
            if self.verbose:
                tmp_log = self.make_logger(method_name="post_ssl")
                if self.useInspect:
                    tmp_exec = inspect.stack()[1][3]
                    tmp_exec += "/"
                tmp_exec = str(uuid.uuid4())
            if base_url is None:
                base_url = harvester_config.pandacon.pandaURLSSL
            url = f"{base_url}/{path}"
            # get auth config
            auth_type, cert_file, key_file, ca_cert, auth_token = self.get_host_specific_auth_config(base_url)
            if self.verbose:
                tmp_log.debug(f"exec={tmp_exec} URL={url} data={str(data)}")
            headers = {"Accept": "application/json", "Connection": "close"}
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
            res = session.post(url, data=data, headers=headers, timeout=harvester_config.pandacon.timeout, verify=ca_cert, cert=cert)
            if self.verbose:
                tmp_log.debug(f"exec={tmp_exec} code={res.status_code} {sw.get_elapsed_time()}. return={res.text}")
            if raw_mode:
                return res
            if res.status_code == 200:
                return True, res
            else:
                err_msg = f"StatusCode={res.status_code} {res.text}"
        except Exception:
            if raw_mode:
                raise
            err_type, err_value = sys.exc_info()[:2]
            err_msg = f"failed to post with {err_type}:{err_value} "
            err_msg += traceback.format_exc()
        return False, err_msg

    # POST a file with https
    def post_file_ssl(self, path, files, cert=None, base_url=None):
        try:
            tmp_log = None
            tmp_exec = None
            if self.verbose:
                tmp_log = self.make_logger(method_name="post_file_ssl")
                if self.useInspect:
                    tmp_exec = inspect.stack()[1][3]
                    tmp_exec += "/"
                tmp_exec = str(uuid.uuid4())
            if base_url is None:
                base_url = harvester_config.pandacon.pandaCacheURL_W
            url = f"{base_url}/{path}"
            # get auth config
            auth_type, cert_file, key_file, ca_cert, auth_token = self.get_host_specific_auth_config(base_url)
            if self.verbose:
                tmp_log.debug(f"exec={tmp_exec} URL={url} files={files['file'][0]}")
            if self.auth_type == "oidc":
                self.renew_token()
                cert = None
                headers = dict()
                headers["Authorization"] = f"Bearer {self.auth_token}"
                headers["Origin"] = harvester_config.pandacon.auth_origin
            else:
                headers = None
                if cert is None:
                    cert = (cert_file, key_file)
            session = get_http_adapter_with_random_dns_resolution()
            res = session.post(url, files=files, headers=headers, timeout=harvester_config.pandacon.timeout, verify=ca_cert, cert=cert)
            if self.verbose:
                tmp_log.debug(f"exec={tmp_exec} code={res.status_code} return={res.text}")
            if res.status_code == 200:
                return True, res
            else:
                err_msg = f"StatusCode={res.status_code} {res.text}"
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            err_msg = f"failed to put with {err_type}:{err_value} "
            err_msg += traceback.format_exc()
        return False, err_msg

    # check server
    def check_panda(self):
        tmp_status, tmp_response = self.post_ssl("isAlive", {})
        if tmp_status:
            return tmp_status, tmp_response.status_code, tmp_response.text
        else:
            return tmp_status, tmp_response

    # get jobs
    def get_jobs(self, site_name, node_name, prod_source_label, computing_element, n_jobs, additional_criteria):
        # get logger
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
        sw = core_utils.get_stopwatch()
        tmp_status, tmp_response = self.post_ssl("getJob", data)
        tmp_log.debug(f"getJob for {n_jobs} jobs {sw.get_elapsed_time()}")
        err_string = "OK"
        if tmp_status is False:
            err_string = core_utils.dump_error_message(tmp_log, tmp_response)
        else:
            try:
                tmp_dict = tmp_response.json()
                tmp_log.debug(f"StatusCode={tmp_dict['StatusCode']}")
                if tmp_dict["StatusCode"] == 0:
                    tmp_log.debug(f"got {len(tmp_dict['jobs'])} jobs")
                    return tmp_dict["jobs"], err_string
                else:
                    if "errorDialog" in tmp_dict:
                        err_string = tmp_dict["errorDialog"]
                    else:
                        err_string = f"StatusCode={tmp_dict['StatusCode']}"
                return [], err_string
            except Exception:
                err_string = core_utils.dump_error_message(tmp_log, tmp_response)
        return [], err_string

    # update jobs
    def update_jobs(self, jobspec_list, id):
        sw = core_utils.get_stopwatch()
        tmp_log_g = self.make_logger(f"id={id}", method_name="update_jobs")
        tmp_log_g.debug(f"update {len(jobspec_list)} jobs")
        ret_list = []
        # upload checkpoints
        for jobSpec in jobspec_list:
            if jobSpec.outFiles:
                tmp_log_g.debug(f"upload {len(jobSpec.outFiles)} checkpoint files for PandaID={jobSpec.PandaID}")
            for fileSpec in jobSpec.outFiles:
                if "sourceURL" in jobSpec.jobParams:
                    tmp_status = self.upload_checkpoint(jobSpec.jobParams["sourceURL"], jobSpec.taskID, jobSpec.PandaID, fileSpec.lfn, fileSpec.path)
                    if tmp_status:
                        fileSpec.status = "done"
        # update events
        for jobSpec in jobspec_list:
            event_ranges, event_specs = jobSpec.to_event_data(max_events=10000)
            if event_ranges != []:
                tmp_log_g.debug(f"update {len(event_specs)} events for PandaID={jobSpec.PandaID}")
                tmp_ret = self.update_event_ranges(event_ranges, tmp_log_g)
                if tmp_ret["StatusCode"] == 0:
                    for event_spec, ret_value in zip(event_specs, tmp_ret["Returns"]):
                        if ret_value in [True, False] and event_spec.is_final_status():
                            event_spec.subStatus = "done"
        # update jobs in bulk
        n_lookup = 100
        i_lookup = 0
        while i_lookup < len(jobspec_list):
            data_list = []
            jobSpecSubList = jobspec_list[i_lookup : i_lookup + n_lookup]
            for jobSpec in jobSpecSubList:
                data = jobSpec.get_job_attributes_for_panda()
                data["jobId"] = jobSpec.PandaID
                data["siteName"] = jobSpec.computingSite
                data["state"] = jobSpec.get_status()
                data["attemptNr"] = jobSpec.attemptNr
                data["jobSubStatus"] = jobSpec.subStatus
                # change cancelled to failed to be accepted by panda server
                if data["state"] in ["cancelled", "missed"]:
                    if jobSpec.is_pilot_closed():
                        data["jobSubStatus"] = "pilot_closed"
                    else:
                        data["jobSubStatus"] = data["state"]
                    data["state"] = "failed"
                if jobSpec.startTime is not None and "startTime" not in data:
                    data["startTime"] = jobSpec.startTime.strftime("%Y-%m-%d %H:%M:%S")
                if jobSpec.endTime is not None and "endTime" not in data:
                    data["endTime"] = jobSpec.endTime.strftime("%Y-%m-%d %H:%M:%S")
                if "coreCount" not in data and jobSpec.nCore is not None:
                    data["coreCount"] = jobSpec.nCore
                if jobSpec.is_final_status() and jobSpec.status == jobSpec.get_status():
                    if jobSpec.metaData is not None:
                        data["metaData"] = json.dumps(jobSpec.metaData)
                    if jobSpec.outputFilesToReport is not None:
                        data["xml"] = jobSpec.outputFilesToReport
                data_list.append(data)
            harvester_id = harvester_config.master.harvester_id
            tmp_data = {"jobList": json.dumps(data_list), "harvester_id": harvester_id}
            tmp_status, tmp_response = self.post_ssl("updateJobsInBulk", tmp_data)
            ret_maps = None
            err_string = ""
            if tmp_status is False:
                err_string = core_utils.dump_error_message(tmp_log_g, tmp_response)
            else:
                try:
                    tmp_status, ret_maps = tmp_response.json()
                    if tmp_status is False:
                        tmp_log_g.error(f"updateJobsInBulk failed with {ret_maps}")
                        ret_maps = None
                except Exception:
                    err_string = core_utils.dump_error_message(tmp_log_g)
            if ret_maps is None:
                ret_map = {"content": {}}
                ret_map["content"]["StatusCode"] = 999
                ret_map["content"]["ErrorDiag"] = err_string
                ret_maps = [json.dumps(ret_map)] * len(jobSpecSubList)
            for jobSpec, ret_map, data in zip(jobSpecSubList, ret_maps, data_list):
                tmp_log = self.make_logger(f"id={id} PandaID={jobSpec.PandaID}", method_name="update_jobs")
                try:
                    ret_map = json.loads(ret_map["content"])
                except Exception:
                    err_string = f"failed to json_load {str(ret_map)}"
                    ret_map = {"StatusCode": 999, "ErrorDiag": err_string}
                tmp_log.debug(f"data={str(data)}")
                tmp_log.debug(f"done with {str(ret_map)}")
                ret_list.append(ret_map)
            i_lookup += n_lookup
        tmp_log_g.debug("done" + sw.get_elapsed_time())
        return ret_list

    # get events
    def get_event_ranges(self, data_map, scattered, base_path):
        ret_status = False
        ret_value = dict()
        try:
            getEventsChunkSize = harvester_config.pandacon.getEventsChunkSize
        except Exception:
            getEventsChunkSize = 5120
        for pandaID, data in data_map.items():
            # get logger
            tmp_log = self.make_logger(f"PandaID={data['pandaID']}", method_name="get_event_ranges")
            if "nRanges" in data:
                n_ranges = data["nRanges"]
            else:
                n_ranges = 1
            if scattered:
                data["scattered"] = True
            if "isHPO" in data:
                isHPO = data["isHPO"]
                del data["isHPO"]
            else:
                isHPO = False
            if "sourceURL" in data:
                source_url = data["sourceURL"]
                del data["sourceURL"]
            else:
                source_url = None
            tmp_log.debug(f"start n_ranges={n_ranges}")
            while n_ranges > 0:
                # use a small chunk size to avoid timeout
                chunk_size = min(getEventsChunkSize, n_ranges)
                data["nRanges"] = chunk_size
                tmp_status, tmp_response = self.post_ssl("getEventRanges", data)
                if tmp_status is False:
                    core_utils.dump_error_message(tmp_log, tmp_response)
                else:
                    try:
                        tmp_dict = tmp_response.json()
                        if tmp_dict["StatusCode"] == 0:
                            ret_status = True
                            ret_value.setdefault(data["pandaID"], [])
                            if not isHPO:
                                ret_value[data["pandaID"]] += tmp_dict["eventRanges"]
                            else:
                                for event in tmp_dict["eventRanges"]:
                                    event_id = event["eventRangeID"]
                                    task_id = event_id.split("-")[0]
                                    point_id = event_id.split("-")[3]
                                    # get HP point
                                    tmpSI, tmpOI = idds_utils.get_hp_point(harvester_config.pandacon.iddsURL, task_id, point_id, tmp_log, self.verbose)
                                    if tmpSI:
                                        event["hp_point"] = tmpOI
                                        # get checkpoint
                                        if source_url:
                                            tmpSO, tmpOO = self.download_checkpoint(source_url, task_id, data["pandaID"], point_id, base_path)
                                            if tmpSO:
                                                event["checkpoint"] = tmpOO
                                        ret_value[data["pandaID"]].append(event)
                                    else:
                                        core_utils.dump_error_message(tmp_log, tmpOI)
                            # got empty
                            if len(tmp_dict["eventRanges"]) == 0:
                                break
                    except Exception:
                        core_utils.dump_error_message(tmp_log)
                        break
                n_ranges -= chunk_size
            tmp_log.debug(f"done with {str(ret_value)}")
        return ret_status, ret_value

    # update events
    def update_event_ranges(self, event_ranges, tmp_log):
        tmp_log.debug("start update_event_ranges")

        # loop over for HPO
        for item in event_ranges:
            new_event_ranges = []
            for event in item["eventRanges"]:
                # report loss to idds
                if "loss" in event:
                    event_id = event["eventRangeID"]
                    task_id = event_id.split("-")[0]
                    point_id = event_id.split("-")[3]
                    tmpSI, tmpOI = idds_utils.update_hp_point(harvester_config.pandacon.iddsURL, task_id, point_id, event["loss"], tmp_log, self.verbose)
                    if not tmpSI:
                        core_utils.dump_error_message(tmp_log, tmpOI)
                        tmp_log.error(f"skip {event_id} since cannot update iDDS")
                        continue
                    else:
                        # clear checkpoint
                        if "sourceURL" in item:
                            tmpSC, tmpOC = self.clear_checkpoint(item["sourceURL"], task_id, point_id)
                            if not tmpSC:
                                core_utils.dump_error_message(tmp_log, tmpOC)
                    del event["loss"]
                new_event_ranges.append(event)
            item["eventRanges"] = new_event_ranges
        # update in panda
        data = {"eventRanges": json.dumps(event_ranges), "version": 1}
        tmp_log.debug(f"data={str(data)}")
        tmp_status, tmp_response = self.post_ssl("updateEventRanges", data)
        ret_map = None
        if tmp_status is False:
            core_utils.dump_error_message(tmp_log, tmp_response)
        else:
            try:
                ret_map = tmp_response.json()
            except Exception:
                core_utils.dump_error_message(tmp_log)
        if ret_map is None:
            ret_map = {"StatusCode": 999}
        tmp_log.debug(f"done updateEventRanges with {str(ret_map)}")
        return ret_map

    # get commands
    def get_commands(self, n_commands):
        harvester_id = harvester_config.master.harvester_id
        tmp_log = self.make_logger(f"harvesterID={harvester_id}", method_name="get_commands")
        tmp_log.debug(f"Start retrieving {n_commands} commands")
        data = {"harvester_id": harvester_id, "n_commands": n_commands}
        tmp_stat, tmp_res = self.post_ssl("getCommands", data)
        if tmp_stat is False:
            core_utils.dump_error_message(tmp_log, tmp_res)
        else:
            try:
                tmp_dict = tmp_res.json()
                if tmp_dict["StatusCode"] == 0:
                    tmp_log.debug(f"Commands {tmp_dict['Commands']}")
                    return tmp_dict["Commands"]
                return []
            except Exception:
                core_utils.dump_error_message(tmp_log, tmp_res)
        return []

    # send ACKs
    def ack_commands(self, command_ids):
        harvester_id = harvester_config.master.harvester_id
        tmp_log = self.make_logger(f"harvesterID={harvester_id}", method_name="ack_commands")
        tmp_log.debug(f"Start acknowledging {len(command_ids)} commands (command_ids={command_ids})")
        data = {"command_ids": json.dumps(command_ids)}
        tmp_stat, tmp_res = self.post_ssl("ackCommands", data)
        if tmp_stat is False:
            core_utils.dump_error_message(tmp_log, tmp_res)
        else:
            try:
                tmp_dict = tmp_res.json()
                if tmp_dict["StatusCode"] == 0:
                    tmp_log.debug("Finished acknowledging commands")
                    return True
                return False
            except Exception:
                core_utils.dump_error_message(tmp_log, tmp_res)
        return False

    # get proxy
    def get_proxy(self, voms_role, cert=None):
        ret_value = None
        ret_msg = ""
        # get logger
        tmp_log = self.make_logger(method_name="get_proxy")
        tmp_log.debug("start")
        data = {"role": voms_role}
        tmp_status, tmp_response = self.post_ssl("getProxy", data, cert)
        if tmp_status is False:
            core_utils.dump_error_message(tmp_log, tmp_response)
        else:
            try:
                tmp_dict = tmp_response.json()
                if tmp_dict["StatusCode"] == 0:
                    ret_value = tmp_dict["userProxy"]
                else:
                    ret_msg = tmp_dict["errorDialog"]
                    core_utils.dump_error_message(tmp_log, ret_msg)
                    tmp_status = False
            except Exception:
                ret_msg = core_utils.dump_error_message(tmp_log, tmp_response)
                tmp_status = False
        if tmp_status:
            tmp_log.debug(f"done with {str(ret_value)}")
        return ret_value, ret_msg

    # get token key
    def get_token_key(self, client_name: str) -> tuple[bool, str]:
        """
        Get a token key

        :param client_name: client name

        :return: a tuple of (status, token key or error message)
        """
        ret_val = None
        ret_msg = ""
        # get logger
        tmp_log = self.make_logger(method_name="get_token_key")
        tmp_log.debug("start")
        data = {"client_name": client_name}
        tmp_stat, tmp_res = self.post_ssl("get_token_key", data)
        if tmp_stat is False:
            core_utils.dump_error_message(tmp_log, tmp_res)
        else:
            try:
                tmp_dict = tmp_res.json()
                if tmp_dict["StatusCode"] == 0:
                    ret_val = tmp_dict["tokenKey"]
                else:
                    ret_msg = tmp_dict["errorDialog"]
                    core_utils.dump_error_message(tmp_log, ret_msg)
                    tmp_stat = False
            except Exception:
                ret_msg = core_utils.dump_error_message(tmp_log, tmp_res)
                tmp_stat = False
        if tmp_stat:
            tmp_log.debug(f"done with {str(ret_val)}")
        return ret_val, ret_msg

    # get resource types
    def get_resource_types(self):
        tmp_log = self.make_logger(method_name="get_resource_types")
        tmp_log.debug("Start retrieving resource types")
        data = {}
        ret_msg = ""
        ret_val = None
        tmp_stat, tmp_res = self.post_ssl("getResourceTypes", data)
        if tmp_stat is False:
            core_utils.dump_error_message(tmp_log, tmp_res)
        else:
            try:
                tmp_dict = tmp_res.json()
                if tmp_dict["StatusCode"] == 0:
                    ret_val = tmp_dict["ResourceTypes"]
                    tmp_log.debug(f"Resource types: {ret_val}")
                else:
                    ret_msg = tmp_dict["errorDialog"]
                    core_utils.dump_error_message(tmp_log, ret_msg)
            except Exception:
                core_utils.dump_error_message(tmp_log, tmp_res)

        return ret_val, ret_msg

    # get job statistics
    def get_job_stats(self):
        tmp_log = self.make_logger(method_name="get_job_stats")
        tmp_log.debug("start")

        tmp_stat, tmp_res = self.post_ssl("getJobStatisticsPerSite", {})
        stats = {}
        if tmp_stat is False:
            ret_msg = "FAILED"
            core_utils.dump_error_message(tmp_log, tmp_res)
        else:
            try:
                stats = pickle.loads(tmp_res.content)
                ret_msg = "OK"
            except Exception:
                ret_msg = "Exception"
                core_utils.dump_error_message(tmp_log)

        return stats, ret_msg

    # update workers
    def update_workers(self, workspec_list):
        tmp_log = self.make_logger(method_name="update_workers")
        tmp_log.debug("start")
        data_list = []
        for workSpec in workspec_list:
            data_list.append(workSpec.convert_to_propagate())
        data = dict()
        data["harvesterID"] = harvester_config.master.harvester_id
        data["workers"] = json.dumps(data_list)
        tmp_log.debug(f"update {len(data_list)} workers")
        tmp_status, tmp_response = self.post_ssl("updateWorkers", data)
        ret_list = None
        err_string = "OK"
        if tmp_status is False:
            err_string = core_utils.dump_error_message(tmp_log, tmp_response)
        else:
            try:
                ret_code, ret_list = tmp_response.json()
                if not ret_code:
                    err_string = core_utils.dump_error_message(tmp_log, ret_list)
                    ret_list = None
                    tmp_status = False
            except Exception:
                err_string = core_utils.dump_error_message(tmp_log)
                tmp_log.error(f"conversion failure from {tmp_response.text}")
                tmp_status = False
        if tmp_status:
            tmp_log.debug(f"done with {err_string}")
        return ret_list, err_string

    # send heartbeat of harvester instance
    def is_alive(self, key_values):
        tmp_log = self.make_logger(method_name="is_alive")
        tmp_log.debug("start")
        # convert datetime
        for tmp_key, tmp_val in key_values.items():
            if isinstance(tmp_val, datetime.datetime):
                tmp_val = "datetime/" + tmp_val.strftime("%Y-%m-%d %H:%M:%S.%f")
                key_values[tmp_key] = tmp_val
        # send data
        data = dict()
        data["harvesterID"] = harvester_config.master.harvester_id
        data["data"] = json.dumps(key_values)
        tmp_status, tmp_response = self.post_ssl("harvesterIsAlive", data)
        ret_code = False
        if tmp_status is False:
            tmp_str = core_utils.dump_error_message(tmp_log, tmp_response)
        else:
            try:
                ret_code, tmp_str = tmp_response.json()
            except Exception:
                tmp_str = core_utils.dump_error_message(tmp_log)
                tmp_log.error(f"conversion failure from {tmp_response.text}")
                tmp_status = False
        if tmp_status:
            tmp_log.debug(f"done with {ret_code} : {tmp_str}")
        return ret_code, tmp_str

    # update worker stats
    def update_worker_stats(self, site_name, stats):
        tmp_log = self.make_logger(method_name="update_worker_stats")
        tmp_log.debug("start")
        data = dict()
        data["harvesterID"] = harvester_config.master.harvester_id
        data["siteName"] = site_name
        data["paramsList"] = json.dumps(stats)
        tmp_log.debug(f"update stats for {site_name}, stats: {stats}")
        tmp_status, tmp_response = self.post_ssl("reportWorkerStats_jobtype", data)
        err_string = "OK"
        if tmp_status is False:
            err_string = core_utils.dump_error_message(tmp_log, tmp_response)
        else:
            try:
                ret_code, ret_msg = tmp_response.json()
                if not ret_code:
                    tmp_status = False
                    err_string = core_utils.dump_error_message(tmp_log, ret_msg)
            except Exception:
                tmp_status = False
                err_string = core_utils.dump_error_message(tmp_log)
                tmp_log.error(f"conversion failure from {tmp_response.text}")
        if tmp_status:
            tmp_log.debug(f"done with {tmp_status}:{err_string}")
        return tmp_status, err_string

    # check jobs
    def check_jobs(self, jobspec_list):
        tmp_log = self.make_logger(method_name="check_jobs")
        tmp_log.debug("start")
        ret_list = []
        n_lookup = 100
        i_lookup = 0
        while i_lookup < len(jobspec_list):
            ids = []
            for jobSpec in jobspec_list[i_lookup : i_lookup + n_lookup]:
                ids.append(str(jobSpec.PandaID))
            i_lookup += n_lookup
            data = dict()
            data["ids"] = ",".join(ids)
            tmp_status, tmp_response = self.post_ssl("checkJobStatus", data)
            err_string = "OK"
            if tmp_status is False:
                err_string = core_utils.dump_error_message(tmp_log, tmp_response)
                tmp_response = None
            else:
                try:
                    tmp_response = tmp_response.json()
                except Exception:
                    tmp_response = None
                    err_string = core_utils.dump_error_message(tmp_log)
            for idx, pandaID in enumerate(ids):
                if tmp_response is None or "data" not in tmp_response or idx >= len(tmp_response["data"]):
                    ret_map = dict()
                    ret_map["StatusCode"] = 999
                    ret_map["ErrorDiag"] = err_string
                else:
                    ret_map = tmp_response["data"][idx]
                    ret_map["StatusCode"] = 0
                    ret_map["ErrorDiag"] = err_string
                ret_list.append(ret_map)
                tmp_log.debug(f"got {str(ret_map)} for PandaID={pandaID}")
        return ret_list

    # get key pair
    def get_key_pair(self, public_key_name, private_key_name):
        tmp_log = self.make_logger(method_name="get_key_pair")
        tmp_log.debug(f"start for {public_key_name}:{private_key_name}")
        data = dict()
        data["publicKeyName"] = public_key_name
        data["privateKeyName"] = private_key_name
        tmp_status, tmp_response = self.post_ssl("getKeyPair", data)
        ret_map = None
        err_string = None
        if tmp_status is False:
            err_string = core_utils.dump_error_message(tmp_log, tmp_response)
        else:
            try:
                ret_map = tmp_response.json()
                if ret_map["StatusCode"] != 0:
                    err_string = f"failed to get key with StatusCode={ret_map['StatusCode']} : {ret_map['errorDialog']}"
                    tmp_log.error(err_string)
                    ret_map = None
                else:
                    tmp_log.debug("got {0} with".format(str(ret_map), err_string))
            except Exception:
                err_string = core_utils.dump_error_message(tmp_log)
        return ret_map, err_string

    # upload file
    def upload_file(self, file_name, file_object, offset, read_bytes):
        tmp_log = self.make_logger(method_name="upload_file")
        tmp_log.debug(f"start for {file_name} {offset}:{read_bytes}")
        file_object.seek(offset)
        files = {"file": (file_name, zlib.compress(file_object.read(read_bytes)))}
        tmp_status, tmp_response = self.post_file_ssl("updateLog", files)
        if tmp_status is False:
            err_string = core_utils.dump_error_message(tmp_log, tmp_response)
        else:
            err_string = tmp_response.text
            tmp_log.debug(f"got {err_string}")
        return tmp_status, err_string

    # check event availability
    def check_event_availability(self, jobspec):
        ret_status = False
        ret_value = None
        tmp_log = self.make_logger(f"PandaID={jobspec.PandaID}", method_name="check_event_availability")
        tmp_log.debug("start")
        data = dict()
        data["taskID"] = jobspec.taskID
        data["pandaID"] = jobspec.PandaID
        if jobspec.jobsetID is None:
            data["jobsetID"] = jobspec.jobParams["jobsetID"]
        else:
            data["jobsetID"] = jobspec.jobsetID
        tmp_status, tmp_response = self.post_ssl("checkEventsAvailability", data)
        if tmp_status is False:
            core_utils.dump_error_message(tmp_log, tmp_response)
        else:
            try:
                tmp_dict = tmp_response.json()
                if tmp_dict["StatusCode"] == 0:
                    ret_status = True
                    ret_value = tmp_dict["nEventRanges"]
            except Exception:
                core_utils.dump_error_message(tmp_log, tmp_response)
        tmp_log.debug(f"done with {ret_value}")
        return ret_status, ret_value

    # send dialog messages
    def send_dialog_messages(self, dialog_list):
        tmp_log = self.make_logger(method_name="send_dialog_messages")
        tmp_log.debug("start")
        data_list = []
        for diagSpec in dialog_list:
            data_list.append(diagSpec.convert_to_propagate())
        data = dict()
        data["harvesterID"] = harvester_config.master.harvester_id
        data["dialogs"] = json.dumps(data_list)
        tmp_log.debug(f"send {len(data_list)} messages")
        tmp_status, tmp_response = self.post_ssl("addHarvesterDialogs", data)
        err_string = "OK"
        if tmp_status is False:
            err_string = core_utils.dump_error_message(tmp_log, tmp_response)
        else:
            try:
                ret_code, tmp_str = tmp_response.json()
                if not ret_code:
                    err_string = core_utils.dump_error_message(tmp_log, tmp_str)
                    tmp_status = False
            except Exception:
                err_string = core_utils.dump_error_message(tmp_log)
                tmp_log.error(f"conversion failure from {tmp_response.text}")
                tmp_status = False
        if tmp_status:
            tmp_log.debug(f"done with {err_string}")
        return tmp_status, err_string

    # update service metrics
    def update_service_metrics(self, service_metrics_list):
        tmp_log = self.make_logger(method_name="update_service_metrics")
        tmp_log.debug("start")
        data = dict()
        data["harvesterID"] = harvester_config.master.harvester_id
        data["metrics"] = json.dumps(service_metrics_list)
        tmp_log.debug("updating metrics...")
        tmp_stat, tmp_res = self.post_ssl("updateServiceMetrics", data)
        err_str = "OK"
        if tmp_stat is False:
            err_str = core_utils.dump_error_message(tmp_log, tmp_res)
        else:
            try:
                ret_code, ret_msg = tmp_res.json()
                if not ret_code:
                    tmp_stat = False
                    err_str = core_utils.dump_error_message(tmp_log, ret_msg)
            except Exception:
                tmp_stat = False
                err_str = core_utils.dump_error_message(tmp_log)
                tmp_log.error(f"conversion failure from {tmp_res.text}")
        if tmp_stat:
            tmp_log.debug(f"done with {tmp_stat}:{err_str}")
        return tmp_stat, err_str

    # upload checkpoint
    def upload_checkpoint(self, base_url, task_id, panda_id, file_name, file_path):
        tmp_log = self.make_logger(f"taskID={task_id} pandaID={panda_id}", method_name="upload_checkpoint")
        tmp_log.debug(f"start for {file_name}")
        try:
            files = {"file": (file_name, open(file_path).read())}
            tmp_status, tmp_response = self.post_file_ssl("server/panda/put_checkpoint", files, base_url=base_url)
            if tmp_status is False:
                core_utils.dump_error_message(tmp_log, tmp_response)
            else:
                tmp_log.debug(f"got {tmp_response.text}")
            return tmp_status
        except Exception:
            core_utils.dump_error_message(tmp_log)
            return False

    # download checkpoint
    def download_checkpoint(self, base_url, task_id, panda_id, point_id, base_path):
        tmp_log = self.make_logger(f"taskID={task_id} pandaID={panda_id}", method_name="download_checkpoint")
        tmp_log.debug(f"start for ID={point_id}")
        try:
            path = f"cache/hpo_cp_{task_id}_{point_id}"
            tmp_status, tmp_response = self.post_ssl(path, {}, base_url=base_url)
            file_name = None
            if tmp_status is False:
                core_utils.dump_error_message(tmp_log, tmp_response)
            else:
                file_name = os.path.join(base_path, str(uuid.uuid4()))
                with open(file_name, "w") as f:
                    f.write(tmp_response.content)
                tmp_log.debug(f"got {file_name}")
            return tmp_status, file_name
        except Exception:
            core_utils.dump_error_message(tmp_log)
            return False, None

    # clear checkpoint
    def clear_checkpoint(self, base_url, task_id, point_id):
        tmp_log = self.make_logger(f"taskID={task_id} pointID={point_id}", method_name="clear_checkpoints")
        data = dict()
        data["task_id"] = task_id
        data["sub_id"] = point_id
        tmp_log.debug("start")
        tmp_status, tmp_response = self.post_ssl("server/panda/delete_checkpoint", data, base_url=base_url)
        ret_map = None
        if tmp_status is False:
            core_utils.dump_error_message(tmp_log, tmp_response)
        else:
            try:
                ret_map = tmp_response.json()
            except Exception:
                core_utils.dump_error_message(tmp_log)
        if ret_map is None:
            ret_map = {"StatusCode": 999}
        tmp_log.debug(f"done with {str(ret_map)}")
        return ret_map

    # check event availability
    def get_max_worker_id(self):
        tmp_log = self.make_logger(method_name="get_max_worker_id")
        tmp_log.debug("start")
        data = {"harvester_id": harvester_config.master.harvester_id}
        ret_status, ret_value = self.post_ssl("get_max_worker_id", data)
        if ret_status is False:
            core_utils.dump_error_message(tmp_log, ret_value)
        else:
            try:
                ret_value = ret_value.json()
            except Exception:
                core_utils.dump_error_message(tmp_log, ret_value.text)
                ret_status = False
                ret_value = ret_value.text
        tmp_log.debug(f"done with {ret_status} {ret_value}")
        return ret_status, ret_value

    # get worker stats from PanDA
    def get_worker_stats_from_panda(self):
        tmp_log = self.make_logger(method_name="get_worker_stats_from_panda")
        tmp_log.debug("start")
        tmp_stat, tmp_res = self.post_ssl("getWorkerStats", {})
        stats = {}
        if tmp_stat is False:
            ret_msg = "FAILED"
            core_utils.dump_error_message(tmp_log, tmp_res)
        else:
            try:
                stats = tmp_res.json()
                ret_msg = "OK"
            except Exception:
                ret_msg = "Exception"
                core_utils.dump_error_message(tmp_log)
        tmp_log.debug(f"done with {ret_msg} {stats}")
        return stats, ret_msg
