"""
Connection to the PanDA server

"""
import ssl

try:
    # disable SNI for TLSV1_UNRECOGNIZED_NAME before importing requests
    ssl.HAS_SNI = False
except Exception:
    pass
import os
import sys
import json
import pickle
import zlib
import uuid
import inspect
import datetime
import traceback
from future.utils import iteritems

# TO BE REMOVED for python2.7
import requests.packages.urllib3

try:
    requests.packages.urllib3.disable_warnings()
except Exception:
    pass
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc import idds_utils
from pandacommon.pandautils.net_utils import get_http_adapter_with_random_dns_resolution

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

    # renew token
    def renew_token(self):
        if hasattr(harvester_config.pandacon, "auth_token"):
            if harvester_config.pandacon.auth_token.startswith("file:"):
                if self.auth_token_last_update is not None and datetime.datetime.utcnow() - self.auth_token_last_update < datetime.timedelta(minutes=60):
                    return
                with open(harvester_config.pandacon.auth_token.split(":")[-1]) as f:
                    self.auth_token = f.read()
                    self.auth_token_last_update = datetime.datetime.utcnow()
            else:
                if self.auth_token_last_update is None:
                    self.auth_token = harvester_config.pandacon.auth_token
                    self.auth_token_last_update = datetime.datetime.utcnow()

    # POST with http
    def post(self, path, data):
        try:
            tmpLog = None
            if self.verbose:
                tmpLog = self.make_logger(method_name="post")
                if self.useInspect:
                    tmpExec = inspect.stack()[1][3]
                    tmpExec += "/"
                tmpExec = str(uuid.uuid4())
            url = "{0}/{1}".format(harvester_config.pandacon.pandaURL, path)
            if self.verbose:
                tmpLog.debug("exec={0} URL={1} data={2}".format(tmpExec, url, str(data)))
            session = get_http_adapter_with_random_dns_resolution()
            res = session.post(url, data=data, headers={"Accept": "application/json", "Connection": "close"}, timeout=harvester_config.pandacon.timeout)
            if self.verbose:
                tmpLog.debug("exec={0} code={1} return={2}".format(tmpExec, res.status_code, res.text))
            if res.status_code == 200:
                return True, res
            else:
                errMsg = "StatusCode={0} {1}".format(res.status_code, res.text)
        except Exception:
            errType, errValue = sys.exc_info()[:2]
            errMsg = "failed to post with {0}:{1} ".format(errType, errValue)
            errMsg += traceback.format_exc()
        return False, errMsg

    # POST with https
    def post_ssl(self, path, data, cert=None, base_url=None):
        try:
            tmpLog = None
            if self.verbose:
                tmpLog = self.make_logger(method_name="post_ssl")
                if self.useInspect:
                    tmpExec = inspect.stack()[1][3]
                    tmpExec += "/"
                tmpExec = str(uuid.uuid4())
            if base_url is None:
                base_url = harvester_config.pandacon.pandaURLSSL
            url = "{0}/{1}".format(base_url, path)
            if self.verbose:
                tmpLog.debug("exec={0} URL={1} data={2}".format(tmpExec, url, str(data)))
            headers = {"Accept": "application/json", "Connection": "close"}
            if self.auth_type == "oidc":
                self.renew_token()
                cert = None
                headers["Authorization"] = "Bearer {0}".format(self.auth_token)
                headers["Origin"] = harvester_config.pandacon.auth_origin
            else:
                if cert is None:
                    cert = (harvester_config.pandacon.cert_file, harvester_config.pandacon.key_file)
            session = get_http_adapter_with_random_dns_resolution()
            sw = core_utils.get_stopwatch()
            res = session.post(url, data=data, headers=headers, timeout=harvester_config.pandacon.timeout, verify=harvester_config.pandacon.ca_cert, cert=cert)
            if self.verbose:
                tmpLog.debug("exec={0} code={1} {3}. return={2}".format(tmpExec, res.status_code, res.text, sw.get_elapsed_time()))
            if res.status_code == 200:
                return True, res
            else:
                errMsg = "StatusCode={0} {1}".format(res.status_code, res.text)
        except Exception:
            errType, errValue = sys.exc_info()[:2]
            errMsg = "failed to post with {0}:{1} ".format(errType, errValue)
            errMsg += traceback.format_exc()
        return False, errMsg

    # PUT with https
    def put_ssl(self, path, files, cert=None, base_url=None):
        try:
            tmpLog = None
            tmpExec = None
            if self.verbose:
                tmpLog = self.make_logger(method_name="put_ssl")
                if self.useInspect:
                    tmpExec = inspect.stack()[1][3]
                    tmpExec += "/"
                tmpExec = str(uuid.uuid4())
            if base_url is None:
                base_url = harvester_config.pandacon.pandaCacheURL_W
            url = "{0}/{1}".format(base_url, path)
            if self.verbose:
                tmpLog.debug("exec={0} URL={1} files={2}".format(tmpExec, url, files["file"][0]))
            if self.auth_type == "oidc":
                self.renew_token()
                cert = None
                headers = dict()
                headers["Authorization"] = "Bearer {0}".format(self.auth_token)
                headers["Origin"] = harvester_config.pandacon.auth_origin
            else:
                headers = None
                if cert is None:
                    cert = (harvester_config.pandacon.cert_file, harvester_config.pandacon.key_file)
            session = get_http_adapter_with_random_dns_resolution()
            res = session.post(
                url, files=files, headers=headers, timeout=harvester_config.pandacon.timeout, verify=harvester_config.pandacon.ca_cert, cert=cert
            )
            if self.verbose:
                tmpLog.debug("exec={0} code={1} return={2}".format(tmpExec, res.status_code, res.text))
            if res.status_code == 200:
                return True, res
            else:
                errMsg = "StatusCode={0} {1}".format(res.status_code, res.text)
        except Exception:
            errType, errValue = sys.exc_info()[:2]
            errMsg = "failed to put with {0}:{1} ".format(errType, errValue)
            errMsg += traceback.format_exc()
        return False, errMsg

    # check server
    def check_panda(self):
        tmpStat, tmpRes = self.post_ssl("isAlive", {})
        if tmpStat:
            return tmpStat, tmpRes.status_code, tmpRes.text
        else:
            return tmpStat, tmpRes

    # get jobs
    def get_jobs(self, site_name, node_name, prod_source_label, computing_element, n_jobs, additional_criteria):
        # get logger
        tmpLog = self.make_logger("siteName={0}".format(site_name), method_name="get_jobs")
        tmpLog.debug("try to get {0} jobs".format(n_jobs))
        data = {}
        data["siteName"] = site_name
        data["node"] = node_name
        data["prodSourceLabel"] = prod_source_label
        data["computingElement"] = computing_element
        data["nJobs"] = n_jobs
        data["schedulerID"] = "harvester-{0}".format(harvester_config.master.harvester_id)
        if additional_criteria is not None:
            for tmpKey, tmpVal in iteritems(additional_criteria):
                data[tmpKey] = tmpVal
        sw = core_utils.get_stopwatch()
        tmpStat, tmpRes = self.post_ssl("getJob", data)
        tmpLog.debug("getJob for {0} jobs {1}".format(n_jobs, sw.get_elapsed_time()))
        errStr = "OK"
        if tmpStat is False:
            errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                tmpDict = tmpRes.json()
                tmpLog.debug("StatusCode={0}".format(tmpDict["StatusCode"]))
                if tmpDict["StatusCode"] == 0:
                    tmpLog.debug("got {0} jobs".format(len(tmpDict["jobs"])))
                    return tmpDict["jobs"], errStr
                else:
                    if "errorDialog" in tmpDict:
                        errStr = tmpDict["errorDialog"]
                    else:
                        errStr = "StatusCode={0}".format(tmpDict["StatusCode"])
                return [], errStr
            except Exception:
                errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        return [], errStr

    # update jobs
    def update_jobs(self, jobspec_list, id):
        sw = core_utils.get_stopwatch()
        tmpLogG = self.make_logger("id={0}".format(id), method_name="update_jobs")
        tmpLogG.debug("update {0} jobs".format(len(jobspec_list)))
        retList = []
        # upload checkpoints
        for jobSpec in jobspec_list:
            if jobSpec.outFiles:
                tmpLogG.debug("upload {0} checkpoint files for PandaID={1}".format(len(jobSpec.outFiles), jobSpec.PandaID))
            for fileSpec in jobSpec.outFiles:
                if "sourceURL" in jobSpec.jobParams:
                    tmpS = self.upload_checkpoint(jobSpec.jobParams["sourceURL"], jobSpec.taskID, jobSpec.PandaID, fileSpec.lfn, fileSpec.path)
                    if tmpS:
                        fileSpec.status = "done"
        # update events
        for jobSpec in jobspec_list:
            eventRanges, eventSpecs = jobSpec.to_event_data(max_events=10000)
            if eventRanges != []:
                tmpLogG.debug("update {0} events for PandaID={1}".format(len(eventSpecs), jobSpec.PandaID))
                tmpRet = self.update_event_ranges(eventRanges, tmpLogG)
                if tmpRet["StatusCode"] == 0:
                    for eventSpec, retVal in zip(eventSpecs, tmpRet["Returns"]):
                        if retVal in [True, False] and eventSpec.is_final_status():
                            eventSpec.subStatus = "done"
        # update jobs in bulk
        nLookup = 100
        iLookup = 0
        while iLookup < len(jobspec_list):
            dataList = []
            jobSpecSubList = jobspec_list[iLookup : iLookup + nLookup]
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
                dataList.append(data)
            harvester_id = harvester_config.master.harvester_id
            tmpData = {"jobList": json.dumps(dataList), "harvester_id": harvester_id}
            tmpStat, tmpRes = self.post_ssl("updateJobsInBulk", tmpData)
            retMaps = None
            errStr = ""
            if tmpStat is False:
                errStr = core_utils.dump_error_message(tmpLogG, tmpRes)
            else:
                try:
                    tmpStat, retMaps = tmpRes.json()
                    if tmpStat is False:
                        tmpLogG.error("updateJobsInBulk failed with {0}".format(retMaps))
                        retMaps = None
                except Exception:
                    errStr = core_utils.dump_error_message(tmpLogG)
            if retMaps is None:
                retMap = {}
                retMap["content"] = {}
                retMap["content"]["StatusCode"] = 999
                retMap["content"]["ErrorDiag"] = errStr
                retMaps = [json.dumps(retMap)] * len(jobSpecSubList)
            for jobSpec, retMap, data in zip(jobSpecSubList, retMaps, dataList):
                tmpLog = self.make_logger("id={0} PandaID={1}".format(id, jobSpec.PandaID), method_name="update_jobs")
                try:
                    retMap = json.loads(retMap["content"])
                except Exception:
                    errStr = "failed to json_load {}".format(str(retMap))
                    retMap = {}
                    retMap["StatusCode"] = 999
                    retMap["ErrorDiag"] = errStr
                tmpLog.debug("data={0}".format(str(data)))
                tmpLog.debug("done with {0}".format(str(retMap)))
                retList.append(retMap)
            iLookup += nLookup
        tmpLogG.debug("done" + sw.get_elapsed_time())
        return retList

    # get events
    def get_event_ranges(self, data_map, scattered, base_path):
        retStat = False
        retVal = dict()
        try:
            getEventsChunkSize = harvester_config.pandacon.getEventsChunkSize
        except Exception:
            getEventsChunkSize = 5120
        for pandaID, data in iteritems(data_map):
            # get logger
            tmpLog = self.make_logger("PandaID={0}".format(data["pandaID"]), method_name="get_event_ranges")
            if "nRanges" in data:
                nRanges = data["nRanges"]
            else:
                nRanges = 1
            if scattered:
                data["scattered"] = True
            if "isHPO" in data:
                isHPO = data["isHPO"]
                del data["isHPO"]
            else:
                isHPO = False
            if "sourceURL" in data:
                sourceURL = data["sourceURL"]
                del data["sourceURL"]
            else:
                sourceURL = None
            tmpLog.debug("start nRanges={0}".format(nRanges))
            while nRanges > 0:
                # use a small chunk size to avoid timeout
                chunkSize = min(getEventsChunkSize, nRanges)
                data["nRanges"] = chunkSize
                tmpStat, tmpRes = self.post_ssl("getEventRanges", data)
                if tmpStat is False:
                    core_utils.dump_error_message(tmpLog, tmpRes)
                else:
                    try:
                        tmpDict = tmpRes.json()
                        if tmpDict["StatusCode"] == 0:
                            retStat = True
                            retVal.setdefault(data["pandaID"], [])
                            if not isHPO:
                                retVal[data["pandaID"]] += tmpDict["eventRanges"]
                            else:
                                for event in tmpDict["eventRanges"]:
                                    event_id = event["eventRangeID"]
                                    task_id = event_id.split("-")[0]
                                    point_id = event_id.split("-")[3]
                                    # get HP point
                                    tmpSI, tmpOI = idds_utils.get_hp_point(harvester_config.pandacon.iddsURL, task_id, point_id, tmpLog, self.verbose)
                                    if tmpSI:
                                        event["hp_point"] = tmpOI
                                        # get checkpoint
                                        if sourceURL:
                                            tmpSO, tmpOO = self.download_checkpoint(sourceURL, task_id, data["pandaID"], point_id, base_path)
                                            if tmpSO:
                                                event["checkpoint"] = tmpOO
                                        retVal[data["pandaID"]].append(event)
                                    else:
                                        core_utils.dump_error_message(tmpLog, tmpOI)
                            # got empty
                            if len(tmpDict["eventRanges"]) == 0:
                                break
                    except Exception:
                        core_utils.dump_error_message(tmpLog)
                        break
                nRanges -= chunkSize
            tmpLog.debug("done with {0}".format(str(retVal)))
        return retStat, retVal

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
                        tmp_log.error("skip {0} since cannot update iDDS".format(event_id))
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
        data = {}
        data["eventRanges"] = json.dumps(event_ranges)
        data["version"] = 1
        tmp_log.debug("data={0}".format(str(data)))
        tmpStat, tmpRes = self.post_ssl("updateEventRanges", data)
        retMap = None
        if tmpStat is False:
            core_utils.dump_error_message(tmp_log, tmpRes)
        else:
            try:
                retMap = tmpRes.json()
            except Exception:
                core_utils.dump_error_message(tmp_log)
        if retMap is None:
            retMap = {}
            retMap["StatusCode"] = 999
        tmp_log.debug("done updateEventRanges with {0}".format(str(retMap)))
        return retMap

    # get commands
    def get_commands(self, n_commands):
        harvester_id = harvester_config.master.harvester_id
        tmpLog = self.make_logger("harvesterID={0}".format(harvester_id), method_name="get_commands")
        tmpLog.debug("Start retrieving {0} commands".format(n_commands))
        data = {}
        data["harvester_id"] = harvester_id
        data["n_commands"] = n_commands
        tmp_stat, tmp_res = self.post_ssl("getCommands", data)
        if tmp_stat is False:
            core_utils.dump_error_message(tmpLog, tmp_res)
        else:
            try:
                tmp_dict = tmp_res.json()
                if tmp_dict["StatusCode"] == 0:
                    tmpLog.debug("Commands {0}".format(tmp_dict["Commands"]))
                    return tmp_dict["Commands"]
                return []
            except Exception:
                core_utils.dump_error_message(tmpLog, tmp_res)
        return []

    # send ACKs
    def ack_commands(self, command_ids):
        harvester_id = harvester_config.master.harvester_id
        tmpLog = self.make_logger("harvesterID={0}".format(harvester_id), method_name="ack_commands")
        tmpLog.debug("Start acknowledging {0} commands (command_ids={1})".format(len(command_ids), command_ids))
        data = {}
        data["command_ids"] = json.dumps(command_ids)
        tmp_stat, tmp_res = self.post_ssl("ackCommands", data)
        if tmp_stat is False:
            core_utils.dump_error_message(tmpLog, tmp_res)
        else:
            try:
                tmp_dict = tmp_res.json()
                if tmp_dict["StatusCode"] == 0:
                    tmpLog.debug("Finished acknowledging commands")
                    return True
                return False
            except Exception:
                core_utils.dump_error_message(tmpLog, tmp_res)
        return False

    # get proxy
    def get_proxy(self, voms_role, cert=None):
        retVal = None
        retMsg = ""
        # get logger
        tmpLog = self.make_logger(method_name="get_proxy")
        tmpLog.debug("start")
        data = {"role": voms_role}
        tmpStat, tmpRes = self.post_ssl("getProxy", data, cert)
        if tmpStat is False:
            core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                tmpDict = tmpRes.json()
                if tmpDict["StatusCode"] == 0:
                    retVal = tmpDict["userProxy"]
                else:
                    retMsg = tmpDict["errorDialog"]
                    core_utils.dump_error_message(tmpLog, retMsg)
                    tmpStat = False
            except Exception:
                retMsg = core_utils.dump_error_message(tmpLog, tmpRes)
                tmpStat = False
        if tmpStat:
            tmpLog.debug("done with {0}".format(str(retVal)))
        return retVal, retMsg

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
                    tmp_log.debug("Resource types: {0}".format(ret_val))
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
        tmpLog = self.make_logger(method_name="update_workers")
        tmpLog.debug("start")
        dataList = []
        for workSpec in workspec_list:
            dataList.append(workSpec.convert_to_propagate())
        data = dict()
        data["harvesterID"] = harvester_config.master.harvester_id
        data["workers"] = json.dumps(dataList)
        tmpLog.debug("update {0} workers".format(len(dataList)))
        tmpStat, tmpRes = self.post_ssl("updateWorkers", data)
        retList = None
        errStr = "OK"
        if tmpStat is False:
            errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                retCode, retList = tmpRes.json()
                if not retCode:
                    errStr = core_utils.dump_error_message(tmpLog, retList)
                    retList = None
                    tmpStat = False
            except Exception:
                errStr = core_utils.dump_error_message(tmpLog)
                tmpLog.error("conversion failure from {0}".format(tmpRes.text))
                tmpStat = False
        if tmpStat:
            tmpLog.debug("done with {0}".format(errStr))
        return retList, errStr

    # send heartbeat of harvester instance
    def is_alive(self, key_values):
        tmpLog = self.make_logger(method_name="is_alive")
        tmpLog.debug("start")
        # convert datetime
        for tmpKey, tmpVal in iteritems(key_values):
            if isinstance(tmpVal, datetime.datetime):
                tmpVal = "datetime/" + tmpVal.strftime("%Y-%m-%d %H:%M:%S.%f")
                key_values[tmpKey] = tmpVal
        # send data
        data = dict()
        data["harvesterID"] = harvester_config.master.harvester_id
        data["data"] = json.dumps(key_values)
        tmpStat, tmpRes = self.post_ssl("harvesterIsAlive", data)
        retCode = False
        if tmpStat is False:
            tmpStr = core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                retCode, tmpStr = tmpRes.json()
            except Exception:
                tmpStr = core_utils.dump_error_message(tmpLog)
                tmpLog.error("conversion failure from {0}".format(tmpRes.text))
                tmpStat = False
        if tmpStat:
            tmpLog.debug("done with {0} : {1}".format(retCode, tmpStr))
        return retCode, tmpStr

    # update worker stats
    def update_worker_stats(self, site_name, stats):
        tmpLog = self.make_logger(method_name="update_worker_stats")
        tmpLog.debug("start")
        data = dict()
        data["harvesterID"] = harvester_config.master.harvester_id
        data["siteName"] = site_name
        data["paramsList"] = json.dumps(stats)
        tmpLog.debug("update stats for {0}, stats: {1}".format(site_name, stats))
        tmpStat, tmpRes = self.post_ssl("reportWorkerStats_jobtype", data)
        errStr = "OK"
        if tmpStat is False:
            errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                retCode, retMsg = tmpRes.json()
                if not retCode:
                    tmpStat = False
                    errStr = core_utils.dump_error_message(tmpLog, retMsg)
            except Exception:
                tmpStat = False
                errStr = core_utils.dump_error_message(tmpLog)
                tmpLog.error("conversion failure from {0}".format(tmpRes.text))
        if tmpStat:
            tmpLog.debug("done with {0}:{1}".format(tmpStat, errStr))
        return tmpStat, errStr

    # check jobs
    def check_jobs(self, jobspec_list):
        tmpLog = self.make_logger(method_name="check_jobs")
        tmpLog.debug("start")
        retList = []
        nLookup = 100
        iLookup = 0
        while iLookup < len(jobspec_list):
            ids = []
            for jobSpec in jobspec_list[iLookup : iLookup + nLookup]:
                ids.append(str(jobSpec.PandaID))
            iLookup += nLookup
            data = dict()
            data["ids"] = ",".join(ids)
            tmpStat, tmpRes = self.post_ssl("checkJobStatus", data)
            errStr = "OK"
            if tmpStat is False:
                errStr = core_utils.dump_error_message(tmpLog, tmpRes)
                tmpRes = None
            else:
                try:
                    tmpRes = tmpRes.json()
                except Exception:
                    tmpRes = None
                    errStr = core_utils.dump_error_message(tmpLog)
            for idx, pandaID in enumerate(ids):
                if tmpRes is None or "data" not in tmpRes or idx >= len(tmpRes["data"]):
                    retMap = dict()
                    retMap["StatusCode"] = 999
                    retMap["ErrorDiag"] = errStr
                else:
                    retMap = tmpRes["data"][idx]
                    retMap["StatusCode"] = 0
                    retMap["ErrorDiag"] = errStr
                retList.append(retMap)
                tmpLog.debug("got {0} for PandaID={1}".format(str(retMap), pandaID))
        return retList

    # get key pair
    def get_key_pair(self, public_key_name, private_key_name):
        tmpLog = self.make_logger(method_name="get_key_pair")
        tmpLog.debug("start for {0}:{1}".format(public_key_name, private_key_name))
        data = dict()
        data["publicKeyName"] = public_key_name
        data["privateKeyName"] = private_key_name
        tmpStat, tmpRes = self.post_ssl("getKeyPair", data)
        retMap = None
        errStr = None
        if tmpStat is False:
            errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                retMap = tmpRes.json()
                if retMap["StatusCode"] != 0:
                    errStr = "failed to get key with StatusCode={0} : {1}".format(retMap["StatusCode"], retMap["errorDialog"])
                    tmpLog.error(errStr)
                    retMap = None
                else:
                    tmpLog.debug("got {0} with".format(str(retMap), errStr))
            except Exception:
                errStr = core_utils.dump_error_message(tmpLog)
        return retMap, errStr

    # upload file
    def upload_file(self, file_name, file_object, offset, read_bytes):
        tmpLog = self.make_logger(method_name="upload_file")
        tmpLog.debug("start for {0} {1}:{2}".format(file_name, offset, read_bytes))
        file_object.seek(offset)
        files = {"file": (file_name, zlib.compress(file_object.read(read_bytes)))}
        tmpStat, tmpRes = self.put_ssl("updateLog", files)
        errStr = None
        if tmpStat is False:
            errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            errStr = tmpRes.text
            tmpLog.debug("got {0}".format(errStr))
        return tmpStat, errStr

    # check event availability
    def check_event_availability(self, jobspec):
        retStat = False
        retVal = None
        tmpLog = self.make_logger("PandaID={0}".format(jobspec.PandaID), method_name="check_event_availability")
        tmpLog.debug("start")
        data = dict()
        data["taskID"] = jobspec.taskID
        data["pandaID"] = jobspec.PandaID
        if jobspec.jobsetID is None:
            data["jobsetID"] = jobspec.jobParams["jobsetID"]
        else:
            data["jobsetID"] = jobspec.jobsetID
        tmpStat, tmpRes = self.post_ssl("checkEventsAvailability", data)
        if tmpStat is False:
            core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                tmpDict = tmpRes.json()
                if tmpDict["StatusCode"] == 0:
                    retStat = True
                    retVal = tmpDict["nEventRanges"]
            except Exception:
                core_utils.dump_error_message(tmpLog, tmpRes)
        tmpLog.debug("done with {0}".format(retVal))
        return retStat, retVal

    # send dialog messages
    def send_dialog_messages(self, dialog_list):
        tmpLog = self.make_logger(method_name="send_dialog_messages")
        tmpLog.debug("start")
        dataList = []
        for diagSpec in dialog_list:
            dataList.append(diagSpec.convert_to_propagate())
        data = dict()
        data["harvesterID"] = harvester_config.master.harvester_id
        data["dialogs"] = json.dumps(dataList)
        tmpLog.debug("send {0} messages".format(len(dataList)))
        tmpStat, tmpRes = self.post_ssl("addHarvesterDialogs", data)
        errStr = "OK"
        if tmpStat is False:
            errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                retCode, tmpStr = tmpRes.json()
                if not retCode:
                    errStr = core_utils.dump_error_message(tmpLog, tmpStr)
                    tmpStat = False
            except Exception:
                errStr = core_utils.dump_error_message(tmpLog)
                tmpLog.error("conversion failure from {0}".format(tmpRes.text))
                tmpStat = False
        if tmpStat:
            tmpLog.debug("done with {0}".format(errStr))
        return tmpStat, errStr

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
                tmp_log.error("conversion failure from {0}".format(tmp_res.text))
        if tmp_stat:
            tmp_log.debug("done with {0}:{1}".format(tmp_stat, err_str))
        return tmp_stat, err_str

    # upload checkpoint
    def upload_checkpoint(self, base_url, task_id, panda_id, file_name, file_path):
        tmp_log = self.make_logger("taskID={0} pandaID={1}".format(task_id, panda_id), method_name="upload_checkpoint")
        tmp_log.debug("start for {0}".format(file_name))
        try:
            files = {"file": (file_name, open(file_path).read())}
            tmpStat, tmpRes = self.put_ssl("server/panda/put_checkpoint", files, base_url=base_url)
            if tmpStat is False:
                core_utils.dump_error_message(tmp_log, tmpRes)
            else:
                tmp_log.debug("got {0}".format(tmpRes.text))
            return tmpStat
        except Exception:
            core_utils.dump_error_message(tmp_log)
            return False

    # download checkpoint
    def download_checkpoint(self, base_url, task_id, panda_id, point_id, base_path):
        tmp_log = self.make_logger("taskID={0} pandaID={1}".format(task_id, panda_id), method_name="download_checkpoint")
        tmp_log.debug("start for ID={0}".format(point_id))
        try:
            path = "cache/hpo_cp_{0}_{1}".format(task_id, point_id)
            tmpStat, tmpRes = self.post_ssl(path, {}, base_url=base_url)
            file_name = None
            if tmpStat is False:
                core_utils.dump_error_message(tmp_log, tmpRes)
            else:
                file_name = os.path.join(base_path, str(uuid.uuid4()))
                with open(file_name, "w") as f:
                    f.write(tmpRes.content)
                tmp_log.debug("got {0}".format(file_name))
            return tmpStat, file_name
        except Exception:
            core_utils.dump_error_message(tmp_log)
            return False, None

    # clear checkpoint
    def clear_checkpoint(self, base_url, task_id, point_id):
        tmp_log = self.make_logger("taskID={0} pointID={1}".format(task_id, point_id), method_name="clear_checkpoints")
        data = dict()
        data["task_id"] = task_id
        data["sub_id"] = point_id
        tmp_log.debug("start")
        tmpStat, tmpRes = self.post_ssl("server/panda/delete_checkpoint", data, base_url=base_url)
        retMap = None
        if tmpStat is False:
            core_utils.dump_error_message(tmp_log, tmpRes)
        else:
            try:
                retMap = tmpRes.json()
            except Exception:
                core_utils.dump_error_message(tmp_log)
        if retMap is None:
            retMap = {}
            retMap["StatusCode"] = 999
        tmp_log.debug("done with {0}".format(str(retMap)))
        return retMap

    # check event availability
    def get_max_worker_id(self):
        tmpLog = self.make_logger(method_name="get_max_worker_id")
        tmpLog.debug("start")
        data = {"harvester_id": harvester_config.master.harvester_id}
        retStat, retVal = self.post_ssl("get_max_worker_id", data)
        if retStat is False:
            core_utils.dump_error_message(tmpLog, retVal)
        else:
            try:
                retVal = retVal.json()
            except Exception:
                core_utils.dump_error_message(tmpLog, retVal.text)
                retStat = False
                retVal = retVal.text
        tmpLog.debug("done with {} {}".format(retStat, retVal))
        return retStat, retVal
