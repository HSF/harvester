"""
Work spec class

"""

import re
import os
import datetime
from future.utils import iteritems

from .spec_base import SpecBase

from pandaharvester.harvesterconfig import harvester_config


# work spec
class WorkSpec(SpecBase):
    # worker statuses
    ST_submitted = "submitted"
    ST_running = "running"
    ST_finished = "finished"
    ST_failed = "failed"
    ST_ready = "ready"
    ST_cancelled = "cancelled"
    ST_idle = "idle"
    ST_missed = "missed"
    ST_pending = "pending"

    # list of worker statuses
    ST_LIST = [ST_submitted, ST_running, ST_finished, ST_failed, ST_ready, ST_cancelled, ST_idle, ST_missed]

    # type of mapping between job and worker
    MT_NoJob = "NoJob"
    MT_OneToOne = "OneToOne"
    MT_MultiJobs = "ManyToOne"
    MT_MultiWorkers = "OneToMany"

    # events
    EV_noEvents = 0
    EV_useEvents = 1
    EV_requestEvents = 2

    # attributes
    attributesWithTypes = (
        "workerID:integer primary key",
        "batchID:text",
        "mapType:text",
        "queueName:text",
        "status:text / index",
        "hasJob:integer",
        "workParams:blob",
        "workAttributes:blob",
        "eventsRequestParams:blob",
        "eventsRequest:integer / index",
        "computingSite:text / index",
        "creationTime:timestamp",
        "submitTime:timestamp / index",
        "startTime:timestamp",
        "endTime:timestamp",
        "nCore:integer",
        "walltime:timestamp",
        "accessPoint:text",
        "modificationTime:timestamp / index",
        "lastUpdate:timestamp / index",
        "eventFeedTime:timestamp / index",
        "lockedBy:text",
        "postProcessed:integer",
        "nodeID:text",
        "minRamCount:integer",
        "maxDiskCount:integer",
        "maxWalltime:integer",
        "killTime:timestamp / index",
        "computingElement:text",
        "nJobsToReFill:integer / index",
        "logFilesToUpload:blob",
        "jobType:text",
        "resourceType:text",
        "nativeExitCode:integer",
        "nativeStatus:text",
        "diagMessage:varchar(500)",
        "nJobs:integer",
        "submissionHost:text",
        "configID:integer / index",
        "syncLevel:integer",
        "checkTime:timestamp",
        "ioIntensity:integer",
        "harvesterHost:text",
        "pilotType:text",
        "eventFeedLock:text",
        "errorCode:integer",
        "errorDiag:text",
    )

    # attributes to skip when slim reading
    skipAttrsToSlim = ("workParams", "workAttributes")

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
        object.__setattr__(self, "isNew", False)
        object.__setattr__(self, "nextLookup", False)
        object.__setattr__(self, "jobspec_list", None)
        object.__setattr__(self, "pandaid_list", None)
        object.__setattr__(self, "new_status", False)
        object.__setattr__(self, "pilot_closed", False)

    # keep state for pickle
    def __getstate__(self):
        odict = SpecBase.__getstate__(self)
        del odict["isNew"]
        del odict["new_status"]
        return odict

    # set status
    def set_status(self, value):
        # prevent reverse transition
        if value == self.ST_submitted and self.status in [self.ST_running, self.ST_idle]:
            return
        if self.status != value:
            self.trigger_propagation()
            self.new_status = True
        self.status = value
        if self.status == self.ST_running:
            self.set_start_time()
        elif self.is_final_status():
            self.set_end_time()

    # get access point
    def get_access_point(self):
        # replace placeholders
        if "$" in self.accessPoint:
            patts = re.findall("\$\{([a-zA-Z\d_.]+)\}", self.accessPoint)
            for patt in patts:
                tmpKey = "${" + patt + "}"
                tmpVar = None
                if hasattr(self, patt):
                    tmpVar = str(getattr(self, patt))
                elif patt == "harvesterID":
                    tmpVar = harvester_config.master.harvester_id
                else:
                    _match = re.search("^_workerID_((?:\d+.)*\d)$", patt)
                    if _match:
                        workerID_str = str(self.workerID)
                        digit_list = _match.group(1).split(".")
                        string_list = []
                        for _d in digit_list:
                            digit = int(_d)
                            try:
                                _n = workerID_str[(-1 - digit)]
                            except IndexError:
                                string_list.append("0")
                            else:
                                string_list.append(_n)
                        tmpVar = "".join(string_list)
                if tmpVar is not None:
                    self.accessPoint = self.accessPoint.replace(tmpKey, tmpVar)
        return self.accessPoint

    # set job spec list
    def set_jobspec_list(self, jobspec_list):
        self.jobspec_list = jobspec_list

    # get job spec list
    def get_jobspec_list(self):
        return self.jobspec_list

    # set number of jobs with jobspec list
    def set_num_jobs_with_list(self):
        if self.jobspec_list is None:
            return
        if len(self.jobspec_list) == 0:
            return
        if self.nJobs is None:
            self.nJobs = 0
        self.nJobs += len(self.jobspec_list)

    # convert worker status to job status
    def convert_to_job_status(self, status=None):
        if status is None:
            status = self.status
        if status in [self.ST_submitted, self.ST_ready]:
            jobStatus = "starting"
            jobSubStatus = status
        elif status in [self.ST_finished, self.ST_failed, self.ST_cancelled]:
            jobStatus = status
            jobSubStatus = "to_transfer"
        elif status in [self.ST_missed]:
            jobStatus = "missed"
            jobSubStatus = status
        else:
            jobStatus = "running"
            jobSubStatus = status
        return jobStatus, jobSubStatus

    # check if post processed
    def is_post_processed(self):
        return self.postProcessed == 1

    # set post processed flag
    def post_processed(self):
        self.postProcessed = 1

    # trigger next lookup
    def trigger_next_lookup(self):
        self.nextLookup = True
        self.modificationTime = datetime.datetime.utcnow() - datetime.timedelta(hours=1)

    # trigger propagation
    def trigger_propagation(self):
        self.lastUpdate = datetime.datetime.utcnow() - datetime.timedelta(hours=24)

    # disable propagation
    def disable_propagation(self):
        self.lastUpdate = None
        self.force_update("lastUpdate")

    # final status
    def is_final_status(self):
        return self.status in [self.ST_finished, self.ST_failed, self.ST_cancelled, self.ST_missed]

    # convert to propagate
    def convert_to_propagate(self):
        data = dict()
        for attr in [
            "workerID",
            "batchID",
            "queueName",
            "status",
            "computingSite",
            "nCore",
            "nodeID",
            "submitTime",
            "startTime",
            "endTime",
            "jobType",
            "resourceType",
            "nativeExitCode",
            "nativeStatus",
            "diagMessage",
            "nJobs",
            "computingElement",
            "syncLevel",
            "submissionHost",
            "harvesterHost",
            "errorCode",
        ]:
            val = getattr(self, attr)
            if val is not None:
                if isinstance(val, datetime.datetime):
                    val = "datetime/" + val.strftime("%Y-%m-%d %H:%M:%S.%f")
                data[attr] = val
        if self.errorCode not in [None, 0] and self.errorDiag not in [None, ""]:
            data["diagMessage"] = self.errorDiag
        if self.pandaid_list is not None:
            data["pandaid_list"] = self.pandaid_list
        if self.workAttributes is not None:
            for attr in ["stdOut", "stdErr", "batchLog", "jdl"]:
                if attr in self.workAttributes:
                    data[attr] = self.workAttributes[attr]
        return data

    # set start time
    def set_start_time(self, force=False):
        if self.startTime is None or force is True:
            self.startTime = datetime.datetime.utcnow()

    # set end time
    def set_end_time(self, force=False):
        if self.endTime is None or force is True:
            self.endTime = datetime.datetime.utcnow()

    # set work params
    def set_work_params(self, data):
        if data is None:
            return
        if self.workParams is None and data is not None:
            self.workParams = dict()
        for key, val in iteritems(data):
            if key not in self.workParams or self.workParams[key] != val:
                self.workParams[key] = val
                self.force_update("workParams")

    # get work params
    def get_work_params(self, name):
        if self.workParams is None or name not in self.workParams:
            return False, None
        return True, self.workParams[name]

    # check if has work params
    def has_work_params(self, name):
        if self.workParams is None or name not in self.workParams:
            return False
        return True

    # set work attributes
    def set_work_attributes(self, data):
        if data is None:
            return
        if self.workAttributes is None and data is not None:
            self.workAttributes = dict()
        for key, val in iteritems(data):
            if key not in self.workAttributes or self.workAttributes[key] != val:
                self.workAttributes[key] = val
                self.force_update("workAttributes")

    # get work attribute
    def get_work_attribute(self, name):
        if self.workAttributes is None or name not in self.workAttributes:
            return False, None
        return True, self.workAttributes[name]

    # check if has work attribute
    def has_work_attribute(self, name):
        if self.workAttributes is None or name not in self.workAttributes:
            return False
        return True

    # update log files to upload
    def update_log_files_to_upload(self, file_path, position, remote_name=None, stream_type=None):
        if self.logFilesToUpload is None:
            self.logFilesToUpload = dict()
        if stream_type is not None:
            # delete existing stream
            for tmp_file_path, tmpDict in iteritems(self.logFilesToUpload.copy()):
                if tmpDict["stream_type"] == stream_type:
                    del self.logFilesToUpload[tmp_file_path]
        if file_path not in self.logFilesToUpload:
            self.logFilesToUpload[file_path] = {"position": position, "remote_name": remote_name, "stream_type": stream_type}
            self.force_update("logFilesToUpload")
        elif self.logFilesToUpload[file_path]["position"] != position:
            self.logFilesToUpload[file_path]["position"] = position
            self.force_update("logFilesToUpload")

    # set log file
    def set_log_file(self, log_type, stream):
        if log_type == "stdout":
            keyName = "stdOut"
        elif log_type == "stderr":
            keyName = "stdErr"
        elif log_type == "jdl":
            keyName = "jdl"
        else:
            keyName = "batchLog"
        if stream.startswith("http"):
            url = stream
        else:
            remoteName = "{0}__{1}".format(harvester_config.master.harvester_id, os.path.basename(stream))
            url = "{0}/{1}".format(harvester_config.pandacon.pandaCacheURL_R, remoteName)
            # set file to periodically upload
            self.update_log_files_to_upload(stream, 0, remoteName, keyName)
        self.set_work_attributes({keyName: url})

    # get the list of log files to upload
    def get_log_files_to_upload(self):
        retList = []
        if self.logFilesToUpload is not None:
            for filePath, fileInfo in iteritems(self.logFilesToUpload):
                if not os.path.exists(filePath):
                    continue
                fileSize = os.stat(filePath).st_size
                if fileSize <= fileInfo["position"]:
                    continue
                retList.append((filePath, fileInfo["position"], fileSize - fileInfo["position"], fileInfo["remote_name"]))
        return retList

    # set dialog message
    def set_dialog_message(self, msg):
        if msg not in (None, ""):
            msg = msg[:500]
            self.diagMessage = msg

    # set pilot error
    def set_pilot_error(self, error_code, error_dialog):
        self.set_work_attributes({"pilotErrorCode": error_code, "pilotErrorDiag": error_dialog})

    # check if has pilot error
    def has_pilot_error(self):
        return self.has_work_attribute("pilotErrorCode")

    # set pilot_closed
    def set_pilot_closed(self):
        self.pilot_closed = True

    # set supplemental error
    def set_supplemental_error(self, error_code, error_diag):
        if error_code is not None:
            self.errorCode = error_code
        if error_diag not in (None, ""):
            self.errorDiag = str(error_diag)[:256]
