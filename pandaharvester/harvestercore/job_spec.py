"""
Job spec class

"""

import copy
import json
import datetime
from past.builtins import long
from future.utils import iteritems

from .spec_base import SpecBase


class JobSpec(SpecBase):
    # has output file
    HO_noOutput = 0
    HO_hasOutput = 1
    HO_hasZipOutput = 2
    HO_hasTransfer = 3
    HO_hasPostZipOutput = 4

    # auxiliary input
    AUX_hasAuxInput = 0
    AUX_inTriggered = 1
    AUX_allTriggered = 2
    AUX_inReady = 3
    AUX_allReady = 4

    # attributes
    attributesWithTypes = (
        "PandaID:integer primary key",
        "taskID:integer / index",
        "attemptNr:integer",
        "status:text",
        "subStatus:text / index",
        "currentPriority:integer / index",
        "computingSite:text / index",
        "creationTime:timestamp",
        "modificationTime:timestamp / index",
        "stateChangeTime:timestamp",
        "startTime:timestamp",
        "endTime:timestamp",
        "nCore:integer",
        "jobParams:blob",
        "jobAttributes:blob",
        "hasOutFile:integer",
        "metaData:blob",
        "outputFilesToReport:blob",
        "lockedBy:text",
        "propagatorLock:text",
        "propagatorTime:timestamp / index",
        "preparatorTime:timestamp / index",
        "submitterTime:timestamp",
        "stagerLock:text",
        "stagerTime:timestamp / index",
        "zipPerMB:integer",
        "nWorkers:integer",
        "nWorkersLimit:integer",
        "submissionAttempts:integer",
        "jobsetID:integer",
        "pilotClosed:integer",
        "configID:integer / index",
        "nRemainingEvents:integer",
        "moreWorkers:integer",
        "maxWorkersInTotal:integer",
        "nWorkersInTotal:integer",
        "jobParamsExtForOutput:blob",
        "jobParamsExtForLog:blob",
        "auxInput:integer",
    )

    # attributes initialized with 0
    zeroAttrs = ("nWorkers", "submissionAttempts", "nWorkersInTotal")

    # attributes to skip when slim reading
    skipAttrsToSlim = "jobParams"

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
        object.__setattr__(self, "events", set())
        object.__setattr__(self, "zipEventMap", {})
        object.__setattr__(self, "inFiles", set())
        object.__setattr__(self, "outFiles", set())
        object.__setattr__(self, "zipFileMap", {})
        object.__setattr__(self, "workspec_list", [])

    # add file
    def add_file(self, filespec):
        if filespec.fileType == "input":
            self.add_in_file(filespec)
        else:
            self.add_out_file(filespec)

    # add input file
    def add_in_file(self, filespec):
        self.inFiles.add(filespec)

    # add output file
    def add_out_file(self, filespec):
        self.outFiles.add(filespec)

    # reset output file list
    def reset_out_file(self):
        self.outFiles.clear()

    # get files to delete
    def get_files_to_delete(self):
        files = []
        for fileSpec in self.inFiles.union(self.outFiles):
            if fileSpec.todelete == 1:
                files.append(fileSpec)
        return files

    # add event
    def add_event(self, event_spec, zip_filespec):
        if zip_filespec is None:
            zipFileID = None
        else:
            zipFileID = zip_filespec.fileID
        if zipFileID not in self.zipEventMap:
            self.zipEventMap[zipFileID] = {"events": set(), "zip": zip_filespec}
        self.zipEventMap[zipFileID]["events"].add(event_spec)
        self.events.add(event_spec)

    # convert from Job JSON
    def convert_job_json(self, data):
        # decode secrets
        try:
            if "secrets" in data:
                data["secrets"] = json.loads(data["secrets"])
        except Exception:
            pass
        self.PandaID = data["PandaID"]
        if data["taskID"] == "NULL":
            self.taskID = None
        else:
            self.taskID = data["taskID"]
        self.attemptNr = data["attemptNr"]
        if data["jobsetID"] == "NULL":
            self.jobsetID = None
        else:
            self.jobsetID = data["jobsetID"]
        self.currentPriority = data["currentPriority"]
        self.jobParams = data
        self.jobParamsExtForOutput = self.get_output_file_attributes()
        self.jobParamsExtForLog = self.get_logfile_info()
        if "zipPerMB" in data:
            self.zipPerMB = data["zipPerMB"]

    # trigger propagation
    def trigger_propagation(self):
        self.propagatorTime = datetime.datetime.utcnow() - datetime.timedelta(hours=1)

    # trigger preparation
    def trigger_preparation(self):
        self.preparatorTime = datetime.datetime.utcnow() - datetime.timedelta(hours=1)

    # trigger stage out
    def trigger_stage_out(self):
        self.stagerTime = datetime.datetime.utcnow() - datetime.timedelta(hours=1)

    # set attributes
    def set_attributes(self, attrs):
        if attrs is None:
            return
        attrs = copy.copy(attrs)
        # set work attribute
        for attName in ["pilotErrorCode", "pilotErrorDiag", "exeErrorCode", "exeErrorDiag"]:
            if attName in attrs:
                if self.PandaID not in attrs:
                    attrs[self.PandaID] = dict()
                if attName not in attrs[self.PandaID]:
                    attrs[self.PandaID][attName] = attrs[attName]
        if self.PandaID not in attrs:
            return
        attrs = copy.copy(attrs[self.PandaID])
        # set metadata and outputs to dedicated attributes
        if "metaData" in attrs:
            self.metaData = attrs["metaData"]
            del attrs["metaData"]
        if "xml" in attrs:
            self.outputFilesToReport = attrs["xml"]
            del attrs["xml"]
        if self.jobAttributes is None:
            self.jobAttributes = attrs
        else:
            for key, val in iteritems(attrs):
                if key not in self.jobAttributes or self.jobAttributes[key] != val:
                    self.jobAttributes[key] = val
                    self.force_update("jobAttributes")

    # set one attribute
    def set_one_attribute(self, attr, value):
        if self.jobAttributes is None:
            self.jobAttributes = dict()
        if attr not in self.jobAttributes or self.jobAttributes[attr] != value:
            self.jobAttributes[attr] = value
            self.force_update("jobAttributes")

    # check if an attribute is there
    def has_attribute(self, attr):
        if self.jobAttributes is None:
            return False
        return attr in self.jobAttributes

    # get an attribute
    def get_one_attribute(self, attr):
        if self.jobAttributes and attr in self.jobAttributes:
            return self.jobAttributes[attr]
        return None

    # check if final status
    def is_final_status(self, job_status=None):
        if job_status is None:
            job_status = self.status
        return job_status in ["finished", "failed", "cancelled", "missed"]

    # get status
    def get_status(self):
        # don't report the final status while staging-out
        if self.is_final_status() and self.subStatus not in ["killed"] and (self.subStatus in ["to_transfer", "transferring"] or not self.all_events_done()):
            return "transferring"
        return self.status

    # check if all events are done
    def all_events_done(self):
        retVal = True
        for eventSpec in self.events:
            if eventSpec.subStatus != "done":
                retVal = False
                break
        return retVal

    # all files are triggered to stage-out
    def all_files_triggered_to_stage_out(self):
        for fileSpec in self.outFiles:
            if fileSpec.status not in ["finished", "failed"]:
                fileSpec.status = "transferring"
                fileSpec.attemptNr = 0

    # all files are zipped
    def all_files_zipped(self, use_post_zipping=False):
        for fileSpec in self.outFiles:
            if fileSpec.status not in ["finished", "failed"]:
                fileSpec.attemptNr = 0
                if use_post_zipping:
                    fileSpec.status = "post_zipping"
                else:
                    fileSpec.status = "defined"
                    fileSpec.groupID = None
                    fileSpec.groupStatus = None
                    fileSpec.groupUpdateTime = None

    # convert to event data
    def to_event_data(self, max_events=None):
        data = []
        eventSpecs = []
        iEvents = 0
        for zipFileID, eventsData in iteritems(self.zipEventMap):
            if max_events is not None and iEvents > max_events:
                break
            eventRanges = []
            for eventSpec in eventsData["events"]:
                eventRanges.append(eventSpec.to_data())
                eventSpecs.append(eventSpec)
                iEvents += 1
            tmpData = {}
            tmpData["eventRanges"] = eventRanges
            if "sourceURL" in self.jobParams:
                tmpData["sourceURL"] = self.jobParams["sourceURL"]
            if zipFileID is not None:
                zipFileSpec = eventsData["zip"]
                if zipFileSpec.status == "finished":
                    objstoreID = "{0}".format(zipFileSpec.objstoreID)
                    if zipFileSpec.pathConvention is not None:
                        objstoreID += "/{0}".format(zipFileSpec.pathConvention)
                    tmpData["zipFile"] = {"lfn": zipFileSpec.lfn, "objstoreID": objstoreID}
                    if zipFileSpec.fsize not in [None, 0]:
                        tmpData["zipFile"]["fsize"] = zipFileSpec.fsize
                    if zipFileSpec.chksum is not None:
                        if zipFileSpec.chksum.startswith("md:"):
                            tmpData["zipFile"]["md5"] = zipFileSpec.chksum.split(":")[-1]
                        elif zipFileSpec.chksum.startswith("ad:"):
                            tmpData["zipFile"]["adler32"] = zipFileSpec.chksum.split(":")[-1]
                        else:
                            tmpData["zipFile"]["adler32"] = zipFileSpec.chksum
            data.append(tmpData)
        return data, eventSpecs

    # get input file attributes
    def get_input_file_attributes(self, skip_ready=False):
        lfnToSkip = set()
        attemptNrMap = dict()
        pathMap = dict()
        for fileSpec in self.inFiles:
            if skip_ready and fileSpec.status == "ready":
                lfnToSkip.add(fileSpec.lfn)
            attemptNrMap[fileSpec.lfn] = fileSpec.attemptNr
            pathMap[fileSpec.lfn] = fileSpec.path
        inFiles = {}
        lfns = self.jobParams["inFiles"].split(",")
        guids = self.jobParams["GUID"].split(",")
        fsizes = self.jobParams["fsize"].split(",")
        chksums = self.jobParams["checksum"].split(",")
        scopes = self.jobParams["scopeIn"].split(",")
        datasets = self.jobParams["realDatasetsIn"].split(",")
        endpoints = self.jobParams["ddmEndPointIn"].split(",")
        for lfn, guid, fsize, chksum, scope, dataset, endpoint in zip(lfns, guids, fsizes, chksums, scopes, datasets, endpoints):
            try:
                fsize = long(fsize)
            except Exception:
                fsize = None
            if lfn in lfnToSkip:
                continue
            if lfn in attemptNrMap:
                attemptNr = attemptNrMap[lfn]
            else:
                attemptNr = 0
            inFiles[lfn] = {"fsize": fsize, "guid": guid, "checksum": chksum, "scope": scope, "dataset": dataset, "endpoint": endpoint, "attemptNr": attemptNr}
        # add path
        if "inFilePaths" in self.jobParams:
            for lfn in lfns:
                if lfn not in inFiles or lfn not in pathMap:
                    continue
                inFiles[lfn]["path"] = pathMap[lfn]
        # delete empty file
        if "" in inFiles:
            del inFiles[""]
        if "NULL" in inFiles:
            del inFiles["NULL"]
        return inFiles

    # set input file paths
    def set_input_file_paths(self, in_files):
        lfns = self.get_input_file_attributes().keys()
        paths = []
        for lfn in lfns:
            # check for consistency
            if lfn in in_files:
                paths.append(in_files[lfn]["path"])
        self.jobParams["inFilePaths"] = ",".join(paths)
        # trigger updating
        self.force_update("jobParams")
        # update file specs
        for fileSpec in self.inFiles:
            if fileSpec.lfn in in_files:
                fileSpec.path = in_files[fileSpec.lfn]["path"]

    # set ready to all input files
    def set_all_input_ready(self):
        # update file specs
        for fileSpec in self.inFiles:
            fileSpec.status = "ready"

    # get output file attributes
    def get_output_file_attributes(self):
        if self.jobParamsExtForOutput is not None:
            return self.jobParamsExtForOutput
        outFiles = {}
        lfns = self.jobParams["outFiles"].split(",")
        scopes = self.jobParams["scopeOut"].split(",")
        scopeLog = self.jobParams["scopeLog"]
        logLFN = self.jobParams["logFile"]
        scopes.insert(lfns.index(logLFN), scopeLog)
        datasets = self.jobParams["realDatasets"].split(",")
        endpoints = self.jobParams["ddmEndPointOut"].split(",")
        for lfn, scope, dataset, endpoint in zip(lfns, scopes, datasets, endpoints):
            outFiles[lfn] = {"scope": scope, "dataset": dataset, "endpoint": endpoint}
        self.jobParamsExtForOutput = outFiles
        return outFiles

    # get log file information
    def get_logfile_info(self):
        if self.jobParamsExtForLog is not None:
            return self.jobParamsExtForLog
        retMap = dict()
        retMap["lfn"] = self.jobParams["logFile"]
        retMap["guid"] = self.jobParams["logGUID"]
        self.jobParamsExtForLog = retMap
        return retMap

    # set start time
    def set_start_time(self, force=False):
        if self.startTime is None or force is True:
            self.startTime = datetime.datetime.utcnow()

    # set end time
    def set_end_time(self, force=False):
        if self.endTime is None or force is True:
            self.endTime = datetime.datetime.utcnow()

    # reset start and end time
    def reset_start_end_time(self):
        self.startTime = datetime.datetime.utcnow()
        self.endTime = self.startTime

    # add work spec list
    def add_workspec_list(self, workspec_list):
        self.workspec_list = workspec_list

    # get work spec list
    def get_workspec_list(self):
        return self.workspec_list

    # get job attributes to be reported to Panda
    def get_job_attributes_for_panda(self):
        data = dict()
        if self.jobAttributes is None:
            return data
        # extract only panda attributes
        # FIXME use set literal for python >=2.7
        panda_attributes = [
            "token",
            "transExitCode",
            "pilotErrorCode",
            "pilotErrorDiag",
            "timestamp",
            "node",
            "workdir",
            "cpuConsumptionTime",
            "cpuConsumptionUnit",
            "remainingSpace",
            "schedulerID",
            "pilotID",
            "siteName",
            "messageLevel",
            "pilotLog",
            "cpuConversionFactor",
            "exeErrorCode",
            "exeErrorDiag",
            "pilotTiming",
            "computingElement",
            "startTime",
            "endTime",
            "nEvents",
            "nInputFiles",
            "batchID",
            "attemptNr",
            "jobMetrics",
            "stdout",
            "coreCount",
            "maxRSS",
            "maxVMEM",
            "maxSWAP",
            "maxPSS",
            "avgRSS",
            "avgVMEM",
            "avgSWAP",
            "avgPSS",
            "totRCHAR",
            "totWCHAR",
            "totRBYTES",
            "totWBYTES",
            "rateRCHAR",
            "rateWCHAR",
            "rateRBYTES",
            "rateWBYTES",
        ]
        panda_attributes = set(panda_attributes)
        for aName, aValue in iteritems(self.jobAttributes):
            if aName in panda_attributes:
                if type(aValue) in (int, long):
                    aValue = str(aValue)
                data[aName] = aValue
        return data

    # get job status from attributes
    def get_job_status_from_attributes(self):
        if self.jobAttributes is None or "jobStatus" not in self.jobAttributes:
            return None
        if self.jobAttributes["jobStatus"] not in ["finished", "failed"]:
            return None
        return self.jobAttributes["jobStatus"]

    # set group to files
    def set_groups_to_files(self, id_map):
        timeNow = datetime.datetime.utcnow()
        # reverse mapping
        revMap = dict()
        for gID, items in iteritems(id_map):
            for lfn in items["lfns"]:
                revMap[lfn] = gID
        # update file specs
        for fileSpec in self.inFiles.union(self.outFiles):
            if fileSpec.lfn in revMap:
                fileSpec.groupID = revMap[fileSpec.lfn]
                fileSpec.groupStatus = id_map[fileSpec.groupID]["groupStatus"]
                fileSpec.groupUpdateTime = timeNow

    # update group status in files
    def update_group_status_in_files(self, group_id, group_status):
        timeNow = datetime.datetime.utcnow()
        # update file specs
        for fileSpec in self.inFiles.union(self.outFiles):
            if fileSpec.groupID == group_id:
                fileSpec.groupStatus = group_status
                fileSpec.groupUpdateTime = timeNow

    # get groups of input files
    def get_groups_of_input_files(self, skip_ready=False):
        groups = dict()
        for fileSpec in self.inFiles:
            if skip_ready and fileSpec.status == "ready":
                continue
            groups[fileSpec.groupID] = {"groupUpdateTime": fileSpec.groupUpdateTime, "groupStatus": fileSpec.groupStatus}
        return groups

    # get groups of output files
    def get_groups_of_output_files(self):
        groups = dict()
        for fileSpec in self.outFiles:
            groups[fileSpec.groupID] = {"groupUpdateTime": fileSpec.groupUpdateTime, "groupStatus": fileSpec.groupStatus}
        return groups

    # get output file specs
    def get_output_file_specs(self, skip_done=False):
        if not skip_done:
            return self.outFiles
        else:
            retList = []
            for fileSpec in self.outFiles:
                if fileSpec.status not in ["finished", "failed"]:
                    retList.append(fileSpec)
            return retList

    # get input file specs for a given group id
    def get_input_file_specs(self, group_id, skip_ready=False):
        retList = []
        for fileSpec in self.inFiles:
            if fileSpec.groupID == group_id:
                if skip_ready and fileSpec.status in ["ready", "failed"]:
                    continue
                retList.append(fileSpec)
        return retList

    # set pilot error
    def set_pilot_error(self, error_code, error_dialog):
        if not self.has_attribute("pilotErrorCode"):
            self.set_one_attribute("pilotErrorCode", error_code)
        if not self.has_attribute("pilotErrorDiag"):
            self.set_one_attribute("pilotErrorDiag", error_dialog)

    # not to suppress heartbeat
    def not_suppress_heartbeat(self):
        if self.subStatus in ["missed"]:
            return True
        return False

    # set pilot_closed
    def set_pilot_closed(self):
        self.pilotClosed = 1

    # check if pilot_closed
    def is_pilot_closed(self):
        return self.pilotClosed == 1

    # get job parameters
    def get_job_params(self, strip):
        if not strip:
            return self.jobParams
        else:
            newParams = dict()
            for k, v in iteritems(self.jobParams):
                if k in ["prodDBlocks", "realDatasetsIn", "dispatchDblock", "ddmEndPointIn", "scopeIn", "dispatchDBlockToken", "prodDBlockToken"]:
                    continue
                newParams[k] = v
            return newParams

    # get pilot type
    def get_pilot_type(self):
        if "prodSourceLabel" not in self.jobParams:
            return None
        if self.jobParams["prodSourceLabel"] == "rc_test":
            return "RC"
        elif self.jobParams["prodSourceLabel"] == "rc_test2":
            return "RC"
        elif self.jobParams["prodSourceLabel"] == "rc_alrb":
            return "ALRB"
        elif self.jobParams["prodSourceLabel"] == "ptest":
            return "PT"
        elif self.jobParams["prodSourceLabel"]:
            return "PR"
        else:
            return None

    # manipulate job parameters related to container
    def manipulate_job_params_for_container(self):
        updated = False
        for fileSpec in self.inFiles:
            for k, v in iteritems(self.jobParams):
                # only container image
                if k == "container_name":
                    if v == fileSpec.url:
                        self.jobParams[k] = fileSpec.path
                        updated = True
                elif k == "containerOptions":
                    for kk, vv in iteritems(v):
                        if kk == "containerImage":
                            if vv == fileSpec.url:
                                self.jobParams[k][kk] = fileSpec.path
                                updated = True
                    continue
        # trigger updating
        if updated:
            self.force_update("jobParams")
