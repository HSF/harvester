import json
import os
import copy
import shutil
import tarfile
import datetime
import itertools

try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

try:
    from os import scandir, walk
except ImportError:
    from scandir import scandir, walk

import re
import uuid
import os.path
import fnmatch
import distutils.spawn
import multiprocessing
from future.utils import iteritems
from past.builtins import long
from concurrent.futures import ThreadPoolExecutor as Pool

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from .base_messenger import BaseMessenger
from pandaharvester.harvesterconfig import harvester_config

# json for worker attributes
jsonAttrsFileName = harvester_config.payload_interaction.workerAttributesFile

# json for job report
jsonJobReport = harvester_config.payload_interaction.jobReportFile

# json for outputs
jsonOutputsFileName = harvester_config.payload_interaction.eventStatusDumpJsonFile

# xml for outputs
xmlOutputsBaseFileName = harvester_config.payload_interaction.eventStatusDumpXmlFile

# json for job request
jsonJobRequestFileName = harvester_config.payload_interaction.jobRequestFile

# json for job spec
jobSpecFileName = harvester_config.payload_interaction.jobSpecFile

# json for event request
jsonEventsRequestFileName = harvester_config.payload_interaction.eventRequestFile

# json to feed events
jsonEventsFeedFileName = harvester_config.payload_interaction.eventRangesFile

# json to update events
jsonEventsUpdateFileName = harvester_config.payload_interaction.updateEventsFile

# PFC for input files
xmlPoolCatalogFileName = harvester_config.payload_interaction.xmlPoolCatalogFile

# json to get PandaIDs
pandaIDsFile = harvester_config.payload_interaction.pandaIDsFile

# json to kill worker itself
try:
    killWorkerFile = harvester_config.payload_interaction.killWorkerFile
except Exception:
    killWorkerFile = "kill_worker.json"

# json for heartbeats from the worker
try:
    heartbeatFile = harvester_config.payload_interaction.heartbeatFile
except Exception:
    heartbeatFile = "worker_heartbeat.json"

# task specific persistent dir
try:
    taskWorkBaseDir = harvester_config.payload_interaction.taskWorkBaseDir
except Exception:
    taskWorkBaseDir = "/tmp/workdir"

# task-level work state file
try:
    taskWorkStateFile = harvester_config.payload_interaction.taskWorkStateFile
except Exception:
    taskWorkStateFile = "state.json"

# task-level work dir
taskWorkDirPathFile = "task_workdir_path.txt"

# post-processing job attributes
postProcessAttrs = "post_process_job_attrs.json"

# suffix to read json
suffixReadJson = ".read"

# logger
_logger = core_utils.setup_logger("shared_file_messenger")


def set_logger(master_logger):
    global _logger
    _logger = master_logger


# filter for log.tgz
def filter_log_tgz(extra=None):
    patt = ["*.log", "*.txt", "*.xml", "*.json", "log*"]
    if extra is not None:
        patt += extra
    return "-o ".join(['-name "{0}" '.format(i) for i in patt])


# tar a single directory
def tar_directory(dir_name, tar_name=None, max_depth=None, extra_files=None, sub_tarball_name=None):
    if tar_name is None:
        tarFilePath = os.path.join(os.path.dirname(dir_name), "{0}.subdir.tar.gz".format(os.path.basename(dir_name)))
    else:
        tarFilePath = tar_name
    # check if sub-tarball already exists
    com = None
    if sub_tarball_name is not None:
        subTarballPath = os.path.join(dir_name, sub_tarball_name)
        if os.path.exists(subTarballPath):
            com = "mv {} {}".format(subTarballPath, tarFilePath)
    # make sub-tarball
    if com is None:
        com = "cd {0}; ".format(dir_name)
        com += "find . "
        if max_depth is not None:
            com += "-maxdepth {0} ".format(max_depth)
        com += r"-type f \( " + filter_log_tgz(extra_files) + r"\) "
        com += r'| grep -v {0} | tr "\n" "\0" | '.format(jobSpecFileName)
        com += "tar "
        if distutils.spawn.find_executable("pigz") is None:
            com += "-z "
        else:
            com += "-I pigz "
        com += "-c -f {0} --null -T -".format(tarFilePath)
    p = subprocess.Popen(com, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdOut, stdErr = p.communicate()
    retCode = p.returncode
    return com, retCode, stdOut, stdErr


# scan files in a directory
def scan_files_in_dir(dir_name, patterns=None, zip_patterns=None):
    fileList = []
    for root, dirs, filenames in walk(dir_name):
        for filename in filenames:
            # check if zipped
            is_zipped = False
            if zip_patterns:
                matched = False
                for pattern in zip_patterns:
                    if re.search(pattern, filename) is not None:
                        matched = True
                        break
                if matched:
                    is_zipped = True
            # check filename
            if not is_zipped and patterns:
                matched = False
                for pattern in patterns:
                    if re.search(pattern, filename) is not None:
                        matched = True
                        break
                if not matched:
                    continue
            # make dict
            tmpFileDict = dict()
            pfn = os.path.join(root, filename)
            tmpFileDict["path"] = pfn
            tmpFileDict["fsize"] = os.stat(pfn).st_size
            tmpFileDict["guid"] = str(uuid.uuid4())
            tmpFileDict["chksum"] = core_utils.calc_adler32(pfn)
            tmpFileDict["eventStatus"] = "finished"
            if is_zipped:
                lfns = []
                # extract actual event filenames from zip
                with tarfile.open(pfn) as f:
                    for tar_info in f.getmembers():
                        lfns.append(os.path.basename(tar_info.name))
                tmpFileDict["type"] = "zip_output"
            else:
                lfns = [os.path.basename(pfn)]
                tmpFileDict["type"] = "es_output"
            for lfn in lfns:
                tmpDict = copy.copy(tmpFileDict)
                tmpDict["eventRangeID"] = lfn.split(".")[-1]

                fileList.append(tmpDict)
    return fileList


# messenger with shared file system
class SharedFileMessenger(BaseMessenger):
    # constructor
    def __init__(self, **kwarg):
        self.jobSpecFileFormat = "json"
        self.stripJobParams = False
        self.scanInPostProcess = False
        self.leftOverPatterns = None
        self.leftOverZipPatterns = None
        self.postProcessInSubDir = False
        self.outputSubDir = None
        self.subTarballName = None
        self.maxWorkersForZip = None
        BaseMessenger.__init__(self, **kwarg)

    # get access point
    def get_access_point(self, workspec, panda_id):
        if workspec.mapType == WorkSpec.MT_MultiJobs:
            accessPoint = os.path.join(workspec.get_access_point(), str(panda_id))
        else:
            accessPoint = workspec.get_access_point()
        return accessPoint

    # get task access point
    def get_task_access_point(self, workspec, jobspec):
        subAccessPoint = self.get_access_point(workspec, jobspec.PandaID)
        tmp_file = os.path.join(subAccessPoint, taskWorkDirPathFile)
        if os.path.exists(tmp_file):
            with open(tmp_file) as f:
                return f.read()
        if jobspec.jobParams and "onSiteMerging" in jobspec.jobParams:
            return os.path.join(taskWorkBaseDir, str(jobspec.taskID))
        return None

    # get attributes of a worker which should be propagated to job(s).
    #  * the worker needs to put a json under the access point
    def get_work_attributes(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="get_work_attributes")
        allRetDict = dict()
        numofreads = 0
        sw_readreports = core_utils.get_stopwatch()
        for pandaID in workspec.pandaid_list:
            # look for the json just under the access point
            accessPoint = self.get_access_point(workspec, pandaID)
            jsonFilePath = os.path.join(accessPoint, jsonAttrsFileName)
            tmpLog.debug("looking for attributes file {0}".format(jsonFilePath))
            retDict = dict()
            if not os.path.exists(jsonFilePath):
                # not found
                tmpLog.debug("not found attributes file")
            else:
                try:
                    with open(jsonFilePath) as jsonFile:
                        retDict = json.load(jsonFile)
                except Exception:
                    tmpLog.debug("failed to load {0}".format(jsonFilePath))
            # look for job report
            jsonFilePath = os.path.join(accessPoint, jsonJobReport)
            tmpLog.debug("looking for job report file {0}".format(jsonFilePath))
            sw_checkjobrep = core_utils.get_stopwatch()
            if not os.path.exists(jsonFilePath):
                # not found
                tmpLog.debug("not found job report file")
            else:
                try:
                    sw_readrep = core_utils.get_stopwatch()
                    with open(jsonFilePath) as jsonFile:
                        tmpDict = json.load(jsonFile)
                    retDict["metaData"] = tmpDict
                    tmpLog.debug("got {0} kB of job report. {1} sec.".format(os.stat(jsonFilePath).st_size / 1024, sw_readrep.get_elapsed_time()))
                    numofreads += 1
                except Exception:
                    tmpLog.debug("failed to load {0}".format(jsonFilePath))
            tmpLog.debug("Check file and read file time: {0} sec.".format(sw_checkjobrep.get_elapsed_time()))
            # loop for post-processing job attributes
            jsonFilePath = os.path.join(accessPoint, postProcessAttrs)
            tmpLog.debug("looking for post-processing job attributes file {0}".format(jsonFilePath))
            if not os.path.exists(jsonFilePath):
                # not found
                tmpLog.debug("not found post-processing job attributes file")
            else:
                try:
                    with open(jsonFilePath) as jsonFile:
                        tmpDict = json.load(jsonFile)
                    retDict.update(tmpDict)
                except Exception:
                    tmpLog.debug("failed to load {0}".format(jsonFilePath))
            allRetDict[pandaID] = retDict

        tmpLog.debug("Reading {0} job report files {1}".format(numofreads, sw_readreports.get_elapsed_time()))
        return allRetDict

    # get files to stage-out.
    #  * the worker needs to put a json under the access point
    def get_files_to_stage_out(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="get_files_to_stage_out")
        fileDict = dict()
        # look for the json just under the access point
        for pandaID in workspec.pandaid_list:
            # look for the json just under the access point
            accessPoint = self.get_access_point(workspec, pandaID)
            jsonFilePath = os.path.join(accessPoint, jsonOutputsFileName)
            readJsonPath = jsonFilePath + suffixReadJson
            # first look for json.read which is not yet acknowledged
            tmpLog.debug("looking for output file {0}".format(readJsonPath))
            if os.path.exists(readJsonPath):
                pass
            else:
                tmpLog.debug("looking for output file {0}".format(jsonFilePath))
                if not os.path.exists(jsonFilePath):
                    # not found
                    tmpLog.debug("not found")
                    continue
                try:
                    tmpLog.debug("found")
                    # rename to prevent from being overwritten
                    os.rename(jsonFilePath, readJsonPath)
                except Exception:
                    tmpLog.error("failed to rename json")
                    continue
            # load json
            toSkip = False
            loadDict = None
            try:
                with open(readJsonPath) as jsonFile:
                    loadDict = json.load(jsonFile)
            except Exception:
                tmpLog.error("failed to load json")
                toSkip = True
            # test validity of data format (ie it should be a Dictionary)
            if not toSkip:
                if not isinstance(loadDict, dict):
                    tmpLog.error("loaded data is not a dictionary")
                    toSkip = True
            # collect files and events
            nData = 0
            if not toSkip:
                sizeMap = dict()
                chksumMap = dict()
                eventsList = dict()
                for tmpPandaID, tmpEventMapList in iteritems(loadDict):
                    tmpPandaID = long(tmpPandaID)
                    # test if tmpEventMapList is a list
                    if not isinstance(tmpEventMapList, list):
                        tmpLog.error("loaded data item is not a list")
                        toSkip = True
                        break
                    for tmpEventInfo in tmpEventMapList:
                        try:
                            nData += 1
                            if "eventRangeID" in tmpEventInfo:
                                tmpEventRangeID = tmpEventInfo["eventRangeID"]
                            else:
                                tmpEventRangeID = None
                            if "path" in tmpEventInfo:
                                tmpFileDict = dict()
                                pfn = tmpEventInfo["path"]
                                lfn = os.path.basename(pfn)
                                tmpFileDict["path"] = pfn
                                if pfn not in sizeMap:
                                    if "fsize" in tmpEventInfo:
                                        sizeMap[pfn] = tmpEventInfo["fsize"]
                                    else:
                                        sizeMap[pfn] = os.stat(pfn).st_size
                                tmpFileDict["fsize"] = sizeMap[pfn]
                                tmpFileDict["type"] = tmpEventInfo["type"]
                                if tmpEventInfo["type"] in ["log", "output", "checkpoint"]:
                                    # disable zipping
                                    tmpFileDict["isZip"] = 0
                                elif tmpEventInfo["type"] == "zip_output":
                                    # already zipped
                                    tmpFileDict["isZip"] = 1
                                elif "isZip" in tmpEventInfo:
                                    tmpFileDict["isZip"] = tmpEventInfo["isZip"]
                                # guid
                                if "guid" in tmpEventInfo:
                                    tmpFileDict["guid"] = tmpEventInfo["guid"]
                                else:
                                    tmpFileDict["guid"] = str(uuid.uuid4())
                                # get checksum
                                if pfn not in chksumMap:
                                    if "chksum" in tmpEventInfo:
                                        chksumMap[pfn] = tmpEventInfo["chksum"]
                                    else:
                                        chksumMap[pfn] = core_utils.calc_adler32(pfn)
                                tmpFileDict["chksum"] = chksumMap[pfn]
                                if tmpPandaID not in fileDict:
                                    fileDict[tmpPandaID] = dict()
                                if lfn not in fileDict[tmpPandaID]:
                                    fileDict[tmpPandaID][lfn] = []
                                fileDict[tmpPandaID][lfn].append(tmpFileDict)
                                # skip if unrelated to events
                                if tmpFileDict["type"] not in ["es_output", "zip_output"]:
                                    continue
                                tmpFileDict["eventRangeID"] = tmpEventRangeID
                            if tmpPandaID not in eventsList:
                                eventsList[tmpPandaID] = list()
                            eventsList[tmpPandaID].append({"eventRangeID": tmpEventRangeID, "eventStatus": tmpEventInfo["eventStatus"]})
                        except Exception:
                            core_utils.dump_error_message(tmpLog)
                # dump events
                if not toSkip:
                    if len(eventsList) > 0:
                        curName = os.path.join(accessPoint, jsonEventsUpdateFileName)
                        newName = curName + ".new"
                        f = open(newName, "w")
                        json.dump(eventsList, f)
                        f.close()
                        os.rename(newName, curName)
            # remove empty file
            if toSkip or nData == 0:
                try:
                    os.remove(readJsonPath)
                except Exception:
                    pass
            tmpLog.debug("got {0} files for PandaID={1}".format(nData, pandaID))
        return fileDict

    # check if job is requested.
    # * the worker needs to put a json under the access point
    def job_requested(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="job_requested")
        # look for the json just under the access point
        jsonFilePath = os.path.join(workspec.get_access_point(), jsonJobRequestFileName)
        tmpLog.debug("looking for job request file {0}".format(jsonFilePath))
        if not os.path.exists(jsonFilePath):
            # not found
            tmpLog.debug("not found")
            return False
        # read nJobs
        try:
            with open(jsonFilePath) as jsonFile:
                tmpDict = json.load(jsonFile)
                nJobs = tmpDict["nJobs"]
        except Exception:
            # request 1 job by default
            nJobs = 1
        tmpLog.debug("requesting {0} jobs".format(nJobs))
        return nJobs

    # feed jobs
    # * worker_jobspec.json is put under the access point
    def feed_jobs(self, workspec, jobspec_list):
        # get logger
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="feed_jobs")
        retVal = True
        # get PFC
        pfc = core_utils.make_pool_file_catalog(jobspec_list)
        pandaIDs = []
        for jobSpec in jobspec_list:
            accessPoint = self.get_access_point(workspec, jobSpec.PandaID)
            jobSpecFilePath = os.path.join(accessPoint, jobSpecFileName)
            xmlFilePath = os.path.join(accessPoint, xmlPoolCatalogFileName)
            tmpLog.debug("feeding jobs to {0}".format(jobSpecFilePath))
            try:
                # put job spec file
                with open(jobSpecFilePath, "w") as jobSpecFile:
                    jobParams = jobSpec.get_job_params(self.stripJobParams)
                    if self.jobSpecFileFormat == "cgi":
                        jobSpecFile.write(urlencode(jobParams))
                    else:
                        json.dump({jobSpec.PandaID: jobParams}, jobSpecFile)
                # put PFC.xml
                with open(xmlFilePath, "w") as pfcFile:
                    pfcFile.write(pfc)
                # make symlink
                for fileSpec in jobSpec.inFiles:
                    dstPath = os.path.join(accessPoint, fileSpec.lfn)
                    if fileSpec.path != dstPath:
                        # test if symlink exists if so remove it
                        if os.path.exists(dstPath):
                            os.unlink(dstPath)
                            tmpLog.debug("removing existing symlink %s" % dstPath)
                        os.symlink(fileSpec.path, dstPath)
                pandaIDs.append(jobSpec.PandaID)
            except Exception:
                core_utils.dump_error_message(tmpLog)
                retVal = False
        # put PandaIDs file
        try:
            jsonFilePath = os.path.join(workspec.get_access_point(), pandaIDsFile)
            with open(jsonFilePath, "w") as jsonPandaIDsFile:
                json.dump(pandaIDs, jsonPandaIDsFile)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            retVal = False
        # remove request file
        try:
            reqFilePath = os.path.join(workspec.get_access_point(), jsonJobRequestFileName)
            os.remove(reqFilePath)
        except Exception:
            pass
        tmpLog.debug("done")
        return retVal

    # request events.
    # * the worker needs to put a json under the access point
    def events_requested(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="events_requested")
        # look for the json just under the access point
        jsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsRequestFileName)
        tmpLog.debug("looking for event request file {0}".format(jsonFilePath))
        if not os.path.exists(jsonFilePath):
            # not found
            tmpLog.debug("not found")
            return {}
        try:
            with open(jsonFilePath) as jsonFile:
                retDict = json.load(jsonFile)
        except Exception:
            tmpLog.debug("failed to load json")
            return {}
        tmpLog.debug("found")
        return retDict

    # feed events
    # * worker_events.json is put under the access point
    def feed_events(self, workspec, events_dict):
        # get logger
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="feed_events")
        retVal = True
        if workspec.mapType in [WorkSpec.MT_OneToOne, WorkSpec.MT_MultiWorkers]:
            # put the json just under the access point
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsFeedFileName)
            tmpLog.debug("feeding events to {0}".format(jsonFilePath))
            try:
                with open(jsonFilePath, "w") as jsonFile:
                    json.dump(events_dict, jsonFile)
            except Exception:
                core_utils.dump_error_message(tmpLog)
                retVal = False
        elif workspec.mapType == WorkSpec.MT_MultiJobs:
            # TOBEFIXED
            pass
        # remove request file
        try:
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsRequestFileName)
            os.remove(jsonFilePath)
        except Exception:
            pass
        tmpLog.debug("done")
        return retVal

    # update events.
    # * the worker needs to put a json under the access point
    def events_to_update(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="events_to_update")
        # look for the json just under the access point
        retDict = dict()
        for pandaID in workspec.pandaid_list:
            # look for the json just under the access point
            accessPoint = self.get_access_point(workspec, pandaID)

            jsonFilePath = os.path.join(accessPoint, jsonEventsUpdateFileName)
            readJsonPath = jsonFilePath + suffixReadJson
            # first look for json.read which is not yet acknowledged
            tmpLog.debug("looking for event update file {0}".format(readJsonPath))
            if os.path.exists(readJsonPath):
                pass
            else:
                tmpLog.debug("looking for event update file {0}".format(jsonFilePath))
                if not os.path.exists(jsonFilePath):
                    # not found
                    tmpLog.debug("not found")
                    continue
                try:
                    # rename to prevent from being overwritten
                    os.rename(jsonFilePath, readJsonPath)
                except Exception:
                    tmpLog.error("failed to rename json")
                    continue
            # load json
            nData = 0
            try:
                with open(readJsonPath) as jsonFile:
                    tmpOrigDict = json.load(jsonFile)
                    newDict = dict()
                    # change the key from str to int
                    for tmpPandaID, tmpDict in iteritems(tmpOrigDict):
                        tmpPandaID = long(tmpPandaID)
                        retDict[tmpPandaID] = tmpDict
                        nData += len(tmpDict)
            except Exception:
                tmpLog.error("failed to load json")
            # delete empty file
            if nData == 0:
                try:
                    os.remove(readJsonPath)
                except Exception:
                    pass
            tmpLog.debug("got {0} events for PandaID={1}".format(nData, pandaID))
        return retDict

    # acknowledge events and files
    # * delete json.read files
    def acknowledge_events_files(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="acknowledge_events_files")
        # remove request file
        for pandaID in workspec.pandaid_list:
            accessPoint = self.get_access_point(workspec, pandaID)
            try:
                jsonFilePath = os.path.join(accessPoint, jsonEventsUpdateFileName)
                jsonFilePath += suffixReadJson
                jsonFilePath_rename = jsonFilePath + "." + datetime.datetime.utcnow().strftime("%Y-%m-%d_%H_%M_%S.%f")
                os.rename(jsonFilePath, jsonFilePath_rename)
            except Exception:
                pass
            try:
                jsonFilePath = os.path.join(accessPoint, jsonOutputsFileName)
                jsonFilePath += suffixReadJson
                jsonFilePath_rename = jsonFilePath + "." + datetime.datetime.utcnow().strftime("%Y-%m-%d_%H_%M_%S.%f")
                os.rename(jsonFilePath, jsonFilePath_rename)
            except Exception:
                pass
        tmpLog.debug("done")
        return

    # setup access points
    def setup_access_points(self, workspec_list):
        try:
            for workSpec in workspec_list:
                accessPoint = workSpec.get_access_point()
                # delete leftover
                if os.path.exists(accessPoint) and workSpec.isNew:
                    shutil.rmtree(accessPoint, ignore_errors=True)
                # make the dir if missing
                if not os.path.exists(accessPoint):
                    os.makedirs(accessPoint)
                jobSpecs = workSpec.get_jobspec_list()
                if jobSpecs is not None:
                    for jobSpec in jobSpecs:
                        subAccessPoint = self.get_access_point(workSpec, jobSpec.PandaID)
                        if accessPoint != subAccessPoint:
                            if not os.path.exists(subAccessPoint):
                                os.mkdir(subAccessPoint)
                        # task level work dir for on-site merging
                        taskAccessDir = self.get_task_access_point(workSpec, jobSpec)
                        if taskAccessDir:
                            if not os.path.exists(taskAccessDir):
                                os.mkdir(taskAccessDir)
                            with open(os.path.join(subAccessPoint, taskWorkDirPathFile), "w") as f:
                                f.write(taskAccessDir)
            return True
        except Exception:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="setup_access_points")
            core_utils.dump_error_message(tmpLog)
            return False

    # filter for log.tar.gz
    def filter_log_tgz(self, name):
        for tmpPatt in ["*.log", "*.txt", "*.xml", "*.json", "log*"]:
            if fnmatch.fnmatch(name, tmpPatt):
                return True
        return False

    # post-processing (archiving log files and collecting job metrics)
    def post_processing(self, workspec, jobspec_list, map_type):
        # get logger
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="post_processing")
        try:
            for jobSpec in jobspec_list:
                # check if log is already there
                hasLog = False
                for fileSpec in jobSpec.outFiles:
                    if fileSpec.fileType == "log":
                        hasLog = True
                        break
                fileDict = dict()
                accessPoint = self.get_access_point(workspec, jobSpec.PandaID)
                origAccessPoint = accessPoint
                if self.postProcessInSubDir:
                    accessPoint = os.path.join(accessPoint, str(jobSpec.PandaID))
                # make log
                if not hasLog:
                    logFileInfo = jobSpec.get_logfile_info()
                    # make log.tar.gz
                    logFilePath = os.path.join(accessPoint, logFileInfo["lfn"])
                    if map_type == WorkSpec.MT_MultiWorkers:
                        # append suffix
                        logFilePath += "._{0}".format(workspec.workerID)
                    tmpLog.debug("making {0}".format(logFilePath))
                    dirs = [os.path.join(accessPoint, name) for name in os.listdir(accessPoint) if os.path.isdir(os.path.join(accessPoint, name))]
                    # tar sub dirs
                    tmpLog.debug("tar for {0} sub dirs".format(len(dirs)))
                    with Pool(max_workers=self.maxWorkersForZip if self.maxWorkersForZip else multiprocessing.cpu_count()) as pool:
                        retValList = pool.map(lambda x, y: tar_directory(x, sub_tarball_name=y), dirs, itertools.repeat(self.subTarballName))
                        for dirName, (comStr, retCode, stdOut, stdErr) in zip(dirs, retValList):
                            if retCode != 0:
                                tmpLog.warning("failed to sub-tar {0} with {1} -> {2}:{3}".format(dirName, comStr, stdOut, stdErr))
                    # tar main dir
                    tmpLog.debug("tar for main dir")
                    comStr, retCode, stdOut, stdErr = tar_directory(accessPoint, logFilePath, 1, ["*.subdir.tar.gz"])
                    tmpLog.debug("used command : " + comStr)
                    if retCode != 0:
                        tmpLog.warning("failed to tar {0} with {1} -> {2}:{3}".format(accessPoint, comStr, stdOut, stdErr))
                    # make file dict
                    fileDict.setdefault(jobSpec.PandaID, [])
                    fileDict[jobSpec.PandaID].append({"path": logFilePath, "type": "log", "isZip": 0})
                # look for leftovers
                if self.scanInPostProcess:
                    tmpLog.debug("scanning leftovers in {0}".format(accessPoint))
                    # set the directory paths to scan for left over files
                    dirs = []
                    if self.outputSubDir is None:
                        dirs = [os.path.join(accessPoint, name) for name in os.listdir(accessPoint) if os.path.isdir(os.path.join(accessPoint, name))]
                    else:
                        # loop over directories first level from accessPoint and then add subdirectory name.
                        upperdirs = [os.path.join(accessPoint, name) for name in os.listdir(accessPoint) if os.path.isdir(os.path.join(accessPoint, name))]
                        dirs = [os.path.join(dirname, self.outputSubDir) for dirname in upperdirs if os.path.isdir(os.path.join(dirname, self.outputSubDir))]
                    patterns = []
                    patterns_zip = []
                    for tmp_patterns, tmp_left_over_patterns in [[patterns, self.leftOverPatterns], [patterns_zip, self.leftOverZipPatterns]]:
                        if tmp_left_over_patterns is None:
                            continue
                        for scanPat in tmp_left_over_patterns:
                            # replace placeholders
                            if "%PANDAID" in scanPat:
                                scanPat = scanPat.replace("%PANDAID", str(jobSpec.PandaID))
                            if "%TASKID" in scanPat:
                                scanPat = scanPat.replace("%TASKID", str(jobSpec.taskID))
                            if "%OUTPUT_FILE" in scanPat:
                                logFileName = jobSpec.get_logfile_info()["lfn"]
                                for outputName in jobSpec.get_output_file_attributes().keys():
                                    if outputName == logFileName:
                                        continue
                                    tmp_patterns.append(scanPat.replace("%OUTPUT_FILE", outputName))
                            else:
                                tmp_patterns.append(scanPat)
                    # scan files
                    nLeftOvers = 0
                    with Pool(max_workers=self.maxWorkersForZip if self.maxWorkersForZip else multiprocessing.cpu_count()) as pool:
                        retValList = pool.map(scan_files_in_dir, dirs, [patterns] * len(dirs), [patterns_zip] * len(dirs))
                        for retVal in retValList:
                            fileDict.setdefault(jobSpec.PandaID, [])
                            fileDict[jobSpec.PandaID] += retVal
                            nLeftOvers += len(retVal)
                    tmpLog.debug("got {0} leftovers".format(nLeftOvers))
                # look into task-level work state file
                taskAccessDir = self.get_task_access_point(workspec, jobSpec)
                if taskAccessDir:
                    doneInputs = set()
                    taskWorkStatePath = os.path.join(taskAccessDir, taskWorkStateFile)
                    if os.path.exists(taskWorkStatePath):
                        nInTaskState = 0
                        with open(taskWorkStatePath) as f:
                            try:
                                tmpData = json.load(f)
                                if "merged" in tmpData:
                                    output_lfns = set()
                                    fileDict.setdefault(jobSpec.PandaID, [])
                                    for tmpIn, tmpOuts in iteritems(tmpData["merged"]):
                                        for tmpLFN, tmpFileDict in iteritems(tmpOuts):
                                            if tmpLFN in output_lfns:
                                                continue
                                            output_lfns.add(tmpLFN)
                                            nInTaskState += 1
                                            pfn = tmpFileDict["path"]
                                            if "fsize" not in tmpFileDict:
                                                tmpFileDict["fsize"] = os.stat(pfn).st_size
                                            tmpFileDict["type"] = "output"
                                            if "guid" not in tmpFileDict:
                                                tmpFileDict["guid"] = str(uuid.uuid4())
                                            if "chksum" not in tmpFileDict:
                                                tmpFileDict["chksum"] = core_utils.calc_adler32(pfn)
                                            fileDict.setdefault(jobSpec.PandaID, [])
                                            fileDict[jobSpec.PandaID].append(tmpFileDict)
                                        doneInputs.add(tmpIn)
                            except Exception:
                                core_utils.dump_error_message(tmpLog)
                                tmpLog.error("failed to parse task-level work state file {0}".format(taskWorkStatePath))
                                raise
                        tmpLog.debug("got {0} output files from task state file".format(nInTaskState))
                    # skipped files
                    skippedInputs = [fileSpec.lfn for fileSpec in jobSpec.inFiles if fileSpec.lfn not in doneInputs]
                    with open(os.path.join(accessPoint, postProcessAttrs), "w") as f:
                        json.dump({"skippedInputs": skippedInputs}, f)
                    tmpLog.debug("set {0} input files to skip".format(len(skippedInputs)))
                # make json to stage-out
                if len(fileDict) > 0:
                    jsonFilePath = os.path.join(origAccessPoint, jsonOutputsFileName)
                    with open(jsonFilePath, "w") as jsonFile:
                        json.dump(fileDict, jsonFile)
                tmpLog.debug("done")
            return True
        except Exception:
            core_utils.dump_error_message(tmpLog)
            return None

    # get PandaIDs for pull model
    def get_panda_ids(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="get_panda_ids")
        # look for the json just under the access point
        jsonFilePath = os.path.join(workspec.get_access_point(), pandaIDsFile)
        tmpLog.debug("looking for PandaID file {0}".format(jsonFilePath))
        retVal = []
        if not os.path.exists(jsonFilePath):
            # not found
            tmpLog.debug("not found")
            return retVal
        try:
            with open(jsonFilePath) as jsonFile:
                retVal = json.load(jsonFile)
        except Exception:
            tmpLog.debug("failed to load json")
            return retVal
        tmpLog.debug("found")
        return retVal

    # check if requested to kill the worker itself
    def kill_requested(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="kill_requested")
        # look for the json just under the access point
        jsonFilePath = os.path.join(workspec.get_access_point(), killWorkerFile)
        tmpLog.debug("looking for kill request file {0}".format(jsonFilePath))
        if not os.path.exists(jsonFilePath):
            # not found
            tmpLog.debug("not found")
            return False
        tmpLog.debug("kill requested")
        return True

    # check if the worker is alive
    def is_alive(self, workspec, time_limit):
        # get logger
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="is_alive")
        # json file
        jsonFilePath = os.path.join(workspec.get_access_point(), heartbeatFile)
        tmpLog.debug("looking for heartbeat file {0}".format(jsonFilePath))
        if not os.path.exists(jsonFilePath):  # no heartbeat file was found
            tmpLog.debug("startTime: {0}, now: {1}".format(workspec.startTime, datetime.datetime.utcnow()))
            if not workspec.startTime:
                # the worker didn't even have time to start
                tmpLog.debug("heartbeat not found, but no startTime yet for worker")
                return True
            elif datetime.datetime.utcnow() - workspec.startTime < datetime.timedelta(minutes=time_limit):
                # the worker is too young and maybe didn't have time to generate the heartbeat
                tmpLog.debug("heartbeat not found, but worker too young")
                return True
            else:
                # the worker is old and the heartbeat should be expected
                tmpLog.debug("not found")
                return None
        try:
            mtime = datetime.datetime.utcfromtimestamp(os.path.getmtime(jsonFilePath))
            tmpLog.debug("last modification time : {0}".format(mtime))
            if datetime.datetime.utcnow() - mtime > datetime.timedelta(minutes=time_limit):
                tmpLog.debug("too old")
                return False
            tmpLog.debug("OK")
            return True
        except Exception:
            tmpLog.debug("failed to get mtime")
            return None

    # clean up. Called by sweeper agent to clean up stuff made by messenger for the worker
    # for shared_file_messenger, clean up worker the directory of access point
    def clean_up(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, "workerID={0}".format(workspec.workerID), method_name="clean_up")
        # Remove from top directory of access point of worker
        errStr = ""
        worker_accessPoint = workspec.get_access_point()
        if os.path.isdir(worker_accessPoint):
            try:
                shutil.rmtree(worker_accessPoint)
            except Exception as _e:
                errStr = "failed to remove directory {0} : {1}".format(worker_accessPoint, _e)
                tmpLog.error(errStr)
            else:
                tmpLog.debug("done")
                return (True, errStr)
        elif not os.path.exists(worker_accessPoint):
            tmpLog.debug("accessPoint directory already gone. Skipped")
            return (None, errStr)
        else:
            errStr = "{0} is not a directory".format(worker_accessPoint)
            tmpLog.error(errStr)
        return (False, errStr)
