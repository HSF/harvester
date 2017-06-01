import json
import os
import re
import math
import uuid
import os.path
import tarfile
import fnmatch
import types 
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.event_spec import EventSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
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
jsonJobSpecFileName = harvester_config.payload_interaction.jobSpecFile

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

# suffix to read json
suffixReadJson = '.read'

# logger
_logger = core_utils.setup_logger()


def set_logger(master_logger):
    global _logger
    _logger = master_logger


# messenger with shared file system
class SharedFileMessenger(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # update job attributes with workers
    def update_job_attributes_with_workers(self, map_type, jobspec_list, workspec_list, files_to_stage_out,
                                           events_to_update):
        if map_type == WorkSpec.MT_OneToOne:
            jobSpec = jobspec_list[0]
            workSpec = workspec_list[0]
            tmpLog = core_utils.make_logger(_logger, 'PandaID={0} workerID={1}'.format(jobSpec.PandaID,
                                                                                       workSpec.workerID))
            jobSpec.set_attributes(workSpec.workAttributes)
            # set start and end times
            if workSpec.status in [WorkSpec.ST_running]:
                jobSpec.set_start_time()
            elif workSpec.status in [WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled]:
                jobSpec.set_end_time()
            # core count
            if workSpec.nCore is not None:
                jobSpec.nCore = workSpec.nCore
            # add files
            if jobSpec.PandaID in files_to_stage_out:
                for lfn, fileAtters in files_to_stage_out[jobSpec.PandaID].iteritems():
                    fileSpec = FileSpec()
                    fileSpec.lfn = lfn
                    fileSpec.PandaID = jobSpec.PandaID
                    fileSpec.taskID = jobSpec.taskID
                    fileSpec.path = fileAtters['path']
                    fileSpec.fsize = fileAtters['fsize']
                    fileSpec.fileType = fileAtters['type']
                    fileSpec.fileAttributes = fileAtters
                    if 'isZip' in fileAtters:
                        fileSpec.isZip = fileAtters['isZip']
                    if 'chksum' in fileAtters:
                        fileSpec.chksum = fileAtters['chksum']
                    if 'eventRangeID' in fileAtters:
                        fileSpec.eventRangeID = fileAtters['eventRangeID']
                    jobSpec.add_out_file(fileSpec)
            # add events
            if jobSpec.PandaID in events_to_update:
                for data in events_to_update[jobSpec.PandaID]:
                    eventSpec = EventSpec()
                    eventSpec.from_data(data)
                    jobSpec.add_event(eventSpec, None)
            jobSpec.status, jobSpec.subStatus = workSpec.convert_to_job_status()
            tmpLog.debug('new jobStatus={0} subStatus={1}'.format(jobSpec.status, jobSpec.subStatus))
        elif map_type == WorkSpec.MT_MultiWorkers:
            jobSpec = jobspec_list[0]
            # scan all workers
            allDone = True
            isRunning = False
            oneFinished = False
            oneFailed = False
            nCore = 0
            nCoreTime = 0
            for workSpec in workspec_list:
                # the the worker is running
                if workSpec.status in [WorkSpec.ST_running]:
                    isRunning = True
                    # set start time
                    jobSpec.set_start_time()
                    nCore += workSpec.nCore
                # the worker is done
                if workSpec.status in [WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled]:
                    nCoreTime += workSpec.nCore * (workSpec.endTime - workSpec.startTime).total_seconds()
                    if workSpec.status == WorkSpec.ST_finished:
                        oneFinished = True
                    elif workSpec.status == WorkSpec.ST_failed:
                        oneFailed = True
                else:
                    # the worker is still active
                    allDone = False
            # set final values
            if allDone:
                # set end time
                jobSpec.set_end_time()
                # time-averaged core count
                jobSpec.nCore = float(nCoreTime) / float((jobSpec.endTime - jobSpec.startTime).total_seconds())
                jobSpec.nCore = int(math.ceil(jobSpec.nCore))
            else:
                # live core count
                jobSpec.nCore = nCore
            # combine worker attributes and set it to job
            # FIXME
            # jobSpec.set_attributes(workAttributes)
            # add files
            if jobSpec.PandaID in files_to_stage_out:
                for lfn, fileAtters in files_to_stage_out[jobSpec.PandaID].iteritems():
                    fileSpec = FileSpec()
                    fileSpec.lfn = lfn
                    fileSpec.PandaID = jobSpec.PandaID
                    fileSpec.taskID = jobSpec.taskID
                    fileSpec.path = fileAtters['path']
                    fileSpec.fsize = fileAtters['fsize']
                    fileSpec.fileType = fileAtters['type']
                    fileSpec.fileAttributes = fileAtters
                    if 'isZip' in fileAtters:
                        fileSpec.isZip = fileAtters['isZip']
                    if 'chksum' in fileAtters:
                        fileSpec.chksum = fileAtters['chksum']
                    if 'eventRangeID' in fileAtters:
                        fileSpec.eventRangeID = fileAtters['eventRangeID']
                    jobSpec.add_out_file(fileSpec)
            # add events
            if jobSpec.PandaID in events_to_update:
                for data in events_to_update[jobSpec.PandaID]:
                    eventSpec = EventSpec()
                    eventSpec.from_data(data)
                    jobSpec.add_event(eventSpec, None)
            # set job status
            workSpec = workspec_list[0]
            if allDone:
                if oneFinished:
                    jobSpec.status, jobSpec.subStatus = workSpec.convert_to_job_status(WorkSpec.ST_finished)
                elif oneFailed:
                    jobSpec.status, jobSpec.subStatus = workSpec.convert_to_job_status(WorkSpec.ST_failed)
                else:
                    jobSpec.status, jobSpec.subStatus = workSpec.convert_to_job_status(WorkSpec.ST_cancelled)
            else:
                if isRunning or jobSpec.jobStatus == 'running':
                    jobSpec.status, jobSpec.subStatus = workSpec.convert_to_job_status(WorkSpec.ST_running)
                else:
                    jobSpec.status, jobSpec.subStatus = workSpec.convert_to_job_status(WorkSpec.ST_submitted)
        elif map_type == WorkSpec.MT_MultiJobs:
            # TOBEFIXED
            pass
        return True

    # get attributes of a worker which should be propagated to job(s).
    #  * the worker needs to put a json under the access point
    def get_work_attributes(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        retDict = {}
        if workspec.mapType == WorkSpec.MT_OneToOne:
            # look for the json just under the access point
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonAttrsFileName)
            tmpLog.debug('looking for attributes file {0}'.format(jsonFilePath))
            retDict = dict()
            if not os.path.exists(jsonFilePath):
                # not found
                tmpLog.debug('not found')
            else:
                try:
                    with open(jsonFilePath) as jsonFile:
                        retDict = json.load(jsonFile)
                except:
                    tmpLog.debug('failed to load {0}'.format(jsonFilePath))
            # look for job report
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonJobReport)
            tmpLog.debug('looking for attributes file {0}'.format(jsonFilePath))
            if not os.path.exists(jsonFilePath):
                # not found
                tmpLog.debug('not found')
            else:
                try:
                    with open(jsonFilePath) as jsonFile:
                        tmpDict = json.load(jsonFile)
                        retDict['metadata'] = tmpDict
                except:
                    tmpLog.debug('failed to load {0}'.format(jsonFilePath))
        elif workspec.mapType == WorkSpec.MT_MultiJobs:
            # look for json files under accesspoint/${PandaID}
            # TOBEFIXED
            pass
        return retDict

    # get files to stage-out.
    #  * the worker needs to put a json under the access point
    def get_files_to_stage_out(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        fileDict = dict()
        if workspec.mapType == WorkSpec.MT_OneToOne:
            # look for the json just under the access point
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonOutputsFileName)
            readJsonPath = jsonFilePath + suffixReadJson
            # first look for json.read which is not yet acknowledged
            tmpLog.debug('looking for output file {0}'.format(readJsonPath))
            if os.path.exists(readJsonPath):
                pass
            else:
                tmpLog.debug('looking for output file {0}'.format(jsonFilePath))
                if not os.path.exists(jsonFilePath):
                    # not found
                    tmpLog.debug('not found')
                    return {}
                try:
                    # rename to prevent from being overwritten
                    os.rename(jsonFilePath, readJsonPath)
                except:
                    tmpLog.error('failed to rename json')
                    return {}
            # load json
            try:
                with open(readJsonPath) as jsonFile:
                    loadDict = json.load(jsonFile)
            except:
                tmpLog.debug('failed to load json')
                return {}
            # test validity of data format (ie it should be a Dictionary)
            if type(loadDict) is types.DictType :
                pass
            else:
                tmpLog.debug('loadDict data is not a dictionary')
                return {}
            # collect files and events
            eventsList = dict()
            for tmpPandaID, tmpEventMapList in loadDict.iteritems():
                tmpPandaID = long(tmpPandaID)
                # test if tmpEventMapList is a list
                if type(tmpEventMapList) is not types.ListType:
                    tmpLog.debug('loadDict data is not a dictionary')
                    return {}
                for tmpEventInfo in tmpEventMapList:
                    if 'eventRangeID' in tmpEventInfo:
                        tmpEventRangeID = tmpEventInfo['eventRangeID']
                    else:
                        tmpEventRangeID = None
                    tmpFileDict = dict()
                    pfn = tmpEventInfo['path']
                    lfn = os.path.basename(pfn)
                    tmpFileDict['path'] = pfn
                    tmpFileDict['fsize'] = os.stat(pfn).st_size
                    tmpFileDict['type'] = tmpEventInfo['type']
                    if tmpEventInfo['type'] in ['log', 'output']:
                        # disable zipping
                        tmpFileDict['isZip'] = 0
                    elif 'isZip' in tmpEventInfo:
                        tmpFileDict['isZip'] = tmpEventInfo['isZip']
                    # guid
                    if 'guid' in tmpEventInfo:
                        tmpFileDict['guid'] = tmpEventInfo['guid']
                    else:
                        tmpFileDict['guid'] = str(uuid.uuid4())
                    # get checksum
                    if 'chksum' not in tmpEventInfo:
                        tmpEventInfo['chksum'] = core_utils.calc_adler32(pfn)
                    tmpFileDict['chksum'] = tmpEventInfo['chksum']
                    if tmpPandaID not in fileDict:
                        fileDict[tmpPandaID] = dict()
                    fileDict[tmpPandaID][lfn] = tmpFileDict
                    # skip if unrelated to events
                    if tmpFileDict['type'] not in ['es_output']:
                        continue
                    tmpFileDict['eventRangeID'] = tmpEventRangeID
                    if tmpPandaID not in eventsList:
                        eventsList[tmpPandaID] = list()
                    eventsList[tmpPandaID].append({'eventRangeID': tmpEventRangeID,
                                                   'eventStatus': tmpEventInfo['eventStatus']})
            # dump events
            if eventsList != []:
                curName = os.path.join(workspec.get_access_point(), jsonEventsUpdateFileName)
                newName = curName + '.new'
                f = open(newName, 'w')
                json.dump(eventsList, f)
                f.close()
                os.rename(newName, curName)
        elif workspec.mapType == WorkSpec.MT_MultiJobs:
            # look for json files under access_point/${PandaID}
            # TOBEFIXED
            pass
        tmpLog.debug('got {0}'.format(str(fileDict)))
        return fileDict

    # check if job is requested.
    # * the worker needs to put a json under the access point
    def job_requested(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        # look for the json just under the access point
        jsonFilePath = os.path.join(workspec.get_access_point(), jsonJobRequestFileName)
        tmpLog.debug('looking for job request file {0}'.format(jsonFilePath))
        if not os.path.exists(jsonFilePath):
            # not found
            tmpLog.debug('not found')
            return False
        tmpLog.debug('found')
        return True

    # feed jobs
    # * worker_jobspec.json is put under the access point
    def feed_jobs(self, workspec, jobspec_list):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        retVal = True
        # get PFC
        pfc = core_utils.make_pool_file_catalog(jobspec_list)
        if workspec.mapType == WorkSpec.MT_OneToOne:
            jobSpec = jobspec_list[0]
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonJobSpecFileName)
            xmlFilePath = os.path.join(workspec.get_access_point(), xmlPoolCatalogFileName)
            tmpLog.debug('feeding jobs to {0}'.format(jsonFilePath))
            try:
                # put job spec json
                with open(jsonFilePath, 'w') as jsonFile:
                    json.dump({jobSpec.PandaID: jobSpec.jobParams}, jsonFile)
                # put PFC.xml
                with open(xmlFilePath, 'w') as pfcFile:
                    pfcFile.write(pfc)
                # make symlink
                inFiles = jobSpec.get_input_file_attributes()
                for inLFN, inFile in inFiles.iteritems():
                    dstPath = os.path.join(workspec.get_access_point(), inLFN)
                    if inFile['path'] != dstPath:
                        # test if symlink exists if so remove it
                        if os.path.exists(dstPath) :
                            os.unlink(dstPath)
                            tmpLog.debug("removing existing symlink %s" % dstPath)
                        os.symlink(inFile['path'], dstPath)
            except:
                core_utils.dump_error_message(tmpLog)
                retVal = False
        elif workspec.mapType == WorkSpec.MT_MultiJobs:
            # TOBEFIXED
            pass
        # remove request file
        try:
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonJobRequestFileName)
            os.remove(jsonFilePath)
        except:
            pass
        tmpLog.debug('done')
        return retVal

    # request events.
    # * the worker needs to put a json under the access point
    def events_requested(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        # look for the json just under the access point
        jsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsRequestFileName)
        tmpLog.debug('looking for event request file {0}'.format(jsonFilePath))
        if not os.path.exists(jsonFilePath):
            # not found
            tmpLog.debug('not found')
            return {}
        try:
            with open(jsonFilePath) as jsonFile:
                retDict = json.load(jsonFile)
        except:
            tmpLog.debug('failed to load json')
            return {}
        tmpLog.debug('found')
        return retDict

    # feed events
    # * worker_events.json is put under the access point
    def feed_events(self, workspec, events_dict):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        retVal = True
        if workspec.mapType == WorkSpec.MT_OneToOne:
            # put the json just under the access point
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsFeedFileName)
            tmpLog.debug('feeding events to {0}'.format(jsonFilePath))
            try:
                with open(jsonFilePath, 'w') as jsonFile:
                    json.dump(events_dict, jsonFile)
            except:
                core_utils.dump_error_message(tmpLog)
                retVal = False
        elif workspec.mapType == WorkSpec.MT_MultiJobs:
            # TOBEFIXED
            pass
        # remove request file
        try:
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsRequestFileName)
            os.remove(jsonFilePath)
        except:
            pass
        tmpLog.debug('done')
        return retVal

    # update events.
    # * the worker needs to put a json under the access point
    def events_to_update(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        # look for the json just under the access point
        jsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsUpdateFileName)
        readJsonPath = jsonFilePath + suffixReadJson
        # first look for json.read which is not yet acknowledged
        tmpLog.debug('looking for event update file {0}'.format(readJsonPath))
        if os.path.exists(readJsonPath):
            pass
        else:
            tmpLog.debug('looking for event update file {0}'.format(jsonFilePath))
            if not os.path.exists(jsonFilePath):
                # not found
                tmpLog.debug('not found')
                return {}
            try:
                # rename to prevent from being overwritten
                os.rename(jsonFilePath, readJsonPath)
            except:
                tmpLog.error('failed to rename json')
                return {}
        # load json
        try:
            with open(readJsonPath) as jsonFile:
                retDict = json.load(jsonFile)
                newDict = dict()
                # change the key from str to int
                for tmpPandaID, tmpDict in retDict.iteritems():
                    tmpPandaID = long(tmpPandaID)
                    newDict[tmpPandaID] = tmpDict
                retDict = newDict
        except:
            tmpLog.debug('failed to load json')
            return {}
        tmpLog.debug('got {0}'.format(str(retDict)))
        return retDict

    # acknowledge events and files
    # * delete json.read files
    def acknowledge_events_files(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        # remove request file
        try:
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsUpdateFileName)
            jsonFilePath += suffixReadJson
            os.remove(jsonFilePath)
        except:
            pass
        try:
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonOutputsFileName)
            jsonFilePath += suffixReadJson
            os.remove(jsonFilePath)
        except:
            pass
        tmpLog.debug('done')
        return

    # setup access points
    def setup_access_points(self, workspec_list):
        for workSpec in workspec_list:
            accessPoint = workSpec.get_access_point()
            # make the dir if missing
            if not os.path.exists(accessPoint):
                os.makedirs(accessPoint)

    # filter for log.tar.gz
    def filter_log_tgz(self, name):
        for tmpPatt in ['*.log', '*.txt', '*.xml', '*.json', 'log*']:
            if fnmatch.fnmatch(name, tmpPatt):
                return True
        return False

    # post-processing (archiving log files and collecting job metrics)
    def post_processing(self, workspec, jobspec_list, map_type):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        if map_type == WorkSpec.MT_OneToOne:
            jobSpec = jobspec_list[0]
            # check if log is already there
            for fileSpec in jobSpec.outFiles:
                if fileSpec.fileType == 'log':
                    return
            logFileInfo = jobSpec.get_logfile_info()
            # make log.tar.gz
            logFilePath = os.path.join(workspec.get_access_point(), logFileInfo['lfn'])
            tmpLog.debug('making {0}'.format(logFilePath))
            with tarfile.open(logFilePath, "w:gz") as tmpTarFile:
                accessPoint = workspec.get_access_point()
                for tmpRoot, tmpDirs, tmpFiles in os.walk(accessPoint):
                    for tmpFile in tmpFiles:
                        if not self.filter_log_tgz(tmpFile):
                            continue
                        tmpFullPath = os.path.join(tmpRoot, tmpFile)
                        tmpRelPath = re.sub(accessPoint+'/*', '', tmpFullPath)
                        tmpTarFile.add(tmpFullPath, arcname=tmpRelPath)
            # make json to stage-out the log file
            fileDict = dict()
            fileDict[jobSpec.PandaID] = []
            fileDict[jobSpec.PandaID].append({'path': logFilePath,
                                              'type': 'log',
                                              'isZip': 0})
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonOutputsFileName)
            with open(jsonFilePath, 'w') as jsonFile:
                json.dump(fileDict, jsonFile)
        else:
            # FIXME
            pass

    # tell PandaIDs for pull model
    def get_panda_ids(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        # look for the json just under the access point
        jsonFilePath = os.path.join(workspec.get_access_point(), pandaIDsFile)
        tmpLog.debug('looking for PandaID file {0}'.format(jsonFilePath))
        retVal = []
        if not os.path.exists(jsonFilePath):
            # not found
            tmpLog.debug('not found')
            return retVal
        try:
            with open(jsonFilePath) as jsonFile:
                retVal = json.load(jsonFile)
        except:
            tmpLog.debug('failed to load json')
            return retVal
        tmpLog.debug('found')
        return retVal
