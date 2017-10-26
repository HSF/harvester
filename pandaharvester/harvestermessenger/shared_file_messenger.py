import json
import os
import re
import urllib
import uuid
import os.path
import tarfile
import fnmatch
from future.utils import iteritems
from past.builtins import long
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
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

# suffix to read json
suffixReadJson = '.read'

# logger
_logger = core_utils.setup_logger('shared_file_messenger')


def set_logger(master_logger):
    global _logger
    _logger = master_logger


# messenger with shared file system
class SharedFileMessenger(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.jobSpecFileFormat = 'json'
        PluginBase.__init__(self, **kwarg)

    # get access point
    def get_access_point(self, workspec, panda_id):
        if workspec.mapType == WorkSpec.MT_MultiJobs:
            accessPoint = os.path.join(workspec.get_access_point(), str(panda_id))
        else:
            accessPoint = workspec.get_access_point()
        return accessPoint

    # get attributes of a worker which should be propagated to job(s).
    #  * the worker needs to put a json under the access point
    def get_work_attributes(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID),
                                        method_name='get_work_attributes')
        allRetDict = dict()
        for pandaID in workspec.pandaid_list:
            # look for the json just under the access point
            accessPoint = self.get_access_point(workspec, pandaID)
            jsonFilePath = os.path.join(accessPoint, jsonAttrsFileName)
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
            jsonFilePath = os.path.join(accessPoint, jsonJobReport)
            tmpLog.debug('looking for job report file {0}'.format(jsonFilePath))
            if not os.path.exists(jsonFilePath):
                # not found
                tmpLog.debug('not found')
            else:
                try:
                    with open(jsonFilePath) as jsonFile:
                        tmpDict = json.load(jsonFile)
                        retDict['metaData'] = tmpDict
                    tmpLog.debug('got {0} kB of job report'.format(os.stat(jsonFilePath).st_size / 1024))
                except:
                    tmpLog.debug('failed to load {0}'.format(jsonFilePath))
            allRetDict[pandaID] = retDict
        return allRetDict

    # get files to stage-out.
    #  * the worker needs to put a json under the access point
    def get_files_to_stage_out(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID),
                                        method_name='get_files_to_stage_out')
        fileDict = dict()
        # look for the json just under the access point
        for pandaID in workspec.pandaid_list:
            # look for the json just under the access point
            accessPoint = self.get_access_point(workspec, pandaID)
            jsonFilePath = os.path.join(accessPoint, jsonOutputsFileName)
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
                    continue
                try:
                    tmpLog.debug('found')
                    # rename to prevent from being overwritten
                    os.rename(jsonFilePath, readJsonPath)
                except:
                    tmpLog.error('failed to rename json')
                    continue
            # load json
            toSkip = False
            loadDict = None
            try:
                with open(readJsonPath) as jsonFile:
                    loadDict = json.load(jsonFile)
            except:
                tmpLog.error('failed to load json')
                toSkip = True
            # test validity of data format (ie it should be a Dictionary)
            if not toSkip:
                if not isinstance(loadDict, dict):
                    tmpLog.error('loaded data is not a dictionary')
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
                        tmpLog.error('loaded data item is not a list')
                        toSkip = True
                        break
                    for tmpEventInfo in tmpEventMapList:
                        try:
                            nData += 1
                            if 'eventRangeID' in tmpEventInfo:
                                tmpEventRangeID = tmpEventInfo['eventRangeID']
                            else:
                                tmpEventRangeID = None
                            tmpFileDict = dict()
                            pfn = tmpEventInfo['path']
                            lfn = os.path.basename(pfn)
                            tmpFileDict['path'] = pfn
                            if pfn not in sizeMap:
                                if 'fsize' in tmpEventInfo:
                                    sizeMap[pfn] = tmpEventInfo['fsize']
                                else:
                                    sizeMap[pfn] = os.stat(pfn).st_size
                            tmpFileDict['fsize'] = sizeMap[pfn]
                            tmpFileDict['type'] = tmpEventInfo['type']
                            if tmpEventInfo['type'] in ['log', 'output']:
                                # disable zipping
                                tmpFileDict['isZip'] = 0
                            elif tmpEventInfo['type'] == 'zip_output':
                                # already zipped
                                tmpFileDict['isZip'] = 1
                            elif 'isZip' in tmpEventInfo:
                                tmpFileDict['isZip'] = tmpEventInfo['isZip']
                            # guid
                            if 'guid' in tmpEventInfo:
                                tmpFileDict['guid'] = tmpEventInfo['guid']
                            else:
                                tmpFileDict['guid'] = str(uuid.uuid4())
                            # get checksum
                            if pfn not in chksumMap:
                                if 'chksum' in tmpEventInfo:
                                    chksumMap[pfn] = tmpEventInfo['chksum']
                                else:
                                    chksumMap[pfn] = core_utils.calc_adler32(pfn)
                            tmpFileDict['chksum'] = chksumMap[pfn]
                            if tmpPandaID not in fileDict:
                                fileDict[tmpPandaID] = dict()
                            if lfn not in fileDict[tmpPandaID]:
                                fileDict[tmpPandaID][lfn] = []
                            fileDict[tmpPandaID][lfn].append(tmpFileDict)
                            # skip if unrelated to events
                            if tmpFileDict['type'] not in ['es_output', 'zip_output']:
                                continue
                            tmpFileDict['eventRangeID'] = tmpEventRangeID
                            if tmpPandaID not in eventsList:
                                eventsList[tmpPandaID] = list()
                            eventsList[tmpPandaID].append({'eventRangeID': tmpEventRangeID,
                                                           'eventStatus': tmpEventInfo['eventStatus']})
                        except:
                            core_utils.dump_error_message(tmpLog)
                # dump events
                if not toSkip:
                    if len(eventsList) > 0:
                        curName = os.path.join(accessPoint, jsonEventsUpdateFileName)
                        newName = curName + '.new'
                        f = open(newName, 'w')
                        json.dump(eventsList, f)
                        f.close()
                        os.rename(newName, curName)
            # remove empty file
            if toSkip or nData == 0:
                try:
                    os.remove(readJsonPath)
                except:
                    pass
            tmpLog.debug('got {0} files for PandaID={1}'.format(nData, pandaID))
        return fileDict

    # check if job is requested.
    # * the worker needs to put a json under the access point
    def job_requested(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID),
                                        method_name='job_requested')
        # look for the json just under the access point
        jsonFilePath = os.path.join(workspec.get_access_point(), jsonJobRequestFileName)
        tmpLog.debug('looking for job request file {0}'.format(jsonFilePath))
        if not os.path.exists(jsonFilePath):
            # not found
            tmpLog.debug('not found')
            return False
        # read nJobs
        try:
            with open(jsonFilePath) as jsonFile:
                tmpDict = json.load(jsonFile)
                nJobs = tmpDict['nJobs']
        except:
            # request 1 job by default
            nJobs = 1
        tmpLog.debug('requesting {0} jobs'.format(nJobs))
        return nJobs

    # feed jobs
    # * worker_jobspec.json is put under the access point
    def feed_jobs(self, workspec, jobspec_list):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID),
                                        method_name='feed_jobs')
        retVal = True
        # get PFC
        pfc = core_utils.make_pool_file_catalog(jobspec_list)
        pandaIDs = []
        for jobSpec in jobspec_list:
            accessPoint = self.get_access_point(workspec, jobSpec.PandaID)
            jobSpecFilePath = os.path.join(accessPoint, jobSpecFileName)
            xmlFilePath = os.path.join(accessPoint, xmlPoolCatalogFileName)
            tmpLog.debug('feeding jobs to {0}'.format(jobSpecFilePath))
            try:
                # put job spec file
                with open(jobSpecFilePath, 'w') as jobSpecFile:
                    if self.jobSpecFileFormat == 'cgi':
                        jobSpecFile.write(urllib.urlencode(jobSpec.jobParams))
                    else:
                        json.dump({jobSpec.PandaID: jobSpec.jobParams}, jobSpecFile)
                # put PFC.xml
                with open(xmlFilePath, 'w') as pfcFile:
                    pfcFile.write(pfc)
                # make symlink
                inFiles = jobSpec.get_input_file_attributes()
                for inLFN, inFile in iteritems(inFiles):
                    dstPath = os.path.join(accessPoint, inLFN)
                    if inFile['path'] != dstPath:
                        # test if symlink exists if so remove it
                        if os.path.exists(dstPath):
                            os.unlink(dstPath)
                            tmpLog.debug("removing existing symlink %s" % dstPath)
                        os.symlink(inFile['path'], dstPath)
                pandaIDs.append(jobSpec.PandaID)
            except:
                core_utils.dump_error_message(tmpLog)
                retVal = False
        # put PandaIDs file
        jsonFilePath = os.path.join(workspec.get_access_point(), pandaIDsFile)
        with open(jsonFilePath, 'w') as jsonPandaIDsFile:
            json.dump(pandaIDs, jsonPandaIDsFile)
        # remove request file
        try:
            reqFilePath = os.path.join(workspec.get_access_point(), jsonJobRequestFileName)
            os.remove(reqFilePath)
        except:
            pass
        tmpLog.debug('done')
        return retVal

    # request events.
    # * the worker needs to put a json under the access point
    def events_requested(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID),
                                        method_name='events_requested')
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
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID),
                                        method_name='feed_events')
        retVal = True
        if workspec.mapType in [WorkSpec.MT_OneToOne, WorkSpec.MT_MultiWorkers]:
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
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID),
                                        method_name='events_to_update')
        # look for the json just under the access point
        retDict = dict()
        for pandaID in workspec.pandaid_list:
            # look for the json just under the access point
            accessPoint = self.get_access_point(workspec, pandaID)

            jsonFilePath = os.path.join(accessPoint, jsonEventsUpdateFileName)
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
                    continue
                try:
                    # rename to prevent from being overwritten
                    os.rename(jsonFilePath, readJsonPath)
                except:
                    tmpLog.error('failed to rename json')
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
                        nData += 1
            except:
                tmpLog.error('failed to load json')
            # delete empty file
            if nData == 0:
                try:
                    os.remove(readJsonPath)
                except:
                    pass
            tmpLog.debug('got {0} events for PandaID={1}'.format(nData, pandaID))
        return retDict

    # acknowledge events and files
    # * delete json.read files
    def acknowledge_events_files(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID),
                                        method_name='acknowledge_events_files')
        # remove request file
        for pandaID in workspec.pandaid_list:
            accessPoint = self.get_access_point(workspec, pandaID)
            try:
                jsonFilePath = os.path.join(accessPoint, jsonEventsUpdateFileName)
                jsonFilePath += suffixReadJson
                os.remove(jsonFilePath)
            except:
                pass
            try:
                jsonFilePath = os.path.join(accessPoint, jsonOutputsFileName)
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
            for jobSpec in workSpec.get_jobspec_list():
                subAccessPoint = self.get_access_point(workSpec, jobSpec.PandaID)
                if accessPoint != subAccessPoint:
                    if not os.path.exists(subAccessPoint):
                        os.mkdir(subAccessPoint)

    # filter for log.tar.gz
    def filter_log_tgz(self, name):
        for tmpPatt in ['*.log', '*.txt', '*.xml', '*.json', 'log*']:
            if fnmatch.fnmatch(name, tmpPatt):
                return True
        return False

    # post-processing (archiving log files and collecting job metrics)
    def post_processing(self, workspec, jobspec_list, map_type):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID),
                                        method_name='post_processing')
        try:
            for jobSpec in jobspec_list:
                # check if log is already there
                for fileSpec in jobSpec.outFiles:
                    if fileSpec.fileType == 'log':
                        continue
                logFileInfo = jobSpec.get_logfile_info()
                # make log.tar.gz
                accessPoint = self.get_access_point(workspec, jobSpec.PandaID)
                logFilePath = os.path.join(accessPoint, logFileInfo['lfn'])
                if map_type == WorkSpec.MT_MultiWorkers:
                    # append suffix
                    logFilePath += '.{0}'.format(workspec.workerID)
                tmpLog.debug('making {0}'.format(logFilePath))
                with tarfile.open(logFilePath, "w:gz") as tmpTarFile:
                    for tmpFile in os.listdir(accessPoint):
                        if not self.filter_log_tgz(tmpFile):
                            continue
                        tmpFullPath = os.path.join(accessPoint, tmpFile)
                        if not os.path.isfile(tmpFullPath):
                            continue
                        tmpRelPath = re.sub(accessPoint+'/*', '', tmpFullPath)
                        tmpTarFile.add(tmpFullPath, arcname=tmpRelPath)
                # make json to stage-out the log file
                fileDict = dict()
                fileDict[jobSpec.PandaID] = []
                fileDict[jobSpec.PandaID].append({'path': logFilePath,
                                                  'type': 'log',
                                                  'isZip': 0})
                jsonFilePath = os.path.join(accessPoint, jsonOutputsFileName)
                with open(jsonFilePath, 'w') as jsonFile:
                    json.dump(fileDict, jsonFile)
            return True
        except:
            core_utils.dump_error_message(tmpLog)
            return False

    # get PandaIDs for pull model
    def get_panda_ids(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID),
                                        method_name='get_panda_ids')
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
