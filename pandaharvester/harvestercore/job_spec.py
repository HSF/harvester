"""
Job spec class

"""

import copy
import datetime

from spec_base import SpecBase


class JobSpec(SpecBase):
    # has output file
    HO_noOutput = 0
    HO_hasOutput = 1
    HO_hasZipOutput = 2
    HO_hasTransfer = 3

    # attributes
    attributesWithTypes = ('PandaID:integer primary key',
                           'taskID:integer',
                           'attemptNr:integer',
                           'status:text',
                           'subStatus:text',
                           'currentPriority:integer',
                           'computingSite:text',
                           'creationTime:timestamp',
                           'modificationTime:timestamp',
                           'stateChangeTime:timestamp',
                           'startTime:timestamp',
                           'endTime:timestamp',
                           'nCore:integer',
                           'jobParams:blob',
                           'jobAttributes:blob',
                           'hasOutFile:integer',
                           'metaData:blob',
                           'outputFilesToReport:blob',
                           'lockedBy:text',
                           'propagatorLock:text',
                           'propagatorTime:timestamp',
                           'preparatorTime:timestamp',
                           'submitterTime:timestamp',
                           'stagerLock:text',
                           'stagerTime:timestamp',
                           'zipPerMB:integer',
                           'nWorkers:integer',
                           'nWorkersLimit:integer',
                           )

    # attributes initialized with 0
    zeroAttrs = ('nWorkers',
                 )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
        object.__setattr__(self, 'events', set())
        object.__setattr__(self, 'zipEventMap', {})
        object.__setattr__(self, 'outFiles', set())
        object.__setattr__(self, 'zipFileMap', {})

    # add output file
    def add_out_file(self, filespec):
        self.outFiles.add(filespec)

    # reset output file list
    def reset_out_file(self):
        self.outFiles.clear()

    # add event
    def add_event(self, eventspec, zip_filespec):
        if zip_filespec is None:
            zipFileID = None
        else:
            zipFileID = zip_filespec.fileID
        if not zipFileID in self.zipEventMap:
            self.zipEventMap[zipFileID] = {'events': set(),
                                           'zip': zip_filespec}
        self.zipEventMap[zipFileID]['events'].add(eventspec)
        self.events.add(eventspec)

    # convert from Job JSON
    def convert_job_json(self, data):
        self.PandaID = data['PandaID']
        self.taskID = data['taskID']
        self.attemptNr = data['attemptNr']
        self.currentPriority = data['currentPriority']
        self.jobParams = data
        if 'zipPerMB' in data:
            self.zipPerMB = data['zipPerMB']

    # trigger propagation
    def trigger_propagation(self):
        self.propagatorTime = datetime.datetime.utcnow() - datetime.timedelta(hours=1)

    # set attributes
    def set_attributes(self, attrs):
        if attrs is None:
            return
        attrs = copy.copy(attrs)
        # set metadata and outputs to dedicated attributes
        if 'metadata' in attrs:
            self.metaData = attrs['metadata']
            del attrs['metadata']
        if 'xml' in attrs:
            self.outputFilesToReport = attrs['xml']
            del attrs['xml']
        self.jobAttributes = attrs

    # check if final status
    def is_final_status(self, job_status=None):
        if job_status is None:
            job_status = self.status
        return job_status in ['finished', 'failed', 'cancelled']

    # get status
    def get_status(self):
        # don't report the final status while staging-out
        if self.is_final_status() and (self.subStatus in ['to_transfer', 'transferring'] or not self.all_events_done()):
            return 'transferring'
        return self.status

    # check if all events are done
    def all_events_done(self):
        retVal = True
        for eventSpec in self.events:
            if eventSpec.subStatus != 'done':
                retVal = False
                break
        return retVal

    # all files are triggered to stage-out
    def all_files_triggered_to_stage_out(self):
        for fileSpec in self.outFiles:
            if fileSpec.status not in ['finished', 'failed']:
                fileSpec.status = 'transferring'

    # all files are zipped
    def all_files_zipped(self):
        for fileSpec in self.outFiles:
            if fileSpec.status not in ['finished', 'failed']:
                fileSpec.status = 'defined'

    # convert to event data
    def to_event_data(self):
        data = []
        eventSpecs = []
        for zipFileID, eventsData in self.zipEventMap.iteritems():
            eventRanges = []
            for eventSpec in eventsData['events']:
                eventRanges.append(eventSpec.to_data())
                eventSpecs.append(eventSpec)
            tmpData = {}
            tmpData['eventRanges'] = eventRanges
            if zipFileID is not None:
                zipFileSpec = eventsData['zip']
                tmpData['zipFile'] = {'lfn': zipFileSpec.lfn,
                                      'objstoreID': zipFileSpec.objstoreID}
            data.append(tmpData)
        return data, eventSpecs

    # get input file attributes
    def get_input_file_attributes(self):
        inFiles = {}
        lfns = self.jobParams['inFiles'].split(',')
        guids = self.jobParams['GUID'].split(',')
        fsizes = self.jobParams['fsize'].split(',')
        chksums = self.jobParams['checksum'].split(',')
        scopes = self.jobParams['scopeIn'].split(',')
        datasets = self.jobParams['realDatasetsIn'].split(',')
        endpoints = self.jobParams['ddmEndPointIn'].split(',')
        for lfn, guid, fsize, chksum, scope, dataset, endpoint in \
                zip(lfns, guids, fsizes, chksums, scopes, datasets, endpoints):
            try:
                fsize = long(fsize)
            except:
                fsize = None
            inFiles[lfn] = {'fsize': fsize,
                            'guid': guid,
                            'checksum': chksum,
                            'scope': scope,
                            'dataset': dataset,
                            'endpoint': endpoint}
        # add path
        if 'inFilePaths' in self.jobParams:
            paths = self.jobParams['inFilePaths'].split(',')
            for lfn, path in zip(lfns, paths):
                inFiles[lfn]['path'] = path
        return inFiles

    # set input file paths
    def set_input_file_paths(self, in_files):
        lfns = self.jobParams['inFiles'].split(',')
        paths = []
        for lfn in lfns:
            paths.append(in_files[lfn]['path'])
        self.jobParams['inFilePaths'] = ','.join(paths)
        # trigger updating
        self.force_update('jobParams')

    # get output file attributes
    def get_output_file_attributes(self):
        outFiles = {}
        lfns = self.jobParams['outFiles'].split(',')
        scopes = self.jobParams['scopeOut'].split(',')
        scopeLog = self.jobParams['scopeLog']
        logLFN = self.jobParams['logFile']
        scopes.insert(lfns.index(logLFN), scopeLog)
        datasets = self.jobParams['realDatasets'].split(',')
        endpoints = self.jobParams['ddmEndPointOut'].split(',')
        for lfn, scope, dataset, endpoint in zip(lfns, scopes, datasets, endpoints):
            outFiles[lfn] = {'scope': scope,
                             'dataset': dataset,
                             'endpoint': endpoint}
        return outFiles

    # get log file information
    def get_logfile_info(self):
        retMap = dict()
        retMap['lfn'] = self.jobParams['logFile']
        retMap['guid'] = self.jobParams['logGUID']
        return retMap

    # set start time
    def set_start_time(self, force=False):
        if self.startTime is None or force is True:
            self.startTime = datetime.datetime.utcnow()

    # set end time
    def set_end_time(self, force=False):
        if self.endTime is None or force is True:
            self.endTime = datetime.datetime.utcnow()
