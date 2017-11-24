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
    ST_submitted = 'submitted'
    ST_running = 'running'
    ST_finished = 'finished'
    ST_failed = 'failed'
    ST_ready = 'ready'
    ST_cancelled = 'cancelled'
    ST_idle = 'idle'
    ST_missed = 'missed'

    # list of worker statuses
    ST_LIST = [ST_submitted,
               ST_running,
               ST_finished,
               ST_failed,
               ST_ready,
               ST_cancelled,
               ST_idle,
               ST_missed]

    # type of mapping between job and worker
    MT_NoJob = 'NoJob'
    MT_OneToOne = 'OneToOne'
    MT_MultiJobs = 'ManyToOne'
    MT_MultiWorkers = 'OneToMany'

    # events
    EV_noEvents = 0
    EV_useEvents = 1
    EV_requestEvents = 2

    # attributes
    attributesWithTypes = ('workerID:integer primary key',
                           'batchID:text',
                           'mapType:text',
                           'queueName:text',
                           'status:text / index',
                           'hasJob:integer',
                           'workParams:blob',
                           'workAttributes:blob',
                           'eventsRequestParams:blob',
                           'eventsRequest:integer',
                           'computingSite:text / index',
                           'creationTime:timestamp',
                           'submitTime:timestamp / index',
                           'startTime:timestamp',
                           'endTime:timestamp',
                           'nCore:integer',
                           'walltime:timestamp',
                           'accessPoint:text',
                           'modificationTime:timestamp / index',
                           'lastUpdate:timestamp / index',
                           'eventFeedTime:timestamp / index',
                           'lockedBy:text',
                           'postProcessed:integer',
                           'nodeID:text',
                           'minRamCount:integer',
                           'maxDiskCount:integer',
                           'maxWalltime:integer',
                           'killTime:timestamp / index',
                           'computingElement:text',
                           'nJobsToReFill:integer',
                           'logFilesToUpload:blob'
                           )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
        object.__setattr__(self, 'isNew', False)
        object.__setattr__(self, 'nextLookup', False)
        object.__setattr__(self, 'jobspec_list', None)
        object.__setattr__(self, 'pandaid_list', None)
        object.__setattr__(self, 'new_status', False)

    # set status
    def set_status(self, value):
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
        if '$' in self.accessPoint:
            patts = re.findall('\$\{([a-zA-Z\d]+)\}', self.accessPoint)
            for patt in patts:
                if hasattr(self, patt):
                    tmpVar = str(getattr(self, patt))
                    tmpKey = '${' + patt + '}'
                    self.accessPoint = self.accessPoint.replace(tmpKey, tmpVar)
        return self.accessPoint

    # set job spec list
    def set_jobspec_list(self, jobspec_list):
        self.jobspec_list = jobspec_list

    # get job spec list
    def get_jobspec_list(self):
        return self.jobspec_list

    # convert worker status to job status
    def convert_to_job_status(self, status=None):
        if status is None:
            status = self.status
        if status in [self.ST_submitted, self.ST_ready]:
            jobStatus = 'starting'
            jobSubStatus = status
        elif status in [self.ST_finished, self.ST_failed, self.ST_cancelled]:
            jobStatus = status
            jobSubStatus = 'to_transfer'
        elif status in [self.ST_missed]:
            jobStatus = 'failed'
            jobSubStatus = status
        else:
            jobStatus = 'running'
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
        self.force_update('lastUpdate')

    # final status
    def is_final_status(self):
        return self.status in [self.ST_finished, self.ST_failed, self.ST_cancelled, self.ST_missed]

    # convert to propagate
    def convert_to_propagate(self):
        data = dict()
        for attr in ['workerID',
                     'batchID',
                     'queueName',
                     'status',
                     'computingSite',
                     'nCore',
                     'nodeID',
                     'submitTime',
                     'startTime',
                     'endTime'
                     ]:
            val = getattr(self, attr)
            if val is not None:
                if isinstance(val, datetime.datetime):
                    val = 'datetime/' + val.strftime('%Y-%m-%d %H:%M:%S.%f')
                data[attr] = val
        if self.pandaid_list is not None:
            data['pandaid_list'] = self.pandaid_list
        if self.workAttributes is not None:
            for attr in ['stdOut',
                         'stdErr',
                         'batchLog'
                         ]:
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

    # set work attributes
    def set_work_attributes(self, data):
        if self.workAttributes is None and data is not None:
            self.workAttributes = dict()
        for key, val in iteritems(data):
            if key not in self.workAttributes or self.workAttributes[key] != val:
                self.workAttributes[key] = val
                self.force_update('workAttributes')

    # update log files to upload
    def update_log_files_to_upload(self, file_path, position, remote_name=None):
        if self.logFilesToUpload is None:
            self.logFilesToUpload = dict()
        if file_path not in self.logFilesToUpload:
            self.logFilesToUpload[file_path] = {'position': position,
                                                'remote_name': remote_name}
            self.force_update('logFilesToUpload')
        elif self.logFilesToUpload[file_path]['position'] != position:
            self.logFilesToUpload[file_path]['position'] = position
            self.force_update('logFilesToUpload')

    # set log file
    def set_log_file(self, log_type, stream):
        if log_type == 'stdout':
            keyName = 'stdOut'
        elif log_type == 'stderr':
            keyName = 'stdErr'
        else:
            keyName = 'batchLog'
        if stream.startswith('http'):
            url = stream
        else:
            remoteName = '{0}__{1}'.format(harvester_config.master.harvester_id,
                                           os.path.basename(stream))
            url = '{0}/{1}'.format(harvester_config.pandacon.pandaCacheURL_R,
                                   remoteName)
            # set file to periodically upload
            self.update_log_files_to_upload(stream, 0, remoteName)
        self.set_work_attributes({keyName: url})

    # get the list of log files to upload
    def get_log_files_to_upload(self):
        retList = []
        if self.logFilesToUpload is not None:
            for filePath, fileInfo in iteritems(self.logFilesToUpload):
                if not os.path.exists(filePath):
                    continue
                fileSize = os.stat(filePath).st_size
                if fileSize <= fileInfo['position']:
                    continue
                retList.append((filePath, fileInfo['position'], fileSize-fileInfo['position'],
                                fileInfo['remote_name']))
        return retList
