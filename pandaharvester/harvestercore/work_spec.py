"""
Work spec class

"""

import re
import datetime
from spec_base import SpecBase


# work spec
class WorkSpec(SpecBase):
    # worker statuses
    ST_submitted = 'submitted'
    ST_running = 'running'
    ST_finished = 'finished'
    ST_failed = 'failed'
    ST_ready = 'ready'
    ST_cancelled = 'cancelled'

    # list of worker statuses
    ST_LIST = [ST_submitted,
               ST_running,
               ST_finished,
               ST_failed,
               ST_ready,
               ST_cancelled]

    # type of mapping between job and worker
    MT_NoJob = 'NoJob'
    MT_OneToOne = 'OneToOne'
    MT_MultiJobs = 'MultiJobs'
    MT_MultiWorkers = 'MultiWorkers'

    # events
    EV_noEvents = 0
    EV_useEvents = 1
    EV_requestEvents = 2

    # attributes
    attributesWithTypes = ('workerID:integer',
                           'batchID:text',
                           'mapType:text',
                           'queueName:text',
                           'status:text',
                           'hasJob:integer',
                           'workParams:blob',
                           'workAttributes:blob',
                           'eventsRequestParams:blob',
                           'eventsRequest:integer',
                           'computingSite:text',
                           'creationTime:timestamp',
                           'submitTime:timestamp',
                           'startTime:timestamp',
                           'endTime:timestamp',
                           'nCore:integer',
                           'walltime:timestamp',
                           'accessPoint:text',
                           'modificationTime:timestamp',
                           'stateChangeTime:timestamp',
                           'eventFeedTime:timestamp',
                           'lockedBy:text',
                           'postProcessed:integer'
                           )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
        object.__setattr__(self, 'isNew', False)
        object.__setattr__(self, 'nextLookup', False)
        object.__setattr__(self, 'jobspec_list', None)

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

    # set status to submitted
    def set_submitted(self):
        self.status = 'submitted'

    # set status to running
    def set_running(self):
        self.status = 'running'

    # set status to finished
    def set_finished(self):
        self.status = 'finished'

    # set status to failed
    def set_failed(self):
        self.status = 'failed'

    # set status to cancelled
    def set_cancelled(self):
        self.status = 'cancelled'

    # convert worker status to job status
    def convert_to_job_status(self):
        if self.status in [self.ST_submitted, self.ST_ready]:
            jobStatus = 'starting'
            jobSubStatus = self.status
        elif self.status in [self.ST_finished, self.ST_failed, self.ST_cancelled]:
            jobStatus = self.status
            jobSubStatus = 'to_transfer'
        else:
            jobStatus = 'running'
            jobSubStatus = self.status
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
