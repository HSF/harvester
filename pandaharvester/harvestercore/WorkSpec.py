"""
Work spec class

"""

import re
from SpecBase import SpecBase


# work spec
class WorkSpec(SpecBase):

    # worker statuses
    ST_submitted = 'submitted'
    ST_running   = 'running'
    ST_finished  = 'finished'
    ST_failed    = 'failed'
    ST_ready     = 'ready'
    ST_cancelled = 'cancelled'

    # list of worker statuses
    ST_LIST = [ST_submitted,
               ST_running,
               ST_finished,
               ST_failed,
               ST_ready,
               ST_cancelled]

    # type of mapping between job and worker
    MT_NoJob        = 'NoJob'
    MT_OneToOne     = 'OneToOne'
    MT_MultiJobs    = 'MultiJobs'
    MT_MultiWorkers = 'MultiWorkers'

    # events
    EV_noEvents      = 0
    EV_useEvents     = 1
    EV_requestEvents = 2
    
    # attributes
    attributesWithTypes = ('workerID:integer primary key',
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
                           'lockedBy:text'
                           )



    # constructor
    def __init__(self):
        SpecBase.__init__(self)



    # get accesspoint
    def getAccessPoint(self):
        # replace placeholders
        if '$' in self.accessPoint:
            patts = re.findall('\$\{([a-zA-Z\d]+)\}',self.accessPoint)
            for patt in patts:
                if hasattr(self,patt):
                    tmpVar = getattr(self,patt)
                    tmpKey = '${' + patt + '}'
                    self.accessPoint = self.accessPoint.replace(tmpKey,tmpVar)
        return self.accessPoint



    # set status to submitted
    def setSubmitted(self):
        self.status = 'submitted'



    # set status to running
    def setRunning(self):
        self.status = 'running'



    # set status to finished
    def setFinished(self):
        self.status = 'finished'



    # set status to failed
    def setFailed(self):
        self.status = 'failed'



    # set status to cancelled
    def setCancelled(self):
        self.status = 'cancelled'



    # convert worker status to job status
    def convertToJobStatus(self):
        if self.status in [self.ST_submitted,self.ST_ready]:
            jobStatus = 'starting'
            jobSubStatus = self.status
        elif self.status in [self.ST_finished,self.ST_failed,self.ST_cancelled]:
            jobStatus = self.status
            jobSubStatus = 'totransfer'
        else:
            jobStatus = 'running'
            jobSubStatus = self.status
        return jobStatus,jobSubStatus



