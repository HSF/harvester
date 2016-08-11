"""
Work spec class

"""

from SpecBase import SpecBase


# work spec
class WorkSpec(SpecBase):

    # worker statues
    ST_submitted = 'submitted'
    ST_queued    = 'queued'
    ST_running   = 'running'
    ST_finished  = 'finished'
    ST_failed    = 'failed'
    ST_cancelled = 'cancelled'

    # list of worker statuses
    ST_LIST = [ST_submitted,
               ST_queued,
               ST_running,
               ST_finished,
               ST_failed,
               ST_cancelled]

    # type of mapping between job and worker
    MT_NoJob        = 'NoJob'
    MT_OneToOne     = 'OneToOne'
    MT_MultiJobs    = 'MultiJobs'
    MT_MultiWorkers = 'MultiWorkers'

    # attributes
    attributesWithTypes = ('workerID:integer primary key',
                           'batchID:text',
                           'mapType:text',
                           'queueName:text',
                           'status:text',
                           'workParams:blob',
                           'workAttributes:blob',
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
                           'lockedBy:text'
                           )



    # constructor
    def __init__(self):
        SpecBase.__init__(self)



    # get accesspoint
    def getAccessPoint(self):
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



    # convert to job status
    def convertJobStatus(self):
        if self.status in ['submitted','queued']:
            return 'starting'
        if self.status in ['finished','failed','cancelled']:
            return self.status
        return 'running'



