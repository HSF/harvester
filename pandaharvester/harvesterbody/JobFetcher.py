import socket
import datetime
import threading

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.JobSpec import JobSpec
from pandaharvester.harvestercore.DBProxy import DBProxy

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('JobFetcher')


# class to fetch jobs
class JobFetcher (threading.Thread):
    
    # constructor
    def __init__(self,communicator,queueConfigMapper,singleMode=False):
        threading.Thread.__init__(self)
        self.dbProxy = DBProxy()
        self.communicator = communicator
        self.nodeName = socket.gethostname()
        self.queueConfigMapper = queueConfigMapper
        self.singleMode = singleMode



    # main loop
    def run (self):
        while True:
            mainLog = CoreUtils.makeLogger(_logger,'id={0}'.format(self.ident))
            mainLog.debug('getting number of jobs to be fetched')
            # get number of jobs to be fetched
            nJobsPerQueue = self.dbProxy.getNumJobsToFetch(harvester_config.jobfetch.nQueues,
                                                           harvester_config.jobfetch.lookupTime)
            mainLog.debug('got {0} queues'.format(len(nJobsPerQueue)))
            # loop over all queues
            for queueName,nJobs in nJobsPerQueue.iteritems():
                # check queue
                if not self.queueConfigMapper.hasQueue(queueName):
                    continue
                tmpLog = CoreUtils.makeLogger(_logger,'queueName={0}'.format(queueName))
                # get queue
                queueConfig = self.queueConfigMapper.getQueue(queueName)
                # upper limit
                if nJobs > harvester_config.jobfetch.maxJobs:
                    nJobs = harvester_config.jobfetch.maxJobs
                # get jobs
                tmpLog.debug('getting {0} jobs'.format(nJobs))
                jobs = self.communicator.getJobs(queueName,self.nodeName,
                                                 queueConfig.prodSourceLabel,
                                                 self.nodeName,nJobs)
                tmpLog.debug('got {0} jobs'.format(len(jobs)))
                # convert to JobSpec
                if len(jobs) > 0:
                    jobSpecs = []
                    for job in jobs:
                        timeNow = datetime.datetime.utcnow()
                        jobSpec = JobSpec()
                        jobSpec.convertJobJson(job)
                        jobSpec.computingSite = queueName
                        jobSpec.status = 'starting'
                        jobSpec.subStatus = 'fetched'
                        jobSpec.creationTime = timeNow
                        jobSpec.stateChangeTime = timeNow
                        jobSpec.triggerPropagation()
                        jobSpecs.append(jobSpec)
                    # insert to DB
                    self.dbProxy.insertJobs(jobSpecs)
            mainLog.debug('done')
            if self.singleMode:
                return
            # sleep
            CoreUtils.sleep(harvester_config.jobfetch.sleepTime)

