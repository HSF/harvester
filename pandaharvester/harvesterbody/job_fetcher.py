import socket
import datetime

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvesterbody.agent_base import AgentBase

# logger
_logger = core_utils.setup_logger()


# class to fetch jobs
class JobFetcher(AgentBase):
    # constructor
    def __init__(self, communicator, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.communicator = communicator
        self.nodeName = socket.gethostname()
        self.queueConfigMapper = queue_config_mapper

    # main loop
    def run(self):
        while True:
            mainLog = core_utils.make_logger(_logger, 'id={0}'.format(self.ident))
            mainLog.debug('getting number of jobs to be fetched')
            # get number of jobs to be fetched
            nJobsPerQueue = self.dbProxy.get_num_jobs_to_fetch(harvester_config.jobfetcher.nQueues,
                                                               harvester_config.jobfetcher.lookupTime)
            mainLog.debug('got {0} queues'.format(len(nJobsPerQueue)))
            # loop over all queues
            for queueName, nJobs in nJobsPerQueue.iteritems():
                # check queue
                if not self.queueConfigMapper.has_queue(queueName):
                    continue
                tmpLog = core_utils.make_logger(_logger, 'queueName={0}'.format(queueName))
                # get queue
                queueConfig = self.queueConfigMapper.get_queue(queueName)
                # upper limit
                if nJobs > harvester_config.jobfetcher.maxJobs:
                    nJobs = harvester_config.jobfetcher.maxJobs
                # get jobs
                tmpLog.debug('getting {0} jobs'.format(nJobs))
                siteName = queueName.split('/')[0]
                if siteName == queueName:
                    resourceType = None
                else:
                    resourceType = queueName.split('/')[-1]
                jobs, errStr = self.communicator.get_jobs(siteName, self.nodeName,
                                                          queueConfig.prodSourceLabel,
                                                          self.nodeName, nJobs)
                tmpLog.debug('got {0} jobs with {1}'.format(len(jobs), errStr))
                # convert to JobSpec
                if len(jobs) > 0:
                    jobSpecs = []
                    for job in jobs:
                        timeNow = datetime.datetime.utcnow()
                        jobSpec = JobSpec()
                        jobSpec.convert_job_json(job)
                        jobSpec.computingSite = queueName
                        jobSpec.status = 'starting'
                        jobSpec.subStatus = 'fetched'
                        jobSpec.creationTime = timeNow
                        jobSpec.stateChangeTime = timeNow
                        if queueConfig.zipPerMB is not None and jobSpec.zipPerMB is None:
                            jobSpec.zipPerMB = queueConfig.zipPerMB
                        jobSpec.trigger_propagation()
                        jobSpecs.append(jobSpec)
                    # insert to DB
                    self.dbProxy.insert_jobs(jobSpecs)
            mainLog.debug('done')
            # check if being terminated
            if self.terminated(harvester_config.jobfetcher.sleepTime):
                mainLog.debug('terminated')
                return
