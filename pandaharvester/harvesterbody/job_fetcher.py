import socket
import datetime

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.file_spec import FileSpec
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
                siteName = queueConfig.siteName
                jobs, errStr = self.communicator.get_jobs(siteName, self.nodeName,
                                                          queueConfig.prodSourceLabel,
                                                          self.nodeName, nJobs,
                                                          queueConfig.getJobCriteria)
                tmpLog.debug('got {0} jobs with {1}'.format(len(jobs), errStr))
                # convert to JobSpec
                if len(jobs) > 0:
                    jobSpecs = []
                    fileStatMap = dict()
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
                        for tmpLFN in jobSpec.get_input_file_attributes().keys():
                            # check file status
                            if tmpLFN not in fileStatMap:
                                fileStatMap[tmpLFN] = self.dbProxy.get_file_status(tmpLFN, 'input',
                                                                                   queueConfig.ddmEndpointIn)
                            # make file spec
                            fileSpec = FileSpec()
                            fileSpec.PandaID = jobSpec.PandaID
                            fileSpec.taskID = jobSpec.taskID
                            fileSpec.lfn = tmpLFN
                            fileSpec.endpoint = queueConfig.ddmEndpointIn
                            # set preparing to skip stage-in if the file is (being) taken care of by another job
                            if 'ready' in fileStatMap[tmpLFN] or 'preparing' in fileStatMap[tmpLFN] \
                                    or 'to_prepare' in fileStatMap[tmpLFN]:
                                fileSpec.status = 'preparing'
                            else:
                                fileSpec.status = 'to_prepare'
                            if fileSpec.status not in fileStatMap[tmpLFN]:
                                fileStatMap[tmpLFN][fileSpec.status] = 0
                            fileStatMap[tmpLFN][fileSpec.status] += 1
                            fileSpec.fileType = 'input'
                            jobSpec.add_in_file(fileSpec)
                        jobSpec.trigger_propagation()
                        jobSpecs.append(jobSpec)
                    # insert to DB
                    self.dbProxy.insert_jobs(jobSpecs)
            mainLog.debug('done')
            # check if being terminated
            if self.terminated(harvester_config.jobfetcher.sleepTime):
                mainLog.debug('terminated')
                return
