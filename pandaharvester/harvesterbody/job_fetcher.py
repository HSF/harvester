import socket
import datetime
import random
from future.utils import iteritems

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvestercore.plugin_factory import PluginFactory

# logger
_logger = core_utils.setup_logger('job_fetcher')


# class to fetch jobs
class JobFetcher(AgentBase):
    # constructor
    def __init__(self, communicator, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.communicator = communicator
        self.nodeName = socket.gethostname()
        self.queueConfigMapper = queue_config_mapper
        self.pluginFactory = PluginFactory()

    # main loop
    def run(self):
        while True:
            mainLog = self.make_logger(_logger, 'id={0}'.format(self.get_pid()), method_name='run')
            mainLog.debug('getting number of jobs to be fetched')
            # get number of jobs to be fetched
            nJobsPerQueue = self.dbProxy.get_num_jobs_to_fetch(harvester_config.jobfetcher.nQueues,
                                                               harvester_config.jobfetcher.lookupTime)
            mainLog.debug('got {0} queues'.format(len(nJobsPerQueue)))
            # loop over all queues
            for queueName, nJobs in iteritems(nJobsPerQueue):
                # check queue
                if not self.queueConfigMapper.has_queue(queueName):
                    continue
                tmpLog = self.make_logger(_logger, 'queueName={0}'.format(queueName),
                                          method_name='run')
                # get queue
                queueConfig = self.queueConfigMapper.get_queue(queueName)
                # upper limit
                if nJobs > harvester_config.jobfetcher.maxJobs:
                    nJobs = harvester_config.jobfetcher.maxJobs
                # get jobs
                default_prodSourceLabel = queueConfig.get_source_label()
                pdpm = getattr(queueConfig, 'prodSourceLabelRandomWeightsPermille', {})
                choice_list = core_utils.make_choice_list(pdpm=pdpm, default=default_prodSourceLabel)
                prodSourceLabel = random.choice(choice_list)
                tmpLog.debug('getting {0} jobs for prodSourceLabel {1}'.format(nJobs, prodSourceLabel))
                sw = core_utils.get_stopwatch()
                siteName = queueConfig.siteName
                jobs, errStr = self.communicator.get_jobs(siteName, self.nodeName,
                                                          prodSourceLabel,
                                                          self.nodeName, nJobs,
                                                          queueConfig.getJobCriteria)
                tmpLog.info('got {0} jobs with {1} {2}'.format(len(jobs), errStr, sw.get_elapsed_time()))
                # convert to JobSpec
                if len(jobs) > 0:
                    # get extractor plugin
                    if hasattr(queueConfig, 'extractor'):
                        extractorCore = self.pluginFactory.get_plugin(queueConfig.extractor)
                    else:
                        extractorCore = None
                    jobSpecs = []
                    fileStatMap = dict()
                    sw_startconvert = core_utils.get_stopwatch()
                    for job in jobs:
                        timeNow = datetime.datetime.utcnow()
                        jobSpec = JobSpec()
                        jobSpec.convert_job_json(job)
                        jobSpec.computingSite = queueName
                        jobSpec.status = 'starting'
                        jobSpec.subStatus = 'fetched'
                        jobSpec.creationTime = timeNow
                        jobSpec.stateChangeTime = timeNow
                        jobSpec.configID = queueConfig.configID
                        jobSpec.set_one_attribute('schedulerID',
                                                  'harvester-{0}'.format(harvester_config.master.harvester_id))
                        if queueConfig.zipPerMB is not None and jobSpec.zipPerMB is None:
                            jobSpec.zipPerMB = queueConfig.zipPerMB
                        fileGroupDictList = [jobSpec.get_input_file_attributes()]
                        if extractorCore is not None:
                            fileGroupDictList.append(extractorCore.get_aux_inputs(jobSpec))
                        for fileGroupDict in fileGroupDictList:
                            for tmpLFN, fileAttrs in iteritems(fileGroupDict):
                                # check file status
                                if tmpLFN not in fileStatMap:
                                    fileStatMap[tmpLFN] = self.dbProxy.get_file_status(tmpLFN, 'input',
                                                                                       queueConfig.ddmEndpointIn,
                                                                                       'starting')
                                # make file spec
                                fileSpec = FileSpec()
                                fileSpec.PandaID = jobSpec.PandaID
                                fileSpec.taskID = jobSpec.taskID
                                fileSpec.lfn = tmpLFN
                                fileSpec.endpoint = queueConfig.ddmEndpointIn
                                fileSpec.scope = fileAttrs['scope']
                                # set preparing to skip stage-in if the file is (being) taken care of by another job
                                if 'ready' in fileStatMap[tmpLFN] or 'preparing' in fileStatMap[tmpLFN] \
                                        or 'to_prepare' in fileStatMap[tmpLFN]:
                                    fileSpec.status = 'preparing'
                                else:
                                    fileSpec.status = 'to_prepare'
                                if fileSpec.status not in fileStatMap[tmpLFN]:
                                    fileStatMap[tmpLFN][fileSpec.status] = 0
                                fileStatMap[tmpLFN][fileSpec.status] += 1
                                if 'INTERNAL_FileType' in fileAttrs:
                                    fileSpec.fileType = fileAttrs['INTERNAL_FileType']
                                    jobSpec.auxInput = JobSpec.AUX_hasAuxInput
                                else:
                                    fileSpec.fileType = 'input'
                                if 'INTERNAL_URL' in fileAttrs:
                                    fileSpec.url = fileAttrs['INTERNAL_URL']
                                jobSpec.add_in_file(fileSpec)
                        jobSpec.trigger_propagation()
                        jobSpecs.append(jobSpec)
                    # insert to DB
                    tmpLog.debug("Converting of {0} jobs {1}".format(len(jobs),sw_startconvert.get_elapsed_time()))
                    sw_insertdb =core_utils.get_stopwatch()
                    self.dbProxy.insert_jobs(jobSpecs)
                    tmpLog.debug('Insert of {0} jobs {1}'.format(len(jobSpecs), sw_insertdb.get_elapsed_time()))
            mainLog.debug('done')
            # check if being terminated
            if self.terminated(harvester_config.jobfetcher.sleepTime):
                mainLog.debug('terminated')
                return
