import datetime

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvesterbody.agent_base import AgentBase

# logger
_logger = core_utils.setup_logger()


# class to prepare jobs
class Preparator(AgentBase):
    # constructor
    def __init__(self, communicator, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.communicator = communicator
        self.queueConfigMapper = queue_config_mapper
        self.pluginFactory = PluginFactory()


    # main loop
    def run(self):
        lockedBy = 'preparator-{0}'.format(self.ident)
        while True:
            mainLog = core_utils.make_logger(_logger, 'id={0}'.format(lockedBy))
            mainLog.debug('try to get jobs to check')
            # get jobs to check preparation
            jobsToCheck = self.dbProxy.get_jobs_in_sub_status('preparing',
                                                              harvester_config.preparator.maxJobsToCheck,
                                                              'preparatorTime', 'lockedBy',
                                                              harvester_config.preparator.checkInterval,
                                                              harvester_config.preparator.lockInterval,
                                                              lockedBy)
            mainLog.debug('got {0} jobs to check'.format(len(jobsToCheck)))
            # loop over all jobs
            for jobSpec in jobsToCheck:
                tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(jobSpec.PandaID))
                tmpLog.debug('start checking')
                # get queue
                if not self.queueConfigMapper.has_queue(jobSpec.computingSite):
                    tmpLog.error('queue config for {0} not found'.format(jobSpec.computingSite))
                    continue
                queueConfig = self.queueConfigMapper.get_queue(jobSpec.computingSite)
                oldSubStatus = jobSpec.subStatus
                # get plugin
                preparatorCore = self.pluginFactory.get_plugin(queueConfig.preparator)
                if preparatorCore is None:
                    # not found
                    tmpLog.error('plugin for {0} not found'.format(jobSpec.computingSite))
                    continue
                tmpStat, tmpStr = preparatorCore.check_status(jobSpec)
                # still running
                if tmpStat is None:
                    # update job
                    jobSpec.lockedBy = None
                    self.dbProxy.update_job(jobSpec, {'lockedBy': lockedBy,
                                                      'subStatus': oldSubStatus})
                    tmpLog.debug('still running')
                    continue
                # succeeded
                if tmpStat is True:
                    # resolve path
                    tmpStat, tmpStr = preparatorCore.resolve_input_paths(jobSpec)
                    if tmpStat is False:
                        jobSpec.lockedBy = None
                        self.dbProxy.update_job(jobSpec, {'lockedBy': lockedBy,
                                                          'subStatus': oldSubStatus})
                        tmpLog.error('failed to resolve input file paths : {0}'.format(tmpStr))
                        continue
                    # update job
                    jobSpec.subStatus = 'prepared'
                    jobSpec.lockedBy = None
                    jobSpec.preparatorTime = None
                    self.dbProxy.update_job(jobSpec, {'lockedBy': lockedBy,
                                                      'subStatus': oldSubStatus})
                    tmpLog.debug('succeeded')
                else:
                    # update job
                    jobSpec.status = 'failed'
                    jobSpec.subStatus = 'failedtoprepare'
                    jobSpec.lockedBy = None
                    jobSpec.preparatorTime = None
                    jobSpec.stateChangeTime = datetime.datetime.utcnow()
                    jobSpec.trigger_propagation()
                    self.dbProxy.update_job(jobSpec, {'lockedBy': lockedBy,
                                                      'subStatus': oldSubStatus})
                    tmpLog.error('failed with {0}'.format(tmpStr))
            # get jobs to trigger preparation
            mainLog.debug('try to get jobs to prepare')
            jobsToTrigger = self.dbProxy.get_jobs_in_sub_status('fetched',
                                                                harvester_config.preparator.maxJobsToTrigger,
                                                                'preparatorTime', 'lockedBy',
                                                                harvester_config.preparator.triggerInterval,
                                                                harvester_config.preparator.lockInterval,
                                                                lockedBy,
                                                                'preparing')
            mainLog.debug('got {0} jobs to prepare'.format(len(jobsToTrigger)))
            # loop over all jobs
            for jobSpec in jobsToTrigger:
                tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(jobSpec.PandaID))
                tmpLog.debug('try to trigger preparation')
                # get queue
                if not self.queueConfigMapper.has_queue(jobSpec.computingSite):
                    tmpLog.error('queue config for {0} not found'.format(jobSpec.computingSite))
                    continue
                queueConfig = self.queueConfigMapper.get_queue(jobSpec.computingSite)
                oldSubStatus = jobSpec.subStatus
                # get plugin
                preparatorCore = self.pluginFactory.get_plugin(queueConfig.preparator)
                if preparatorCore is None:
                    # not found
                    tmpLog.error('plugin for {0} not found'.format(jobSpec.computingSite))
                    continue
                # trigger preparation
                tmpStat, tmpStr = preparatorCore.trigger_preparation(jobSpec)
                # succeeded
                if tmpStat is True:
                    # update job
                    jobSpec.subStatus = 'preparing'
                    jobSpec.lockedBy = None
                    jobSpec.preparatorTime = None
                    self.dbProxy.update_job(jobSpec, {'lockedBy': lockedBy,
                                                      'subStatus': oldSubStatus})
                    tmpLog.debug('triggered')
                else:
                    # update job
                    jobSpec.status = 'failed'
                    jobSpec.subStatus = 'failedtoprepare'
                    jobSpec.lockedBy = None
                    jobSpec.preparatorTime = None
                    jobSpec.stateChangeTime = datetime.datetime.utcnow()
                    jobSpec.trigger_propagation()
                    self.dbProxy.update_job(jobSpec, {'lockedBy': lockedBy,
                                                      'subStatus': oldSubStatus})
                    tmpLog.debug('failed to trigger with {0}'.format(tmpStr))
            mainLog.debug('done')
            # check if being terminated
            if self.terminated(harvester_config.preparator.sleepTime):
                mainLog.debug('terminated')
                return
