from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvesterbody.agent_base import AgentBase

# logger
_logger = core_utils.setup_logger()


# class for stage-out
class Stager(AgentBase):
    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.queueConfigMapper = queue_config_mapper
        self.pluginFactory = PluginFactory()


    # main loop
    def run(self):
        lockedBy = 'stager-{0}'.format(self.ident)
        while True:
            mainLog = core_utils.make_logger(_logger, 'id={0}'.format(lockedBy))
            mainLog.debug('try to get jobs to check')
            # get jobs to check preparation
            jobsToCheck = self.dbProxy.get_jobs_for_stage_out(harvester_config.stager.maxJobsToCheck,
                                                              harvester_config.stager.checkInterval,
                                                              harvester_config.stager.lockInterval,
                                                              lockedBy, 'transferring',
                                                              JobSpec.HO_hasTransfer)
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
                # get plugin
                stagerCore = self.pluginFactory.get_plugin(queueConfig.stager)
                if stagerCore is None:
                    # not found
                    tmpLog.error('plugin for {0} not found'.format(jobSpec.computingSite))
                    continue
                tmpStat, tmpStr = stagerCore.check_status(jobSpec)
                # succeeded
                if tmpStat is True:
                    # update job
                    newSubStatus = self.dbProxy.update_job_for_stage_out(jobSpec, True)
                    tmpLog.debug('succeeded new subStatus={0}'.format(newSubStatus))
                else:
                    # failed
                    tmpLog.debug('failed with {0}'.format(tmpStr))
            # get jobs to trigger stage-out
            jobsToTrigger = self.dbProxy.get_jobs_for_stage_out(harvester_config.stager.maxJobsToTrigger,
                                                                harvester_config.stager.triggerInterval,
                                                                harvester_config.stager.lockInterval,
                                                                lockedBy, 'to_transfer',
                                                                JobSpec.HO_hasOutput,
                                                                JobSpec.HO_hasZipOutput)
            mainLog.debug('got {0} jobs to trigger'.format(len(jobsToTrigger)))
            # loop over all jobs
            for jobSpec in jobsToTrigger:
                tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(jobSpec.PandaID))
                tmpLog.debug('try to trigger stage-out')
                # get queue
                if not self.queueConfigMapper.has_queue(jobSpec.computingSite):
                    tmpLog.error('queue config for {0} not found'.format(jobSpec.computingSite))
                    continue
                queueConfig = self.queueConfigMapper.get_queue(jobSpec.computingSite)
                # get plugin
                stagerCore = self.pluginFactory.get_plugin(queueConfig.stager)
                if stagerCore is None:
                    # not found
                    tmpLog.error('plugin for {0} not found'.format(jobSpec.computingSite))
                    continue
                # trigger stage-out
                tmpStat, tmpStr = stagerCore.trigger_stage_out(jobSpec)
                # succeeded
                if tmpStat is True:
                    # update job
                    jobSpec.all_files_triggered_to_stage_out()
                    newSubStatus = self.dbProxy.update_job_for_stage_out(jobSpec, False)
                    tmpLog.debug('triggered new subStatus={0}'.format(newSubStatus))
                else:
                    # failed
                    tmpLog.debug('failed to trigger with {0}'.format(tmpStr))
            # get jobs to zip output
            jobsToZip = self.dbProxy.get_jobs_for_stage_out(harvester_config.stager.maxJobsToZip,
                                                            harvester_config.stager.triggerInterval,
                                                            harvester_config.stager.lockInterval,
                                                            lockedBy, 'to_transfer',
                                                            JobSpec.HO_hasZipOutput,
                                                            JobSpec.HO_hasOutput)
            mainLog.debug('got {0} jobs to zip'.format(len(jobsToZip)))
            # loop over all jobs
            for jobSpec in jobsToZip:
                tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(jobSpec.PandaID))
                tmpLog.debug('try to zip output')
                # get queue
                if not self.queueConfigMapper.has_queue(jobSpec.computingSite):
                    tmpLog.error('queue config for {0} not found'.format(jobSpec.computingSite))
                    continue
                queueConfig = self.queueConfigMapper.get_queue(jobSpec.computingSite)
                # get plugin
                stagerCore = self.pluginFactory.get_plugin(queueConfig.stager)
                if stagerCore is None:
                    # not found
                    tmpLog.error('plugin for {0} not found'.format(jobSpec.computingSite))
                    continue
                # trigger preparation
                tmpStat, tmpStr = stagerCore.zip_output(jobSpec)
                # succeeded
                if tmpStat is True:
                    # update job
                    jobSpec.all_files_zipped()
                    newSubStatus = self.dbProxy.update_job_for_stage_out(jobSpec, False)
                    tmpLog.debug('zipped new subStatus={0}'.format(newSubStatus))
                else:
                    # failed
                    tmpLog.debug('failed to zip with {0}'.format(tmpStr))
            mainLog.debug('done')
            # check if being terminated
            if self.terminated(harvester_config.stager.sleepTime):
                mainLog.debug('terminated')
                return
