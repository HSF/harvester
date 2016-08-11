import socket
import datetime
import threading

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.JobSpec import JobSpec
from pandaharvester.harvestercore.DBProxy import DBProxy
from pandaharvester.harvestercore.PluginFactory import PluginFactory

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('Preparator')


# class to prepare jobs
class Preparator (threading.Thread):
    
    # constructor
    def __init__(self,communicator,queueConfigMapper,singleMode=False):
        threading.Thread.__init__(self)
        self.dbProxy = DBProxy()
        self.communicator = communicator
        self.queueConfigMapper = queueConfigMapper
        self.singleMode = singleMode
        self.pluginFactory = PluginFactory()


    # main loop
    def run (self):
        lockedBy = 'preparator-{0}'.format(self.ident)
        while True:
            mainLog = CoreUtils.makeLogger(_logger,'id={0}'.format(lockedBy))
            mainLog.debug('getting number of jobs to be fetched')
            # get jobs to check preparation
            jobsToCheck = self.dbProxy.getJobsInSubStatus('preparing',
                                                          harvester_config.preparator.maxJobsToCheck,
                                                          'preparatorTime','lockedBy',
                                                          harvester_config.preparator.checkInterval,
                                                          harvester_config.preparator.lockInterval,
                                                          lockedBy)
            mainLog.debug('got {0} jobs to check'.format(len(jobsToCheck)))
            # loop over all jobs
            for jobSpec in jobsToCheck:
                tmpLog = CoreUtils.makeLogger(_logger,'PandaID={0}'.format(jobSpec.PandaID))
                tmpLog.debug('start checking')
                # get queue
                if not self.queueConfigMapper.hasQueue(jobSpec.computingSite):
                    tmpLog.error('queue config for {0} not found'.format(jobSpec.computingSite))
                    continue
                queueConifg = self.queueConfigMapper.getQueue(jobSpec.computingSite)
                # get plugin
                preparatorCore = self.pluginFactory.getPlugin(queueConifg.preparator)
                if preparatorCore == None:
                    # not found
                    tmpLog.error('plugin for {0} not found'.format(jobSpec.computingSite))
                    continue
                tmpStat,tmpStr = preparatorCore.checkStatus(jobSpec)
                # still running
                if tmpStat == None:
                    # update job
                    jobSpec.lockedBy = None
                    self.dbProxy.updateJob(tmpJobSpec,{'lockedBy':lockedBy,
                                                       'subStatus':'preparing'})
                    tmpLog.debug('still running')
                    continue
                # successed
                if tmpStat == True:
                    # update job
                    jobSpec.subStatus = 'prepared'
                    jobSpec.lockedBy = None
                    jobSpec.preparatorTime = None
                    self.dbProxy.updateJob(jobSpec,{'lockedBy':lockedBy,
                                                    'subStatus':'preparing'})
                    tmpLog.debug('successed')
                else:
                    # update job
                    jobSpec.status = 'failed'
                    jobSpec.subStatus = 'failedtoprepare'
                    jobSpec.lockedBy = None
                    jobSpec.preparatorTime = None
                    jobSpec.stateChangeTime = datetime.datetime.utcnow()
                    jobSpec.triggerPropagation()
                    self.dbProxy.updateJob(jobSpec,{'lockedBy':lockedBy,
                                                    'subStatus':'preparing'})
                    tmpLog.debug('failed with {0}'.format(tmpStr))
            # get jobs to trigger preparation
            jobsToTrigger = self.dbProxy.getJobsInSubStatus('fetched',
                                                            harvester_config.preparator.maxJobsToTrigger,
                                                            'preparatorTime','lockedBy',
                                                            harvester_config.preparator.triggerInterval,
                                                            harvester_config.preparator.lockInterval,
                                                            lockedBy,
                                                            'preparing')
            mainLog.debug('got {0} jobs to trigger'.format(len(jobsToTrigger)))
            # loop over all jobs
            for jobSpec in jobsToTrigger:
                tmpLog = CoreUtils.makeLogger(_logger,'PandaID={0}'.format(jobSpec.PandaID))
                tmpLog.debug('trigger preparation')
                # get queue
                if not self.queueConfigMapper.hasQueue(jobSpec.computingSite):
                    tmpLog.error('queue config for {0} not found'.format(jobSpec.computingSite))
                    continue
                queueConifg = self.queueConfigMapper.getQueue(jobSpec.computingSite)
                # get plugin
                preparatorCore = self.pluginFactory.getPlugin(queueConifg.preparator)
                if preparatorCore == None:
                    # not found
                    tmpLog.error('plugin for {0} not found'.format(jobSpec.computingSite))
                    continue
                # trigger preparation
                tmpStat,tmpStr = preparatorCore.triggerPreparation(jobSpec)
                # successed
                if tmpStat == True:
                    # update job
                    jobSpec.subStatus = 'preparing'
                    jobSpec.lockedBy = None
                    jobSpec.preparatorTime = None
                    self.dbProxy.updateJob(jobSpec,{'lockedBy':lockedBy,
                                                    'subStatus':'fetched'})
                    tmpLog.debug('successfully triggered')
                else:
                    # update job
                    jobSpec.status = 'failed'
                    jobSpec.subStatus = 'failedtoprepare'
                    jobSpec.lockedBy = None
                    jobSpec.preparatorTime = None
                    jobSpec.stateChangeTime = datetime.datetime.utcnow()
                    jobSpec.triggerPropagation()
                    self.dbProxy.updateJob(jobSpec,{'lockedBy':lockedBy,
                                                    'subStatus':'fetched'})
                    tmpLog.debug('failed to trigger with {0}'.format(tmpStr))
            mainLog.debug('done')
            if self.singleMode:
                return
            # sleep
            CoreUtils.sleep(harvester_config.preparator.sleepTime)

