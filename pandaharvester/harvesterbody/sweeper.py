import datetime

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvesterbody.agent_base import AgentBase

# logger
_logger = core_utils.setup_logger()


# class for cleanup
class Sweeper(AgentBase):
    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.queueConfigMapper = queue_config_mapper
        self.pluginFactory = PluginFactory()


    # main loop
    def run(self):
        lockedBy = 'sweeper-{0}'.format(self.ident)
        while True:
            mainLog = core_utils.make_logger(_logger, 'id={0}'.format(lockedBy))
            mainLog.debug('try to get workers to kill')
            # get workers to kill
            workersToKill = self.dbProxy.get_workers_to_kill(harvester_config.sweeper.maxWorkers,
                                                             harvester_config.sweeper.checkInterval)
            mainLog.debug('got {0} queues to kill workers'.format(len(workersToKill)))
            # loop over all workers
            for queueName, workSpecs in workersToKill.iteritems():
                # get sweeper
                if not self.queueConfigMapper.has_queue(queueName):
                    mainLog.error('queue config for {0} not found'.format(queueName))
                    continue
                queueConfig = self.queueConfigMapper.get_queue(queueName)
                sweeperCore = self.pluginFactory.get_plugin(queueConfig.sweeper)
                for workSpec in workSpecs:
                    tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workSpec.workerID))
                    tmpLog.debug('start killing')
                    tmpStat, tmpOut = sweeperCore.kill_worker(workSpec)
                    tmpLog.debug('done with status={0} diag={1}'.format(tmpStat, tmpOut))
            mainLog.debug('done kill')
            # get workers for cleanup
            statusTimeoutMap = {'finished': harvester_config.sweeper.keepFinished,
                                'failed': harvester_config.sweeper.keepFailed,
                                'cancelled': harvester_config.sweeper.keepCancelled
                                }
            workersForCleanup = self.dbProxy.get_workers_for_cleanup(harvester_config.sweeper.maxWorkers,
                                                                     statusTimeoutMap)
            mainLog.debug('got {0} queues for workers cleanup'.format(len(workersForCleanup)))
            for queueName, workSpecs in workersForCleanup.iteritems():
                # get sweeper
                if not self.queueConfigMapper.has_queue(queueName):
                    mainLog.error('queue config for {0} not found'.format(queueName))
                    continue
                queueConfig = self.queueConfigMapper.get_queue(queueName)
                sweeperCore = self.pluginFactory.get_plugin(queueConfig.sweeper)
                for workSpec in workSpecs:
                    tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workSpec.workerID))
                    tmpLog.debug('start cleanup')
                    tmpStat, tmpOut = sweeperCore.sweep_worker(workSpec)
                    tmpLog.debug('done with status={0} diag={1}'.format(tmpStat, tmpOut))
                    if tmpStat:
                        # delete from DB
                        self.dbProxy.delete_worker(workSpec.workerID)
            mainLog.debug('done cleanup')
            # check if being terminated
            if self.terminated(harvester_config.sweeper.sleepTime):
                mainLog.debug('terminated')
                return
