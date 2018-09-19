from future.utils import iteritems
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvesterbody.agent_base import AgentBase

# logger
_logger = core_utils.setup_logger('sweeper')


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
        lockedBy = 'sweeper-{0}'.format(self.get_pid())
        while True:
            mainLog = self.make_logger(_logger, 'id={0}'.format(lockedBy), method_name='run')
            mainLog.debug('try to get workers to kill')
            # get workers to kill
            workersToKill = self.dbProxy.get_workers_to_kill(harvester_config.sweeper.maxWorkers,
                                                             harvester_config.sweeper.checkInterval)
            mainLog.debug('got {0} queues to kill workers'.format(len(workersToKill)))
            # loop over all workers
            for queueName, configIdWorkSpecs in iteritems(workersToKill):
                for configID, workSpecs in iteritems(configIdWorkSpecs):
                    # get sweeper
                    if not self.queueConfigMapper.has_queue(queueName, configID):
                        mainLog.error('queue config for {0}/{1} not found'.format(queueName, configID))
                        continue
                    queueConfig = self.queueConfigMapper.get_queue(queueName, configID)
                    sweeperCore = self.pluginFactory.get_plugin(queueConfig.sweeper)
                    for workSpec in workSpecs:
                        tmpLog = self.make_logger(_logger, 'workerID={0}'.format(workSpec.workerID),
                                                  method_name='run')
                        try:
                            tmpLog.debug('start killing')
                            tmpStat, tmpOut = sweeperCore.kill_worker(workSpec)
                            tmpLog.debug('done with status={0} diag={1}'.format(tmpStat, tmpOut))
                        except Exception:
                            core_utils.dump_error_message(tmpLog)
            mainLog.debug('done kill')
            # timeout for missed
            try:
                keepMissed = harvester_config.sweeper.keepMissed
            except Exception:
                keepMissed = 24
            keepPending = 24
            # get workers for cleanup
            statusTimeoutMap = {'finished': harvester_config.sweeper.keepFinished,
                                'failed': harvester_config.sweeper.keepFailed,
                                'cancelled': harvester_config.sweeper.keepCancelled,
                                'missed': keepMissed,
                                'pending': keepPending
                                }
            workersForCleanup = self.dbProxy.get_workers_for_cleanup(harvester_config.sweeper.maxWorkers,
                                                                     statusTimeoutMap)
            mainLog.debug('got {0} queues for workers cleanup'.format(len(workersForCleanup)))
            for queueName, configIdWorkSpecs in iteritems(workersForCleanup):
                for configID, workSpecs in iteritems(configIdWorkSpecs):
                    # get sweeper
                    if not self.queueConfigMapper.has_queue(queueName, configID):
                        mainLog.error('queue config for {0}/{1} not found'.format(queueName, configID))
                        continue
                    queueConfig = self.queueConfigMapper.get_queue(queueName, configID)
                    sweeperCore = self.pluginFactory.get_plugin(queueConfig.sweeper)
                    messenger = self.pluginFactory.get_plugin(queueConfig.messenger)
                    for workSpec in workSpecs:
                        tmpLog = self.make_logger(_logger, 'workerID={0}'.format(workSpec.workerID),
                                                  method_name='run')
                        try:
                            tmpLog.debug('start cleanup')
                            # sweep worker
                            tmpStat, tmpOut = sweeperCore.sweep_worker(workSpec)
                            tmpLog.debug('done sweep_worker with status={0} diag={1}'.format(tmpStat, tmpOut))
                            # clean up of messenger
                            if hasattr(messenger, 'clean_up'):
                                mc_tmpStat, mc_tmpOut = messenger.clean_up(workSpec)
                            else:
                                mc_tmpStat, mc_tmpOut = None, 'skipped'
                            tmpLog.debug('done clean_up with status={0} diag={1}'.format(mc_tmpStat, mc_tmpOut))
                            if tmpStat:
                                # delete from DB
                                self.dbProxy.delete_worker(workSpec.workerID)
                        except Exception:
                            core_utils.dump_error_message(tmpLog)
            # delete old jobs
            mainLog.debug('delete old jobs')
            jobTimeout = max(statusTimeoutMap.values()) + 1
            self.dbProxy.delete_old_jobs(jobTimeout)
            mainLog.debug('done cleanup')
            # check if being terminated
            if self.terminated(harvester_config.sweeper.sleepTime):
                mainLog.debug('terminated')
                return
