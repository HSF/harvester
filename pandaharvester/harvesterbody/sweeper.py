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
            sw_main = core_utils.get_stopwatch()
            mainLog = self.make_logger(_logger, 'id={0}'.format(lockedBy), method_name='run')
            # killing stage
            sw_kill = core_utils.get_stopwatch()
            mainLog.debug('try to get workers to kill')
            # get workers to kill
            workersToKill = self.dbProxy.get_workers_to_kill(harvester_config.sweeper.maxWorkers,
                                                             harvester_config.sweeper.checkInterval)
            mainLog.debug('got {0} queues to kill workers'.format(len(workersToKill)))
            # loop over all workers
            sw = core_utils.get_stopwatch()
            for queueName, configIdWorkSpecList in iteritems(workersToKill):
                for configID, workspec_list in iteritems(configIdWorkSpecList):
                    # get sweeper
                    if not self.queueConfigMapper.has_queue(queueName, configID):
                        mainLog.error('queue config for {0}/{1} not found'.format(queueName, configID))
                        continue
                    queueConfig = self.queueConfigMapper.get_queue(queueName, configID)
                    sweeperCore = self.pluginFactory.get_plugin(queueConfig.sweeper)
                    sw.reset()
                    n_workers = len(workspec_list)
                    try:
                        # try bulk method
                        tmpLog = self.make_logger(_logger, 'id={0}'.format(lockedBy), method_name='run')
                        tmpLog.debug('start killing')
                        tmpList = sweeperCore.kill_workers(workspec_list)
                    except AttributeError:
                        # fall back to single-worker method
                        for workspec in workspec_list:
                            tmpLog = self.make_logger(_logger, 'workerID={0}'.format(workspec.workerID),
                                                      method_name='run')
                            try:
                                tmpLog.debug('start killing one worker')
                                tmpStat, tmpOut = sweeperCore.kill_worker(workspec)
                                tmpLog.debug('done killing with status={0} diag={1}'.format(tmpStat, tmpOut))
                            except Exception:
                                core_utils.dump_error_message(tmpLog)
                    except Exception:
                        core_utils.dump_error_message(mainLog)
                    else:
                        # bulk method
                        n_killed = 0
                        for workspec, (tmpStat, tmpOut) in zip(workspec_list, tmpList):
                            tmpLog.debug('done killing workerID={0} with status={1} diag={2}'.format(
                                            workspec.workerID, tmpStat, tmpOut))
                            if tmpStat:
                                n_killed += 1
                        tmpLog.debug('killed {0}/{1} workers'.format(n_killed, n_workers))
                    mainLog.debug('done killing {0} workers'.format(n_workers) + sw.get_elapsed_time())
            mainLog.debug('done all killing' + sw_kill.get_elapsed_time())
            # cleanup stage
            sw_cleanup = core_utils.get_stopwatch()
            # timeout for missed
            try:
                keepMissed = harvester_config.sweeper.keepMissed
            except Exception:
                keepMissed = 24
            try:
                keepPending = harvester_config.sweeper.keepPending
            except Exception:
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
            sw = core_utils.get_stopwatch()
            for queueName, configIdWorkSpecList in iteritems(workersForCleanup):
                for configID, workspec_list in iteritems(configIdWorkSpecList):
                    # get sweeper
                    if not self.queueConfigMapper.has_queue(queueName, configID):
                        mainLog.error('queue config for {0}/{1} not found'.format(queueName, configID))
                        continue
                    queueConfig = self.queueConfigMapper.get_queue(queueName, configID)
                    sweeperCore = self.pluginFactory.get_plugin(queueConfig.sweeper)
                    messenger = self.pluginFactory.get_plugin(queueConfig.messenger)
                    sw.reset()
                    n_workers = len(workspec_list)
                    # make sure workers to clean up are all terminated
                    mainLog.debug('making sure workers to clean up are all terminated')
                    try:
                        # try bulk method
                        tmpList = sweeperCore.kill_workers(workspec_list)
                    except AttributeError:
                        # fall back to single-worker method
                        for workspec in workspec_list:
                            tmpLog = self.make_logger(_logger, 'workerID={0}'.format(workspec.workerID),
                                                      method_name='run')
                            try:
                                tmpStat, tmpOut = sweeperCore.kill_worker(workspec)
                            except Exception:
                                core_utils.dump_error_message(tmpLog)
                    except Exception:
                        core_utils.dump_error_message(mainLog)
                    mainLog.debug('made sure workers to clean up are all terminated')
                    # start cleanup
                    for workspec in workspec_list:
                        tmpLog = self.make_logger(_logger, 'workerID={0}'.format(workspec.workerID),
                                                  method_name='run')
                        try:
                            tmpLog.debug('start cleaning up one worker')
                            # sweep worker
                            tmpStat, tmpOut = sweeperCore.sweep_worker(workspec)
                            tmpLog.debug('swept_worker with status={0} diag={1}'.format(tmpStat, tmpOut))
                            tmpLog.debug('start messenger cleanup')
                            mc_tmpStat, mc_tmpOut = messenger.clean_up(workspec)
                            tmpLog.debug('messenger cleaned up with status={0} diag={1}'.format(mc_tmpStat, mc_tmpOut))
                            if tmpStat:
                                self.dbProxy.delete_worker(workspec.workerID)
                        except Exception:
                            core_utils.dump_error_message(tmpLog)
                    mainLog.debug('done cleaning up {0} workers'.format(n_workers) + sw.get_elapsed_time())
            mainLog.debug('done all cleanup' + sw_cleanup.get_elapsed_time())
            # old-job-deletion stage
            sw_delete = core_utils.get_stopwatch()
            mainLog.debug('delete old jobs')
            jobTimeout = max(statusTimeoutMap.values()) + 1
            self.dbProxy.delete_old_jobs(jobTimeout)
            # delete orphaned job info
            self.dbProxy.delete_orphaned_job_info()
            mainLog.debug('done deletion of old jobs' + sw_delete.get_elapsed_time())
            # time the cycle
            mainLog.debug('done a sweeper cycle' + sw_main.get_elapsed_time())
            # check if being terminated
            if self.terminated(harvester_config.sweeper.sleepTime):
                mainLog.debug('terminated')
                return
