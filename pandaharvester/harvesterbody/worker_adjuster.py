import copy
from future.utils import iteritems

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestermisc.apfmon import Apfmon

# logger
_logger = core_utils.setup_logger('worker_adjuster')


# class to define number of workers to submit
class WorkerAdjuster:
    # constructor
    def __init__(self, queue_config_mapper):
        self.queueConfigMapper = queue_config_mapper
        self.pluginFactory = PluginFactory()
        self.dbProxy = DBProxy()
        self.throttlerMap = dict()
        self.apf_mon = Apfmon(self.queueConfigMapper)

    # define number of workers to submit based on various information
    def define_num_workers(self, static_num_workers, site_name):
        tmpLog = core_utils.make_logger(_logger, 'site={0}'.format(site_name), method_name='define_num_workers')
        tmpLog.debug('start')
        tmpLog.debug('static_num_workers: {0}'.format(static_num_workers))
        dyn_num_workers = copy.deepcopy(static_num_workers)
        try:
            # get queue status
            queueStat = self.dbProxy.get_cache("panda_queues.json", None)
            if queueStat is None:
                queueStat = dict()
            else:
                queueStat = queueStat.data

            # define num of new workers
            for queueName in static_num_workers:
                apf_msg = None
                for resource_type, tmpVal in iteritems(static_num_workers[queueName]):
                    tmpLog.debug('Processing queue {0} resource {1} with static_num_workers {2}'.
                                 format(queueName, resource_type, tmpVal))

                    # set 0 to num of new workers when the queue is disabled
                    if queueName in queueStat and queueStat[queueName]['status'] in ['offline', 'standby',
                                                                                     'maintenance']:
                        dyn_num_workers[queueName][resource_type]['nNewWorkers'] = 0
                        retMsg = 'set nNewWorkers=0 since status={0}'.format(queueStat[queueName]['status'])
                        tmpLog.debug(retMsg)
                        apf_msg = 'Not submitting workers since queue status = {0}'.format(queueStat[queueName]['status'])
                        continue

                    # get queue
                    queueConfig = self.queueConfigMapper.get_queue(queueName)

                    # protection against not-up-to-date queue config
                    if queueConfig is None:
                        dyn_num_workers[queueName][resource_type]['nNewWorkers'] = 0
                        retMsg = 'set nNewWorkers=0 due to missing queueConfig'
                        tmpLog.debug(retMsg)
                        apf_msg = 'Not submitting workers because of missing queueConfig'
                        continue

                    # get throttler
                    if queueName not in self.throttlerMap:
                        if hasattr(queueConfig, 'throttler'):
                            throttler = self.pluginFactory.get_plugin(queueConfig.throttler)
                        else:
                            throttler = None
                        self.throttlerMap[queueName] = throttler

                    # check throttler
                    throttler = self.throttlerMap[queueName]
                    if throttler is not None:
                        toThrottle, tmpMsg = throttler.to_be_throttled(queueConfig)
                        if toThrottle:
                            dyn_num_workers[queueName][resource_type]['nNewWorkers'] = 0
                            retMsg = 'set nNewWorkers=0 by {0}:{1}'.format(throttler.__class__.__name__, tmpMsg)
                            tmpLog.debug(retMsg)
                            continue

                    # check stats
                    nQueue = tmpVal['nQueue']
                    nReady = tmpVal['nReady']
                    nRunning = tmpVal['nRunning']
                    nQueueLimit = queueConfig.nQueueLimitWorker
                    maxWorkers = queueConfig.maxWorkers
                    if queueConfig.runMode == 'slave':
                        nNewWorkersDef = tmpVal['nNewWorkers']
                        if nNewWorkersDef == 0:
                            dyn_num_workers[queueName][resource_type]['nNewWorkers'] = 0
                            retMsg = 'set nNewWorkers=0 by panda in slave mode'
                            tmpLog.debug(retMsg)
                            continue
                    else:
                        nNewWorkersDef = None

                    # define num of new workers based on static site config
                    nNewWorkers = 0
                    if nQueue >= nQueueLimit > 0:
                        # enough queued workers
                        retMsg = 'No nNewWorkers since nQueue({0})>=nQueueLimit({1})'.format(nQueue, nQueueLimit)
                        tmpLog.debug(retMsg)
                        pass
                    elif (nQueue + nReady + nRunning) >= maxWorkers > 0:
                        # enough workers in the system
                        retMsg = 'No nNewWorkers since nQueue({0}) + nReady({1}) + nRunning({2}) '.format(nQueue,
                                                                                                          nReady,
                                                                                                          nRunning)
                        retMsg += '>= maxWorkers({0})'.format(maxWorkers)
                        tmpLog.debug(retMsg)
                        pass
                    else:

                        maxQueuedWorkers = None

                        if nQueueLimit > 0:  # there is a limit set for the queue
                            maxQueuedWorkers = nQueueLimit

                        if nNewWorkersDef is not None:  # don't surpass limits given centrally
                            maxQueuedWorkers_slave = nNewWorkersDef + nQueue
                            if maxQueuedWorkers is not None:
                                maxQueuedWorkers = min(maxQueuedWorkers_slave, maxQueuedWorkers)
                            else:
                                maxQueuedWorkers = maxQueuedWorkers_slave

                        if maxQueuedWorkers is None:  # no value found, use default value
                            maxQueuedWorkers = 1

                        # new workers
                        nNewWorkers = max(maxQueuedWorkers - nQueue, 0)
                        tmpLog.debug('setting nNewWorkers to {0} in maxQueuedWorkers calculation'
                                     .format(nNewWorkers))
                        if maxWorkers > 0:
                            nNewWorkers = min(nNewWorkers, max(maxWorkers - nQueue - nReady - nRunning, 0))
                            tmpLog.debug('setting nNewWorkers to {0} to respect maxWorkers'
                                         .format(nNewWorkers))
                    if queueConfig.maxNewWorkersPerCycle > 0:
                        nNewWorkers = min(nNewWorkers, queueConfig.maxNewWorkersPerCycle)
                        tmpLog.debug('setting nNewWorkers to {0} in order to respect maxNewWorkersPerCycle'
                                     .format(nNewWorkers))
                    dyn_num_workers[queueName][resource_type]['nNewWorkers'] = nNewWorkers

                if not apf_msg:
                    apf_msg = 'Attempting to submit new workers (across all CEs in the queue): {0}'.format(dyn_num_workers[queueName])

                self.apf_mon.update_label(queueName, apf_msg)

            # dump
            tmpLog.debug('defined {0}'.format(str(dyn_num_workers)))
            return dyn_num_workers
        except Exception:
            # dump error
            errMsg = core_utils.dump_error_message(tmpLog)
            return None
