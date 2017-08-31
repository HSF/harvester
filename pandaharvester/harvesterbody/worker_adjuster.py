import copy

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory

# logger
_logger = core_utils.setup_logger()


# class to define number of workers to submit
class WorkerAdjuster:
    # constructor
    def __init__(self, queue_config_mapper):
        self.queueConfigMapper = queue_config_mapper
        self.pluginFactory = PluginFactory()
        self.dbProxy = DBProxy()
        self.throttlerMap = dict()

    # define number of workers to submit based on various information
    def define_num_workers(self, static_num_workers, site_name):
        tmpLog = core_utils.make_logger(_logger, 'site={0}'.format(site_name))
        tmpLog.debug('start')
        dyn_num_workers = copy.copy(static_num_workers)
        try:
            # get queue status
            queueStat = self.dbProxy.get_cache("panda_queues.json", None)
            if queueStat is None:
                queueStat = dict()
            else:
                queueStat = queueStat.data
            # define num of new workers
            for queueName, tmpVal in static_num_workers.iteritems():
                # set 0 to num of new workers when the queue is disabled
                if queueName in queueStat and queueStat[queueName]['status'] in ['offline']:
                    dyn_num_workers[queueName]['nNewWorkers'] = 0
                    retMsg = 'set nNewWorkers=0 since status={0}'.format(queueStat[queueName]['status'])
                    tmpLog.debug(retMsg)
                    continue
                # get queue
                queueConfig = self.queueConfigMapper.get_queue(queueName)
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
                        dyn_num_workers[queueName]['nNewWorkers'] = 0
                        retMsg = 'set nNewWorkers=0 by {0}:{1}'.format(throttler.__class__.__name__,
                                                                       tmpMsg)
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
                        dyn_num_workers[queueName]['nNewWorkers'] = 0
                        retMsg = 'set nNewWorkers=0 by panda in slave mode'
                        tmpLog.debug(retMsg)
                        continue
                else:
                    nNewWorkersDef = None
                # define num of new workers based on static site config
                nNewWorkers = 0
                if nQueueLimit > 0 and nQueue >= nQueueLimit:
                    # enough queued workers
                    retMsg = 'No nNewWorkers since nQueue({0})>=nQueueLimit({1})'.format(nQueue, nQueueLimit)
                    tmpLog.debug(retMsg)
                    pass
                elif maxWorkers > 0 and (nQueue + nReady + nRunning) >= maxWorkers:
                    # enough workers in the system
                    retMsg = 'No nNewWorkers since nQueue({0}) + nReady({1}) + nRunning({2}) '.format(nQueue,
                                                                                                      nReady,
                                                                                                      nRunning)
                    retMsg += '>= maxWorkers({0})'.format(maxWorkers)
                    tmpLog.debug(retMsg)
                    pass
                else:
                    # get max number of queued workers
                    maxQueuedWorkers = 0
                    if nQueueLimit > 0:
                        maxQueuedWorkers = nQueueLimit
                    if maxQueuedWorkers == 0:
                        if nNewWorkersDef is not None:
                            # slave mode
                            maxQueuedWorkers = nNewWorkersDef + nQueue
                        else:
                            # use default value
                            maxQueuedWorkers = 1
                    # new workers
                    nNewWorkers = max(maxQueuedWorkers - nQueue, 0)
                    if maxWorkers > 0:
                        nNewWorkers = min(nNewWorkers, max(maxWorkers - nQueue - nReady - nRunning, 0))
                if queueConfig.maxNewWorkersPerCycle > 0:
                    nNewWorkers = min(nNewWorkers, queueConfig.maxNewWorkersPerCycle)
                dyn_num_workers[queueName]['nNewWorkers'] = nNewWorkers
            # dump
            tmpLog.debug('defined {0}'.format(str(dyn_num_workers)))
            return dyn_num_workers
        except:
            # dump error
            errMsg = core_utils.dump_error_message(tmpLog)
            return None
