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
                    continue
                # get queue
                queueConfig = self.queueConfigMapper.get_queue(queueName)
                nQueue = tmpVal['nQueue']
                nReady = tmpVal['nReady']
                nRunning = tmpVal['nRunning']
                nQueueLimit = queueConfig.nQueueLimitWorker
                maxWorkers = queueConfig.maxWorkers
                qrWorkerRatio = queueConfig.qrWorkerRatio
                if queueConfig.runMode == 'slave':
                    nNewWorkersDef = tmpVal['nNewWorkers']
                else:
                    nNewWorkersDef = None
                # define num of new workers based on static site config
                nNewWorkers = 0
                if nQueueLimit > 0 and nQueue >= nQueueLimit:
                    # enough queued workers
                    pass
                elif maxWorkers > 0 and (nQueue + nReady + nRunning) >= maxWorkers:
                    # enough workers in the system
                    pass
                elif qrWorkerRatio > 0 and nRunning > 0 and nQueue > qrWorkerRatio * nRunning / 100:
                    # enough workers in terms of Running-to-Queued ratio
                    pass
                else:
                    # get max number of queued workers
                    maxQueuedWorkers = 0
                    if nQueueLimit > 0:
                        maxQueuedWorkers = nQueueLimit
                    if qrWorkerRatio > 0 and nRunning > 0:
                        maxQueuedWorkers = max(maxQueuedWorkers, qrWorkerRatio * nRunning / 100)
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
            # correction for global shares
            # TO BE IMPLEMENTED
            # correction based on commands from PanDA
            # TO BE IMPLEMENTED
            # correction based on resource information
            # TO BE IMPLEMENTED : need plugin?
            # dump
            tmpLog.debug('defined {0}'.format(str(dyn_num_workers)))
            return dyn_num_workers
        except:
            # dump error
            core_utils.dump_error_message(tmpLog)
            return None
