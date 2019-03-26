import copy
from future.utils import iteritems

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestermisc.apfmon import Apfmon

# logger
_logger = core_utils.setup_logger('worker_adjuster')


# class to define number of workers to submit
class WorkerAdjuster(object):
    # constructor
    def __init__(self, queue_config_mapper):
        self.queueConfigMapper = queue_config_mapper
        self.pluginFactory = PluginFactory()
        self.dbProxy = DBProxy()
        self.throttlerMap = dict()
        self.apf_mon = Apfmon(self.queueConfigMapper)
        try:
            self.maxNewWorkers = harvester_config.submitter.maxNewWorkers
        except AttributeError:
            self.maxNewWorkers = None

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

            # get job statistics
            job_stats = self.dbProxy.get_cache("job_statistics.json", None)
            if job_stats is None:
                job_stats = dict()
            else:
                job_stats = job_stats.data

            # define num of new workers
            for queueName in static_num_workers:
                # get queue
                queueConfig = self.queueConfigMapper.get_queue(queueName)
                workerLimits_dict = self.dbProxy.get_worker_limits(queueName)
                maxWorkers = workerLimits_dict.get('maxWorkers', 0)
                nQueueLimit = workerLimits_dict.get('nQueueLimitWorker', 0)
                nQueueLimitPerRT = workerLimits_dict['nQueueLimitWorkerPerRT']
                nQueue_total, nReady_total, nRunning_total = 0, 0, 0
                apf_msg = None
                apf_data = None
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
                    if resource_type != 'ANY':
                        nQueue_total += nQueue
                        nReady_total += nReady
                        nRunning_total += nRunning
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
                    if nQueue >= nQueueLimitPerRT > 0:
                        # enough queued workers
                        retMsg = 'No nNewWorkers since nQueue({0})>=nQueueLimitPerRT({1})'.format(nQueue, nQueueLimitPerRT)
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

                        if nQueueLimitPerRT > 0:  # there is a limit set for the queue
                            maxQueuedWorkers = nQueueLimitPerRT

                        # Reset the maxQueueWorkers according to particular
                        if nNewWorkersDef is not None:  # don't surpass limits given centrally
                            maxQueuedWorkers_slave = nNewWorkersDef + nQueue
                            if maxQueuedWorkers is not None:
                                maxQueuedWorkers = min(maxQueuedWorkers_slave, maxQueuedWorkers)
                            else:
                                maxQueuedWorkers = maxQueuedWorkers_slave

                        elif queueConfig.mapType == 'NoJob': # for pull mode, limit to activated jobs
                            # limit the queue to the number of activated jobs to avoid empty pilots
                            try:
                                n_activated = max(job_stats[queueName]['activated'], 1) # avoid no activity queues
                                queue_limit = maxQueuedWorkers
                                maxQueuedWorkers = min(n_activated, maxQueuedWorkers)
                                tmpLog.debug('limiting maxQueuedWorkers to min(n_activated={0}, queue_limit={1})'.
                                             format(n_activated, queue_limit))
                            except KeyError:
                                tmpLog.warning('n_activated not defined, defaulting to configured queue limits')
                                pass

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
                    if self.maxNewWorkers is not None and self.maxNewWorkers > 0:
                        nNewWorkers = min(nNewWorkers, self.maxNewWorkers)
                        tmpLog.debug('setting nNewWorkers to {0} in order to respect universal maxNewWorkers'
                                     .format(nNewWorkers))
                    dyn_num_workers[queueName][resource_type]['nNewWorkers'] = nNewWorkers

                # adjust nNewWorkers for UCORE to let aggregations over RT respect nQueueLimitWorker and maxWorkers
                if queueConfig is None:
                    maxNewWorkersPerCycle = 0
                    retMsg = 'set maxNewWorkersPerCycle=0 in UCORE aggregation due to missing queueConfig'
                    tmpLog.debug(retMsg)
                else:
                    maxNewWorkersPerCycle = queueConfig.maxNewWorkersPerCycle
                if len(dyn_num_workers[queueName]) > 1:
                    total_new_workers_rts = sum( dyn_num_workers[queueName][_rt]['nNewWorkers']
                                                if _rt != 'ANY' else 0
                                                for _rt in dyn_num_workers[queueName] )
                    nNewWorkers_max_agg = min(
                                                max(nQueueLimit - nQueue_total, 0),
                                                max(maxWorkers - nQueue_total - nReady_total - nRunning_total, 0),
                                                )
                    if maxNewWorkersPerCycle >= 0:
                        nNewWorkers_max_agg = min(nNewWorkers_max_agg, maxNewWorkersPerCycle)
                    if self.maxNewWorkers is not None and self.maxNewWorkers > 0:
                        nNewWorkers_max_agg = min(nNewWorkers_max_agg, self.maxNewWorkers)
                    # exceeded max, to adjust
                    if total_new_workers_rts > nNewWorkers_max_agg:
                        if nNewWorkers_max_agg == 0:
                            for resource_type in dyn_num_workers[queueName]:
                                dyn_num_workers[queueName][resource_type]['nNewWorkers'] = 0
                            tmpLog.debug('No nNewWorkers since nNewWorkers_max_agg=0 for UCORE')
                        else:
                            tmpLog.debug('nNewWorkers_max_agg={0} for UCORE'.format(nNewWorkers_max_agg))
                            _d = dyn_num_workers[queueName].copy()
                            del _d['ANY']
                            simple_rt_nw_list = [ [_rt, _d[_rt].get('nNewWorkers', 0), 0] for _rt in _d ]
                            _countdown = nNewWorkers_max_agg
                            for _rt_list in simple_rt_nw_list:
                                resource_type, nNewWorkers_orig, _r = _rt_list
                                nNewWorkers, remainder = divmod(nNewWorkers_orig*nNewWorkers_max_agg, total_new_workers_rts)
                                dyn_num_workers[queueName][resource_type]['nNewWorkers'] = nNewWorkers
                                _rt_list[2] = remainder
                                _countdown -= nNewWorkers
                            _s_list = sorted(simple_rt_nw_list, key=(lambda x: x[1]))
                            sorted_rt_nw_list = sorted(_s_list, key=(lambda x: x[2]), reverse=True)
                            for resource_type, nNewWorkers_orig, remainder in sorted_rt_nw_list:
                                if _countdown <= 0:
                                    break
                                dyn_num_workers[queueName][resource_type]['nNewWorkers'] += 1
                                _countdown -= 1
                        for resource_type in dyn_num_workers[queueName]:
                            if resource_type == 'ANY':
                                continue
                            nNewWorkers = dyn_num_workers[queueName][resource_type]['nNewWorkers']
                            tmpLog.debug('setting nNewWorkers to {0} of type {1} in order to respect RT aggregations for UCORE'
                                         .format(nNewWorkers, resource_type))

                if not apf_msg:
                    apf_data = copy.deepcopy(dyn_num_workers[queueName])

                self.apf_mon.update_label(queueName, apf_msg, apf_data)

            # dump
            tmpLog.debug('defined {0}'.format(str(dyn_num_workers)))
            return dyn_num_workers
        except Exception:
            # dump error
            errMsg = core_utils.dump_error_message(tmpLog)
            return None
