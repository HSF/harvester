import copy
import math
import traceback

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.resource_type_mapper import ResourceTypeMapper
from pandaharvester.harvestermisc.apfmon import Apfmon
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict

# logger
_logger = core_utils.setup_logger("worker_adjuster")


# class to define number of workers to submit
class WorkerAdjuster(object):
    # constructor
    def __init__(self, queue_config_mapper):
        tmp_log = core_utils.make_logger(_logger, method_name="__init__")
        self.queue_configMapper = queue_config_mapper
        self.pluginFactory = PluginFactory()
        self.dbProxy = DBProxy()
        self.throttlerMap = dict()
        self.apf_mon = Apfmon(self.queue_configMapper)
        try:
            self.maxNewWorkers = harvester_config.submitter.maxNewWorkers
        except AttributeError:
            self.maxNewWorkers = None
        try:
            if harvester_config.submitter.activateWorkerFactor == "auto":
                self.activate_worker_factor = "auto"
            else:
                self.activate_worker_factor = float(harvester_config.submitter.activateWorkerFactor)
        except AttributeError:
            self.activate_worker_factor = 1.0
        except Exception:
            err_str = traceback.format_exc()
            tmp_log.error(err_str)
            tmp_log.warning("default activate_worker_factor = 1")
            self.activate_worker_factor = 1.0
        try:
            if harvester_config.submitter.noPilotsWhenNoActiveJobs:
                self.no_pilots_when_no_active_jobs = True
        except AttributeError:
            self.no_pilots_when_no_active_jobs = False
        except Exception:
            err_str = traceback.format_exc()
            tmp_log.error(err_str)
            tmp_log.warning("default no_pilots_when_no_active_jobs = False")
            self.no_pilots_when_no_active_jobs = False

    # get queue noPilotsWhenNoActiveJobs
    def get_queue_no_pilots_when_no_active_jobs(self, site_name=None):
        tmp_log = core_utils.make_logger(_logger, f"site={site_name}", method_name="get_queue_no_pilots_when_no_active_jobs")
        ret_val = False

        if self.no_pilots_when_no_active_jobs:
            return True

        try:
            if self.queue_configMapper.has_queue(site_name):
                queue_config = self.queue_configMapper.get_queue(site_name)
                ret_val = bool(queue_config.submitter.get("noPilotsWhenNoActiveJobs", False))
        except Exception:
            pass
        tmp_log.debug(f"ret_val={ret_val}")
        return ret_val

    def get_core_factor(self, queue_config, queue_dict, job_type, resource_type, tmp_logger):
        try:
            is_unified_queue = queue_dict.get("capability", "") == "ucore"
            nCoreFactor = queue_config.submitter.get("nCoreFactor", 1)
            if type(nCoreFactor) in [dict]:
                if job_type in nCoreFactor:
                    t_job_type = job_type
                else:
                    t_job_type = 'Any'
                if is_unified_queue:
                    t_resource_type = resource_type
                else:
                    t_resource_type = 'Undefined'
                n_core_factor = nCoreFactor.get(t_job_type, {}).get(t_resource_type, 1)
                return int(n_core_factor)
            else:
                return int(self.nCoreFactor)
        except Exception as ex:
            tmp_logger.warning(f"Failed to get core factor: {ex}")
        return 1

    # get queue activate worker factor
    def get_queue_activate_worker_factor(self, site_name=None, job_type=None, resource_type=None, queue_dict=None):
        tmp_log = core_utils.make_logger(_logger, f"site={site_name}", method_name="get_queue_activate_worker_factor")
        ret_val = 1.0

        # balance in a queue when MCore is used in a pilot wrapper
        try:
            if self.queue_configMapper.has_queue(site_name):
                queue_config = self.queue_configMapper.get_queue(site_name)

                # tmp_log.debug("queue_config.submitter:%s" % str(queue_config.submitter))
                nCoreFactor = self.get_core_factor(queue_config, queue_dict, job_type, resource_type, tmp_log)

                ret_val = 1.0 / nCoreFactor
        except Exception:
            pass
        tmp_log.debug(f"ret_val={ret_val}")
        return ret_val

    # get activate worker factor
    def get_activate_worker_factor(self, site_name=None, job_type=None, resource_type=None, queue_dict=None):
        tmp_log = core_utils.make_logger(_logger, f"site={site_name}", method_name="get_activate_worker_factor")
        ret_val = 1.0

        # balance between multiple harvesters
        if self.activate_worker_factor == "auto":
            # dynamic factor
            worker_stats_from_panda = self.dbProxy.get_cache("worker_statistics.json", None)
            if not worker_stats_from_panda:
                # got empty, return default
                pass
            else:
                worker_stats_from_panda = worker_stats_from_panda.data
                try:
                    # return 1/n_harvester_instances for the site
                    val_dict = worker_stats_from_panda[site_name]
                    n_harvester_instances = len(list(val_dict.keys()))
                    tmp_log.debug(f"number of harvesters: {n_harvester_instances}")
                    ret_val = 1.0 / max(n_harvester_instances, 1)
                except KeyError:
                    # no data for this site, return default
                    pass
        else:
            # static factor
            ret_val = self.activate_worker_factor

        queue_factor = self.get_queue_activate_worker_factor(site_name=site_name, job_type=job_type, resource_type=resource_type, queue_dict=queue_dict)
        ret_val = ret_val * queue_factor

        tmp_log.debug(f"ret_val={ret_val}")
        return ret_val

    # define number of workers to submit based on various information
    def define_num_workers(self, static_num_workers, site_name):
        tmp_log = core_utils.make_logger(_logger, f"site={site_name}", method_name="define_num_workers")
        tmp_log.debug("start")
        tmp_log.debug(f"static_num_workers: {static_num_workers}")
        dyn_num_workers = copy.deepcopy(static_num_workers)
        try:
            # get queue status
            queue_stat = self.dbProxy.get_cache("panda_queues.json", None)
            if queue_stat is None:
                queue_stat = dict()
            else:
                queue_stat = queue_stat.data

            # get job statistics
            job_stats = self.dbProxy.get_cache("job_statistics.json", None)
            if job_stats is not None:
                job_stats = job_stats.data

            # get panda queues dict from CRIC
            panda_queues_dict = PandaQueuesDict()

            # get resource type mapper
            rt_mapper = ResourceTypeMapper()

            # define num of new workers
            for queue_name in static_num_workers:
                # get queue
                queue_config = self.queue_configMapper.get_queue(queue_name)
                worker_limits_dict, worker_stats_map = self.dbProxy.get_worker_limits(queue_name, queue_config)
                max_workers = worker_limits_dict.get("maxWorkers", 0)
                n_queue_limit = worker_limits_dict.get("nQueueLimitWorker", 0)
                n_queue_limit_per_rt = n_queue_limit
                queue_limit_cores = worker_limits_dict["nQueueWorkerCores"]
                queue_limit_memory = worker_limits_dict["nQueueWorkerMemory"]
                cores_queue = worker_stats_map["queue"]["core"]
                memory_queue = worker_stats_map["queue"]["mem"]
                n_queue_total, n_ready_total, n_running_total = 0, 0, 0
                apf_msg = None
                apf_data = None
                for job_type, jt_values in static_num_workers[queue_name].items():
                    for resource_type, tmp_val in jt_values.items():
                        tmp_log.debug(f"Processing queue {queue_name} job_type {job_type} resource_type {resource_type} with static_num_workers {tmp_val}")

                        # get cores and memory request per worker of this resource_type
                        queue_dict = panda_queues_dict.get(queue_name, {})
                        rtype_request_cores, rtype_request_memory = rt_mapper.calculate_worker_requirements(resource_type, queue_dict)

                        # set 0 to num of new workers when the queue is disabled
                        if queue_name in queue_stat and queue_stat[queue_name]["status"] in ["offline", "standby", "maintenance"]:
                            dyn_num_workers[queue_name][job_type][resource_type]["nNewWorkers"] = 0
                            ret_msg = f"set n_new_workers=0 since status={queue_stat[queue_name]['status']}"
                            tmp_log.debug(ret_msg)
                            apf_msg = f"Not submitting workers since queue status = {queue_stat[queue_name]['status']}"
                            continue

                        # protection against not-up-to-date queue config
                        if queue_config is None:
                            dyn_num_workers[queue_name][job_type][resource_type]["nNewWorkers"] = 0
                            ret_msg = "set n_new_workers=0 due to missing queue_config"
                            tmp_log.debug(ret_msg)
                            apf_msg = "Not submitting workers because of missing queue_config"
                            continue

                        # get throttler
                        if queue_name not in self.throttlerMap:
                            if hasattr(queue_config, "throttler"):
                                throttler = self.pluginFactory.get_plugin(queue_config.throttler)
                            else:
                                throttler = None
                            self.throttlerMap[queue_name] = throttler

                        # check throttler
                        throttler = self.throttlerMap[queue_name]
                        if throttler is not None:
                            to_throttle, tmp_msg = throttler.to_be_throttled(queue_config, queue_config_mapper=self.queue_configMapper)
                            if to_throttle:
                                dyn_num_workers[queue_name][job_type][resource_type]["nNewWorkers"] = 0
                                ret_msg = f"set n_new_workers=0 by {throttler.__class__.__name__}:{tmp_msg}"
                                tmp_log.debug(ret_msg)
                                continue

                        # check stats
                        n_queue = tmp_val["nQueue"]
                        n_ready = tmp_val["nReady"]
                        n_running = tmp_val["nRunning"]
                        if resource_type != "ANY" and job_type != "ANY" and job_type is not None:
                            n_queue_total += n_queue
                            n_ready_total += n_ready
                            n_running_total += n_running
                        if queue_config.runMode == "slave":
                            n_new_workers_def = tmp_val["nNewWorkers"]
                            if n_new_workers_def == 0:
                                dyn_num_workers[queue_name][job_type][resource_type]["nNewWorkers"] = 0
                                ret_msg = "set n_new_workers=0 by panda in slave mode"
                                tmp_log.debug(ret_msg)
                                continue
                        else:
                            n_new_workers_def = None

                        # define num of new workers based on static site config
                        n_new_workers = 0
                        if n_queue >= n_queue_limit_per_rt > 0:
                            # enough queued workers
                            ret_msg = f"No n_new_workers since n_queue({n_queue})>=n_queue_limit_per_rt({n_queue_limit_per_rt})"
                            tmp_log.debug(ret_msg)
                            pass
                        elif (n_queue + n_ready + n_running) >= max_workers > 0:
                            # enough workers in the system
                            ret_msg = (
                                f"No n_new_workers since n_queue({n_queue}) + n_ready({n_ready}) + n_running({n_running}) " f">= max_workers({max_workers})"
                            )
                            tmp_log.debug(ret_msg)
                            pass
                        elif queue_limit_cores is not None and cores_queue >= queue_limit_cores:
                            # enough queuing cores
                            ret_msg = f"No n_new_workers since cores_queue({cores_queue}) >= " f"queue_limit_cores({queue_limit_cores})"
                            tmp_log.debug(ret_msg)
                            pass
                        elif queue_limit_memory is not None and memory_queue >= queue_limit_memory:
                            # enough queuing cores
                            ret_msg = f"No n_new_workers since memory_queue({memory_queue} MB) >= " f"queue_limit_memory({queue_limit_memory} MB)"
                            tmp_log.debug(ret_msg)
                            pass
                        else:
                            max_queued_workers = None

                            if n_queue_limit_per_rt > 0:  # there is a limit set for the queue
                                max_queued_workers = n_queue_limit_per_rt

                            # Reset the maxQueueWorkers according to particular
                            if n_new_workers_def is not None:  # don't surpass limits given centrally
                                maxQueuedWorkers_slave = n_new_workers_def + n_queue
                                if max_queued_workers is not None:
                                    max_queued_workers = min(maxQueuedWorkers_slave, max_queued_workers)
                                else:
                                    max_queued_workers = maxQueuedWorkers_slave

                            elif queue_config.mapType == "NoJob":  # for pull mode, limit to activated jobs
                                if job_stats is None:
                                    tmp_log.warning("n_activated not defined, defaulting to configured queue limits")
                                    pass
                                else:
                                    # limit the queue to the number of activated jobs to avoid empty pilots
                                    try:
                                        n_min_pilots = 1
                                        if self.get_queue_no_pilots_when_no_active_jobs(queue_name):
                                            n_min_pilots = 0

                                        queue_activated = job_stats[queue_name]["activated"]
                                        tmp_log.debug(f"available activated panda jobs {queue_activated}")

                                        activate_worker_factor = self.get_activate_worker_factor(queue_name, job_type, resource_type, queue_dict)
                                        if job_stats[queue_name]["activated"] * activate_worker_factor > 0:
                                            n_min_pilots = 1
                                        n_activated = max(
                                            int(job_stats[queue_name]["activated"] * activate_worker_factor), n_min_pilots
                                        )  # avoid no activity queues
                                    except KeyError:
                                        # zero job in the queue
                                        tmp_log.debug("no job in queue")
                                        if self.get_queue_no_pilots_when_no_active_jobs(queue_name):
                                            n_activated = 0
                                        else:
                                            n_activated = max(1 - n_queue - n_ready - n_running, 0)
                                    finally:
                                        queue_limit = max_queued_workers
                                        max_queued_workers = min(n_activated, max_queued_workers)
                                        tmp_log.debug(f"limiting max_queued_workers to min(n_activated={n_activated}, queue_limit={queue_limit})")

                            if max_queued_workers is None:  # no value found, use default value
                                max_queued_workers = 1

                            # new workers
                            n_new_workers = max(max_queued_workers - n_queue, 0)
                            tmp_log.debug(f"setting n_new_workers to {n_new_workers} in max_queued_workers calculation")
                            if max_workers > 0:
                                n_new_workers = min(n_new_workers, max(max_workers - n_queue - n_ready - n_running, 0))
                                tmp_log.debug(f"setting n_new_workers to {n_new_workers} to respect max_workers")
                            if queue_limit_cores:
                                new_worker_cores_max = max(queue_limit_cores - cores_queue, 0)
                                n_new_workers = min(n_new_workers, math.ceil(new_worker_cores_max / rtype_request_cores))
                                tmp_log.debug(f"setting n_new_workers to {n_new_workers} to respect queue_limit_cores")
                            if queue_limit_memory:
                                new_worker_memory_max = max(queue_limit_memory - memory_queue, 0)
                                n_new_workers = min(n_new_workers, math.ceil(new_worker_memory_max / rtype_request_memory))
                                tmp_log.debug(f"setting n_new_workers to {n_new_workers} to respect queue_limit_memory")
                        if queue_config.maxNewWorkersPerCycle > 0:
                            n_new_workers = min(n_new_workers, queue_config.maxNewWorkersPerCycle)
                            tmp_log.debug(f"setting n_new_workers to {n_new_workers} in order to respect maxNewWorkersPerCycle")
                        if self.maxNewWorkers is not None and self.maxNewWorkers > 0:
                            n_new_workers = min(n_new_workers, self.maxNewWorkers)
                            tmp_log.debug(f"setting n_new_workers to {n_new_workers} in order to respect universal maxNewWorkers")
                        dyn_num_workers[queue_name][job_type][resource_type]["nNewWorkers"] = n_new_workers

                # adjust n_new_workers for UCORE to let aggregations over RT respect nQueueLimitWorker and max_workers
                if queue_config is None:
                    max_new_workers_per_cycle = 0
                    ret_msg = "set max_new_workers_per_cycle=0 in UCORE aggregation due to missing queue_config"
                    tmp_log.debug(ret_msg)
                else:
                    max_new_workers_per_cycle = queue_config.maxNewWorkersPerCycle
                if len(dyn_num_workers[queue_name]) > 1:
                    total_new_workers_rts = 0
                    for _jt in dyn_num_workers[queue_name]:
                        for _rt in dyn_num_workers[queue_name][_jt]:
                            if _jt != "ANY" and _rt != "ANY":
                                total_new_workers_rts = total_new_workers_rts + dyn_num_workers[queue_name][_jt][_rt]["nNewWorkers"]
                    n_new_workers_max_agg = min(max(n_queue_limit - n_queue_total, 0), max(max_workers - n_queue_total - n_ready_total - n_running_total, 0))
                    if max_new_workers_per_cycle >= 0:
                        n_new_workers_max_agg = min(n_new_workers_max_agg, max_new_workers_per_cycle)
                    if self.maxNewWorkers is not None and self.maxNewWorkers > 0:
                        n_new_workers_max_agg = min(n_new_workers_max_agg, self.maxNewWorkers)

                    # exceeded max, to adjust
                    if total_new_workers_rts > n_new_workers_max_agg:
                        if n_new_workers_max_agg == 0:
                            for job_type in dyn_num_workers[queue_name]:
                                for resource_type in dyn_num_workers[queue_name][job_type]:
                                    dyn_num_workers[queue_name][job_type][resource_type]["nNewWorkers"] = 0
                            tmp_log.debug("No n_new_workers since n_new_workers_max_agg=0 for UCORE")
                        else:
                            tmp_log.debug(f"n_new_workers_max_agg={n_new_workers_max_agg} for UCORE")
                            _d = dyn_num_workers[queue_name].copy()
                            del _d["ANY"]

                            # TODO: needs to be recalculated
                            simple_rt_nw_list = []
                            for job_type in _d:  # jt: job type
                                for resource_type in _d[job_type]:  # rt: resource type
                                    simple_rt_nw_list.append([(resource_type, job_type), _d[job_type][resource_type].get("nNewWorkers", 0), 0])

                            _countdown = n_new_workers_max_agg
                            for _rt_list in simple_rt_nw_list:
                                (resource_type, job_type), n_new_workers_orig, _r = _rt_list
                                n_new_workers, remainder = divmod(n_new_workers_orig * n_new_workers_max_agg, total_new_workers_rts)
                                dyn_num_workers[queue_name][job_type].setdefault(resource_type, {"nReady": 0, "nRunning": 0, "nQueue": 0, "nNewWorkers": 0})
                                dyn_num_workers[queue_name][job_type][resource_type]["nNewWorkers"] = n_new_workers
                                _rt_list[2] = remainder
                                _countdown -= n_new_workers
                            _s_list = sorted(simple_rt_nw_list, key=(lambda x: x[1]))
                            sorted_rt_nw_list = sorted(_s_list, key=(lambda x: x[2]), reverse=True)
                            for (resource_type, job_type), n_new_workers_orig, remainder in sorted_rt_nw_list:
                                if _countdown <= 0:
                                    break
                                dyn_num_workers[queue_name][job_type][resource_type]["nNewWorkers"] += 1
                                _countdown -= 1
                        for job_type in dyn_num_workers[queue_name]:
                            for resource_type in dyn_num_workers[queue_name][job_type]:
                                if job_type == "ANY" or resource_type == "ANY":
                                    continue
                                n_new_workers = dyn_num_workers[queue_name][job_type][resource_type]["nNewWorkers"]
                                tmp_log.debug(
                                    "setting n_new_workers to {0} of job_type {1} resource_type {2} in order to respect RT aggregations for UCORE".format(
                                        n_new_workers, job_type, resource_type
                                    )
                                )

                if not apf_msg:
                    apf_data = copy.deepcopy(dyn_num_workers[queue_name])

                self.apf_mon.update_label(queue_name, apf_msg, apf_data)

            # dump
            tmp_log.debug(f"defined {str(dyn_num_workers)}")
            return dyn_num_workers
        except Exception:
            # dump error
            err_msg = core_utils.dump_error_message(tmp_log)
            tmp_log.error(err_msg)
            return None
