import copy
import math
import traceback
from dataclasses import dataclass

import polars as pl

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.resource_type_constants import (
    CAPABILITY_TAG,
    UNIFIED_QUEUE_TAG,
)
from pandaharvester.harvestercore.resource_type_mapper import ResourceTypeMapper
from pandaharvester.harvestermisc.apfmon import Apfmon
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict

# logger
_logger = core_utils.setup_logger("worker_adjuster")

DEFAULT_JOB_TYPE = "managed"
DEFAULT_PILOT_TYPE = "PR"
DEFAULT_PRIORITIZED_PROD_SOURCE_LABELS = ["rc_alrb"]

# polars config
pl.Config.set_ascii_tables(True)
pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_tbl_hide_column_data_types(True)
pl.Config.set_tbl_rows(-1)
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_width_chars(140)
pl.Config.set_tbl_cell_numeric_alignment("RIGHT")


# limits and current usage of a queue
@dataclass
class QueueLimits:
    max_workers: int
    n_queue_limit: int
    queue_limit_cores: int | None
    queue_limit_memory: int | None
    cores_queue: int
    memory_queue: int


# min of values that are not None
def _min_ignoring_none(*values):
    return min(value for value in values if value is not None)


# class to define number of workers to submit
class WorkerAdjuster:
    # constructor
    def __init__(self, queue_config_mapper):
        tmp_log = core_utils.make_logger(_logger, method_name="__init__")
        self.queue_configMapper = queue_config_mapper
        self.pluginFactory = PluginFactory()
        self.dbProxy = DBProxy()
        self.throttlerMap = dict()
        self.apf_mon = Apfmon(self.queue_configMapper)
        self.maxNewWorkers = getattr(harvester_config.submitter, "maxNewWorkers", None)
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
        self.no_pilots_when_no_active_jobs = bool(getattr(harvester_config.submitter, "noPilotsWhenNoActiveJobs", False))

    # transform job statistics dict to polars dataframe
    def _job_stats_to_df(self, job_stats_dict: dict | None) -> pl.DataFrame:
        """
        Transform nested job statistics dict into a polars dataframe.

        Args:
            job_stats_dict (dict|None): Dict structure from getDetailedJobStatistics with form
                {computing_site: {resource_type: {prod_source_label: {job_status: n_jobs}}}}
                If None, returns an empty dataframe with the correct schema.

        Returns:
            polars.DataFrame: Dataframe with columns: computing_site (Utf8), resource_type (Utf8),
                prod_source_label (Utf8), job_status (Utf8), n_jobs (Int64)
        """
        schema = {
            "computing_site": pl.Utf8,
            "resource_type": pl.Utf8,
            "prod_source_label": pl.Utf8,
            "job_status": pl.Utf8,
            "n_jobs": pl.Int64,
        }
        if job_stats_dict is None:
            return pl.DataFrame(schema=schema)
        else:
            return pl.from_records(
                [
                    {
                        "computing_site": computing_site,
                        "resource_type": resource_type,
                        "prod_source_label": prod_source_label,
                        "job_status": job_status,
                        "n_jobs": n_jobs,
                    }
                    for computing_site, resource_types in job_stats_dict.items()
                    for resource_type, prod_labels in resource_types.items()
                    for prod_source_label, statuses in prod_labels.items()
                    for job_status, n_jobs in statuses.items()
                ],
                schema=schema,
            )

    # transform num workers dict to polars dataframe
    def _num_workers_dict_to_df(self, num_workers_dict: dict | None) -> pl.DataFrame:
        """
        Transform nested num workers dict into a polars dataframe.

        Args:
            num_workers_dict (dict|None): Dict structure with form
                {queue_name: {job_type: {resource_type: {pilot_type: {"nQueue": int, "nReady": int, "nRunning": int, "nNewWorkers": int}}}}}
                If None, returns an empty dataframe with the correct schema.

        Returns:
            polars.DataFrame: Dataframe with columns: queue_name (Utf8), job_type (Utf8),
                resource_type (Utf8), pilot_type (Utf8), nQueue (Int64), nReady (Int64), nRunning (Int64), nNewWorkers (Int64)
        """
        schema = {
            "queue_name": pl.Utf8,
            "job_type": pl.Utf8,
            "resource_type": pl.Utf8,
            "pilot_type": pl.Utf8,
            "nQueue": pl.Int64,
            "nReady": pl.Int64,
            "nRunning": pl.Int64,
            "nNewWorkers": pl.Int64,
        }

        if num_workers_dict is None:
            return pl.DataFrame(schema=schema)
        else:
            return pl.from_records(
                [
                    {
                        "queue_name": queue_name,
                        "job_type": job_type,
                        "resource_type": resource_type,
                        "pilot_type": pilot_type,
                        "nQueue": pilot_data.get("nQueue", 0),
                        "nReady": pilot_data.get("nReady", 0),
                        "nRunning": pilot_data.get("nRunning", 0),
                        "nNewWorkers": pilot_data.get("nNewWorkers", 0),
                    }
                    for queue_name, job_types in num_workers_dict.items()
                    for job_type, resource_types in job_types.items()
                    for resource_type, pilot_types in resource_types.items()
                    for pilot_type, pilot_data in pilot_types.items()
                ],
                schema=schema,
            )

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
            is_unified_queue = queue_dict.get(CAPABILITY_TAG, "") == UNIFIED_QUEUE_TAG
            nCoreFactor = queue_config.submitter.get("nCoreFactor", 1)
            if type(nCoreFactor) in [dict]:
                if job_type in nCoreFactor:
                    t_job_type = job_type
                else:
                    t_job_type = "Any"
                if is_unified_queue:
                    t_resource_type = resource_type
                else:
                    t_resource_type = "Undefined"
                n_core_factor = nCoreFactor.get(t_job_type, {}).get(t_resource_type, 1)
                return int(n_core_factor)
            else:
                return int(nCoreFactor)
        except Exception as ex:
            tmp_logger.warning(f"Failed to get core factor: {ex}")
        return 1

    # get queue activate worker factor
    def get_queue_activate_worker_factor(self, site_name=None, job_type=None, resource_type=None, queue_dict=None, queue_config=None):
        tmp_log = core_utils.make_logger(_logger, f"site={site_name}", method_name="get_queue_activate_worker_factor")
        ret_val = 1.0

        # balance in a queue when MCore is used in a pilot wrapper
        try:
            if queue_config:
                nCoreFactor = self.get_core_factor(queue_config, queue_dict, job_type, resource_type, tmp_log)

                ret_val = 1.0 / nCoreFactor
        except Exception:
            pass
        tmp_log.debug(f"ret_val={ret_val}")
        return ret_val

    # get activate worker factor
    def get_activate_worker_factor(self, site_name=None, job_type=None, resource_type=None, queue_dict=None, queue_config=None):
        tmp_log = core_utils.make_logger(_logger, f"site={site_name}", method_name="get_activate_worker_factor")
        ret_val = 1.0

        if queue_config.runMode == "slave":
            ret_val = 1.0
        else:
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

        queue_factor = self.get_queue_activate_worker_factor(
            site_name=site_name, job_type=job_type, resource_type=resource_type, queue_dict=queue_dict, queue_config=queue_config
        )
        ret_val = ret_val * queue_factor

        tmp_log.debug(f"ret_val={ret_val}")
        return ret_val

    # Helper methods for dataframe transformations in define_num_workers

    def _build_new_workers_df(self, static_num_workers, queue_name):
        """Build and filter workers dataframe for a given queue."""
        return (
            self._num_workers_dict_to_df({queue_name: static_num_workers[queue_name]})
            .filter(pl.col("resource_type").is_not_null())
            .filter(pl.col("pilot_type").is_not_null())
            .with_columns(
                [
                    pl.col("nQueue").fill_null(0),
                    pl.col("nReady").fill_null(0),
                    pl.col("nRunning").fill_null(0),
                    pl.col("nNewWorkers").fill_null(0),
                ]
            )
        )

    def _build_activated_df(self, job_stats_new_df, queue_name):
        """Build activated jobs dataframe with aggregations for ANY pilot types."""
        activated_df = (
            job_stats_new_df.filter((pl.col("computing_site") == queue_name) & (pl.col("job_status") == "activated"))
            .with_columns(
                pl.col("computing_site").alias("queue_name"),
                pl.col("prod_source_label").map_elements(core_utils.prod_source_label_to_pilot_type, return_dtype=pl.Utf8).alias("pilot_type"),
            )
            .select(["queue_name", "resource_type", "pilot_type", "n_jobs"])
        )
        # Add aggregated rows with pilot_type="ANY" (sum over all pilot_types for each resource_type)
        activated_df_any_pt = (
            activated_df.select(["queue_name", "resource_type", "n_jobs"])
            .group_by(["queue_name", "resource_type"])
            .agg(pl.col("n_jobs").sum())
            .with_columns(pl.lit("ANY").alias("pilot_type"))
            .select(["queue_name", "resource_type", "pilot_type", "n_jobs"])
        )
        # Add aggregated row with both resource_type="ANY" and pilot_type="ANY" (sum over all)
        activated_df_any_both = (
            activated_df.select(["queue_name", "n_jobs"])
            .group_by(["queue_name"])
            .agg(pl.col("n_jobs").sum())
            .with_columns(pl.lit("ANY").alias("resource_type"), pl.lit("ANY").alias("pilot_type"))
            .select(["queue_name", "resource_type", "pilot_type", "n_jobs"])
        )
        return pl.concat([activated_df, activated_df_any_pt, activated_df_any_both])

    def _join_workers_activated_dfs(self, activated_df, tmp_new_workers_df, queue_name):
        """Join activated jobs and workers dataframes."""
        return (
            activated_df.join(
                tmp_new_workers_df,
                on=["queue_name", "resource_type", "pilot_type"],
                how="full",
                suffix="_right",
            )
            .with_columns(
                [
                    pl.col("queue_name").fill_null(pl.lit(queue_name)),
                    pl.col("job_type").fill_null(DEFAULT_JOB_TYPE),
                    pl.coalesce(pl.col("resource_type"), pl.col("resource_type_right")).fill_null(pl.lit("ANY")).alias("resource_type"),
                    pl.coalesce(pl.col("pilot_type"), pl.col("pilot_type_right")).fill_null(pl.lit("ANY")).alias("pilot_type"),
                    pl.col("nQueue").fill_null(0),
                    pl.col("nReady").fill_null(0),
                    pl.col("nRunning").fill_null(0),
                    pl.col("nNewWorkers").fill_null(0),
                    pl.col("n_jobs").fill_null(0),
                ]
            )
            .select(pl.all().exclude(["resource_type_right", "pilot_type_right"]))
        )

    def _build_master_df(self, joined_df, queue_name, static_num_workers):
        """Build master dataframe with grouping, default rows, and sorting."""
        tmp_master_df = joined_df.group_by(["queue_name", "job_type", "resource_type", "pilot_type"]).agg(
            pl.col("nQueue").max(),
            pl.col("nReady").max(),
            pl.col("nRunning").max(),
            pl.col("nNewWorkers").max(),
            pl.col("n_jobs").sum().alias("n_activated_jobs"),
        )

        # Ensure DEFAULT_PILOT_TYPE exists in static_num_workers and master_df for all (job_type, resource_type) pairs
        required_default_rows = []
        for job_type in static_num_workers[queue_name]:
            for resource_type in static_num_workers[queue_name][job_type]:
                static_num_workers[queue_name][job_type][resource_type].setdefault(
                    DEFAULT_PILOT_TYPE, {"nReady": 0, "nRunning": 0, "nQueue": 0, "nNewWorkers": 0}
                )
                required_default_rows.append(
                    {
                        "queue_name": queue_name,
                        "job_type": job_type,
                        "resource_type": resource_type,
                    }
                )

        if required_default_rows:
            required_default_df = pl.DataFrame(required_default_rows)
            existing_default_df = (
                tmp_master_df.filter((pl.col("queue_name") == queue_name) & (pl.col("pilot_type") == DEFAULT_PILOT_TYPE))
                .select(["queue_name", "job_type", "resource_type"])
                .unique()
            )
            missing_default_df = required_default_df.join(
                existing_default_df,
                on=["queue_name", "job_type", "resource_type"],
                how="anti",
            )
            if missing_default_df.height > 0:
                missing_default_df = missing_default_df.with_columns(
                    [
                        pl.lit(DEFAULT_PILOT_TYPE).alias("pilot_type"),
                        pl.lit(0, dtype=pl.Int64).alias("nQueue"),
                        pl.lit(0, dtype=pl.Int64).alias("nReady"),
                        pl.lit(0, dtype=pl.Int64).alias("nRunning"),
                        pl.lit(0, dtype=pl.Int64).alias("nNewWorkers"),
                        pl.lit(0, dtype=pl.Int64).alias("n_activated_jobs"),
                    ]
                ).select(
                    [
                        "queue_name",
                        "job_type",
                        "resource_type",
                        "pilot_type",
                        "nQueue",
                        "nReady",
                        "nRunning",
                        "nNewWorkers",
                        "n_activated_jobs",
                    ]
                )
                tmp_master_df = pl.concat([tmp_master_df, missing_default_df])

        # Sort master_df to have consistent order and prioritized pilot types on top
        return tmp_master_df.sort(
            [
                "queue_name",
                pl.when(pl.col("job_type") == "ANY").then(1).otherwise(0),
                "job_type",
                pl.when(pl.col("resource_type") == "ANY").then(1).otherwise(0),
                "resource_type",
                pl.when(pl.col("pilot_type") == "ANY").then(2).when(pl.col("pilot_type") == DEFAULT_PILOT_TYPE).then(0).otherwise(1),
                "pilot_type",
            ]
        )

    def _sync_master_df_to_static_workers(self, tmp_master_df, tmp_static_num_workers):
        """Update tmp_static_num_workers dict from master dataframe."""
        for row in tmp_master_df.iter_rows(named=True):
            queue_name_from_row = row["queue_name"]
            job_type = row["job_type"]
            resource_type = row["resource_type"]
            pilot_type = row["pilot_type"]
            # create missing keys in nested dictionary
            if queue_name_from_row not in tmp_static_num_workers:
                tmp_static_num_workers[queue_name_from_row] = {}
            if job_type not in tmp_static_num_workers[queue_name_from_row]:
                tmp_static_num_workers[queue_name_from_row][job_type] = {}
            if resource_type not in tmp_static_num_workers[queue_name_from_row][job_type]:
                tmp_static_num_workers[queue_name_from_row][job_type][resource_type] = {}
            if pilot_type not in tmp_static_num_workers[queue_name_from_row][job_type][resource_type]:
                tmp_static_num_workers[queue_name_from_row][job_type][resource_type][pilot_type] = {}
            # update values
            tmp_static_num_workers[queue_name_from_row][job_type][resource_type][pilot_type].update(
                {
                    "nQueue": row["nQueue"],
                    "nReady": row["nReady"],
                    "nRunning": row["nRunning"],
                    "nNewWorkers": row["nNewWorkers"],
                }
            )

    def _set_initial_new_workers(
        self, tmp_master_df, tmp_static_num_workers, static_num_workers, queue_name, queue_dict, queue_config, prioritized_pilot_types, tmp_log
    ):
        """Set initial nNewWorkers for pilot types based on activated jobs and activation factor; return them as {(job_type, resource_type, pilot_type): nNewWorkers}."""
        # number of activated jobs and queued workers, as {(job_type, resource_type): {pilot_type: (n_activated_jobs, nQueue)}}
        ptype_stats = {}
        for row in tmp_master_df.iter_rows(named=True):
            ptype_stats.setdefault((row["job_type"], row["resource_type"]), {})[row["pilot_type"]] = (row["n_activated_jobs"], row["nQueue"])
        initial_new_workers = {}
        for job_type in tmp_static_num_workers[queue_name]:
            for resource_type, pilot_type_dict in tmp_static_num_workers[queue_name][job_type].items():
                total_n_new_workers = pilot_type_dict["ANY"]["nNewWorkers"]
                if total_n_new_workers <= 0:
                    continue
                # calculate the total number of new workers needed for prioritized pilot types
                remaining_n_new_workers = total_n_new_workers
                activate_worker_factor = self.get_activate_worker_factor(queue_name, job_type, resource_type, queue_dict, queue_config)
                rt_ptype_stats = ptype_stats.get((job_type, resource_type), {})
                prio_ptype_stats = [val for pilot_type, val in rt_ptype_stats.items() if pilot_type in prioritized_pilot_types]
                total_prio_ptype_n_activated_jobs = sum(n_activated_jobs for n_activated_jobs, _ in prio_ptype_stats)
                total_prio_ptype_nQueue = sum(nQueue for _, nQueue in prio_ptype_stats)
                total_prio_ptype_calculated_n_new_workers = max(int(total_prio_ptype_n_activated_jobs * activate_worker_factor) - total_prio_ptype_nQueue, 0)
                if total_prio_ptype_calculated_n_new_workers > 0:
                    adjust_ratio = min(total_n_new_workers / total_prio_ptype_calculated_n_new_workers, 1)
                    for pilot_type in pilot_type_dict:
                        if pilot_type in prioritized_pilot_types:
                            n_activated_jobs, nQueue = rt_ptype_stats.get(pilot_type, (0, 0))
                            calculated_n_new_workers = int(max(int(n_activated_jobs * activate_worker_factor) - nQueue, 0) * adjust_ratio)
                            if calculated_n_new_workers <= 0:
                                continue
                            tmp_static_num_workers[queue_name][job_type][resource_type][pilot_type]["nNewWorkers"] = calculated_n_new_workers
                            static_num_workers[queue_name].setdefault(job_type, {}).setdefault(resource_type, {}).setdefault(
                                pilot_type, {"nReady": 0, "nRunning": 0, "nQueue": 0, "nNewWorkers": 0}
                            )["nNewWorkers"] = calculated_n_new_workers
                            remaining_n_new_workers -= calculated_n_new_workers
                            initial_new_workers[(job_type, resource_type, pilot_type)] = calculated_n_new_workers
                            tmp_log.debug(
                                f"Set initial nNewWorkers to {calculated_n_new_workers} for queue={queue_name} job_type={job_type} resource_type={resource_type} pilot_type={pilot_type}"
                            )
                if remaining_n_new_workers > 0:
                    # allocate remaining n_new_workers to DEFAULT_PILOT_TYPE PR
                    tmp_static_num_workers[queue_name][job_type][resource_type][DEFAULT_PILOT_TYPE]["nNewWorkers"] += remaining_n_new_workers
                    default_n_new_workers = tmp_static_num_workers[queue_name][job_type][resource_type][DEFAULT_PILOT_TYPE]["nNewWorkers"]
                    static_num_workers[queue_name].setdefault(job_type, {}).setdefault(resource_type, {}).setdefault(
                        DEFAULT_PILOT_TYPE, {"nReady": 0, "nRunning": 0, "nQueue": 0, "nNewWorkers": 0}
                    )["nNewWorkers"] = default_n_new_workers
                    initial_new_workers[(job_type, resource_type, DEFAULT_PILOT_TYPE)] = default_n_new_workers
                    tmp_log.debug(
                        f"Set remaining nNewWorkers to {remaining_n_new_workers} for queue={queue_name} job_type={job_type} resource_type={resource_type} pilot_type={DEFAULT_PILOT_TYPE}"
                    )
        return initial_new_workers

    def _format_result_dataframe(self, dyn_num_workers, tmp_log):
        """Format result dataframe for logging."""
        dyn_num_workers_rows = []
        for qn, job_types in dyn_num_workers.items():
            for job_type, resource_types in job_types.items():
                for resource_type, pilot_types in resource_types.items():
                    for pilot_type, worker_data in pilot_types.items():
                        dyn_num_workers_rows.append(
                            {
                                "queue_name": qn,
                                "job_type": job_type,
                                "resource_type": resource_type,
                                "pilot_type": pilot_type,
                                "nQueue": worker_data.get("nQueue", 0),
                                "nReady": worker_data.get("nReady", 0),
                                "nRunning": worker_data.get("nRunning", 0),
                                "nNewWorkers": worker_data.get("nNewWorkers", 0),
                            }
                        )
        if dyn_num_workers_rows:
            result_df = (
                pl.DataFrame(dyn_num_workers_rows)
                .select(pl.all().exclude(["queue_name"]))
                .sort(
                    [
                        pl.when(pl.col("job_type") == "ANY").then(1).otherwise(0),
                        "job_type",
                        pl.when(pl.col("resource_type") == "ANY").then(1).otherwise(0),
                        "resource_type",
                        pl.when(pl.col("pilot_type") == "ANY").then(2).when(pl.col("pilot_type") == DEFAULT_PILOT_TYPE).then(0).otherwise(1),
                        "pilot_type",
                    ]
                )
            )
            tmp_log.debug(f"result_df:\n{result_df}")
        else:
            tmp_log.debug("result_df: nothing to display")

    # normalize job types of a queue in place: keep job type ANY, and merge all other job types into DEFAULT_JOB_TYPE
    def _normalize_job_type_any(self, queue_dict, tmp_log):
        tmp_log.debug(f"normalize_job_type_any got: {queue_dict}")
        if len(queue_dict) == 1:
            only_job_type = next(iter(queue_dict))
            queue_dict[DEFAULT_JOB_TYPE] = queue_dict.pop(only_job_type)
            tmp_log.debug(f"normalize_job_type_any returned: {queue_dict}")
            return
        merged = {}
        any_job_types = {}
        for _job_type, rt_map in queue_dict.items():
            if _job_type == "ANY":
                any_job_types[_job_type] = rt_map
                continue
            for rt, stats in rt_map.items():
                if rt not in merged:
                    merged[rt] = copy.deepcopy(stats)
                    continue
                for key, val in stats.items():
                    if isinstance(val, (int, float)):
                        merged[rt][key] = merged[rt].get(key, 0) + val
                    else:
                        merged[rt].setdefault(key, val)
        queue_dict.clear()
        queue_dict.update(any_job_types)
        queue_dict[DEFAULT_JOB_TYPE] = merged
        tmp_log.debug(f"normalize_job_type_any returned: {queue_dict}")

    # whether a cache entry is missing or older than max_age seconds
    @staticmethod
    def _is_cache_stale(cache_spec, max_age):
        return cache_spec is None or cache_spec.lastUpdate is None or (core_utils.naive_utcnow() - cache_spec.lastUpdate).total_seconds() > max_age

    # get queue status, job statistics, and detailed job statistics of the queues as dataframe from the cache
    def _get_demand_inputs(self, queue_names, tmp_log):
        # get queue status
        queue_stat = self.dbProxy.get_cache("panda_queues.json", None)
        queue_stat = queue_stat.data if queue_stat is not None else dict()

        # get job statistics
        job_stats_spec = self.dbProxy.get_cache("job_statistics.json", None)
        job_stats = job_stats_spec.data if job_stats_spec is not None else None
        job_stats_new_spec = self.dbProxy.get_cache("job_statistics_new.json", None)
        job_stats_new = job_stats_new_spec.data if job_stats_new_spec is not None else None

        # job statistics missing or older than jobStatsMaxAge seconds are not used as demand
        job_stats_max_age = getattr(harvester_config.submitter, "jobStatsMaxAge", 0)
        if job_stats_max_age > 0:
            if self._is_cache_stale(job_stats_spec, job_stats_max_age):
                tmp_log.error(f"job statistics missing or older than jobStatsMaxAge={job_stats_max_age}s; no demand from activated jobs")
                job_stats = {}
                job_stats_new = None
            if self._is_cache_stale(job_stats_new_spec, job_stats_max_age):
                tmp_log.error(f"detailed job statistics missing or older than jobStatsMaxAge={job_stats_max_age}s; no demand from detailed activated jobs")
                job_stats_new = None

        # detailed job statistics of the queues only
        if job_stats_new is not None:
            job_stats_new = {computing_site: val for computing_site, val in job_stats_new.items() if computing_site in queue_names}
        return queue_stat, job_stats, self._job_stats_to_df(job_stats_new)

    # get pilot types to prioritize for a queue
    @staticmethod
    def _get_prioritized_pilot_types(queue_config):
        prioritized_pslabels = getattr(queue_config, "prioritizedProdSourceLabels", DEFAULT_PRIORITIZED_PROD_SOURCE_LABELS)
        return [core_utils.prod_source_label_to_pilot_type(label) for label in prioritized_pslabels]

    # distribute nNewWorkers of pilot type ANY of a queue to pilot types based on activated jobs, then remove pilot type ANY
    def _distribute_new_workers_to_pilot_types(self, static_num_workers, queue_name, queue_config, queue_dict, job_stats_new_df, tmp_log):
        if queue_config is None:
            tmp_log.warning(f"missing queue_config for queue: {queue_name}")
        else:
            tmp_new_workers_df = self._build_new_workers_df(static_num_workers, queue_name)
            activated_df = self._build_activated_df(job_stats_new_df, queue_name)
            joined_df = self._join_workers_activated_dfs(activated_df, tmp_new_workers_df, queue_name)

            tmp_master_df = self._build_master_df(joined_df, queue_name, static_num_workers)
            tmp_static_num_workers = {queue_name: copy.deepcopy(static_num_workers[queue_name])}

            # update tmp_static_num_workers with tmp_master_df
            self._sync_master_df_to_static_workers(tmp_master_df, tmp_static_num_workers)

            # set initial nNewWorkers for pilot types based on number of activated jobs and the activate worker factor
            initial_new_workers = self._set_initial_new_workers(
                tmp_master_df,
                tmp_static_num_workers,
                static_num_workers,
                queue_name,
                queue_dict,
                queue_config,
                self._get_prioritized_pilot_types(queue_config),
                tmp_log,
            )
            # show the initial nNewWorkers in master_df
            master_df = tmp_master_df
            if initial_new_workers:
                master_df = tmp_master_df.with_columns(
                    pl.concat_str(["job_type", "resource_type", "pilot_type"], separator="|")
                    .replace_strict({"|".join(key): val for key, val in initial_new_workers.items()}, default=pl.col("nNewWorkers"), return_dtype=pl.Int64)
                    .alias("nNewWorkers")
                )
            display_master_df = master_df.select(["job_type", "resource_type", "pilot_type", "nQueue", "nReady", "nRunning", "nNewWorkers", "n_activated_jobs"])
            tmp_log.debug(f"master_df: \n{display_master_df}")
        # remove pilot type ANY
        for pilot_type_dict_map in static_num_workers[queue_name].values():
            for pilot_type_dict in pilot_type_dict_map.values():
                pilot_type_dict.pop("ANY", None)

    # get the throttler of a queue; made again when the queue configuration changes
    def _get_throttler(self, queue_name, queue_config):
        config_id = getattr(queue_config, "configID", None)
        cached = self.throttlerMap.get(queue_name)
        if cached is None or cached[0] != config_id:
            throttler = self.pluginFactory.get_plugin(queue_config.throttler) if hasattr(queue_config, "throttler") else None
            cached = (config_id, throttler)
            self.throttlerMap[queue_name] = cached
        return cached[1]

    # get limits and current usage of a queue
    def _get_queue_limits(self, queue_name, queue_config):
        worker_limits_dict, worker_stats_map = self.dbProxy.get_worker_limits(queue_name, queue_config)
        return QueueLimits(
            max_workers=worker_limits_dict.get("maxWorkers", 0),
            n_queue_limit=worker_limits_dict.get("nQueueLimitWorker", 0),
            queue_limit_cores=worker_limits_dict.get("nQueueWorkerCores"),
            queue_limit_memory=worker_limits_dict.get("nQueueWorkerMemory"),
            cores_queue=worker_stats_map["queue"]["core"],
            memory_queue=worker_stats_map["queue"]["mem"],
        )

    # calculate the number of new workers of a resource type and pilot type within the limits of the queue
    def _calculate_n_new_workers(
        self, queue_name, queue_config, queue_dict, job_type, resource_type, tmp_val, n_new_workers_def, limits, job_stats, rtype_request, tmp_log
    ):
        n_queue = tmp_val["nQueue"]
        n_ready = tmp_val["nReady"]
        n_running = tmp_val["nRunning"]
        # the limit of queued workers is the one of the whole queue; nQueueLimitWorkerPerRT is not used on purpose
        if n_queue >= limits.n_queue_limit > 0:
            # enough queued workers
            tmp_log.debug(f"No n_new_workers since n_queue({n_queue})>=n_queue_limit({limits.n_queue_limit})")
            return 0
        if (n_queue + n_ready + n_running) >= limits.max_workers > 0:
            # enough workers in the system
            tmp_log.debug(f"No n_new_workers since n_queue({n_queue}) + n_ready({n_ready}) + n_running({n_running}) >= max_workers({limits.max_workers})")
            return 0
        if limits.queue_limit_cores is not None and limits.cores_queue >= limits.queue_limit_cores:
            # enough queuing cores
            tmp_log.debug(f"No n_new_workers since cores_queue({limits.cores_queue}) >= queue_limit_cores({limits.queue_limit_cores})")
            return 0
        if limits.queue_limit_memory is not None and limits.memory_queue >= limits.queue_limit_memory:
            # enough queuing memory
            tmp_log.debug(f"No n_new_workers since memory_queue({limits.memory_queue} MB) >= queue_limit_memory({limits.queue_limit_memory} MB)")
            return 0

        # the limit set for the queue, if any
        max_queued_workers = limits.n_queue_limit if limits.n_queue_limit > 0 else None

        # Reset the maxQueueWorkers according to particular
        if n_new_workers_def is not None:
            # don't surpass limits given centrally
            max_queued_workers = _min_ignoring_none(n_new_workers_def + n_queue, max_queued_workers)
        elif queue_config.mapType == "NoJob":
            # for pull mode, limit to activated jobs
            if job_stats is None:
                tmp_log.warning("n_activated not defined, defaulting to configured queue limits")
            else:
                # limit the queue to the number of activated jobs to avoid empty pilots
                no_pilots_when_no_active_jobs = self.get_queue_no_pilots_when_no_active_jobs(queue_name)
                tmp_n_activated_jobs = (job_stats.get(queue_name) or {}).get("activated")
                if tmp_n_activated_jobs is None:
                    # zero job in the queue
                    tmp_log.debug("no job in queue")
                    if no_pilots_when_no_active_jobs:
                        n_activated = 0
                    else:
                        n_activated = max(1 - n_queue - n_ready - n_running, 0)
                else:
                    tmp_log.debug(f"available activated panda jobs {tmp_n_activated_jobs}")
                    activate_worker_factor = self.get_activate_worker_factor(queue_name, job_type, resource_type, queue_dict, queue_config)
                    # at least one pilot to avoid no activity queues, unless no pilots wanted without activated jobs
                    if no_pilots_when_no_active_jobs and tmp_n_activated_jobs * activate_worker_factor <= 0:
                        n_min_pilots = 0
                    else:
                        n_min_pilots = 1
                    n_activated = max(int(tmp_n_activated_jobs * activate_worker_factor), n_min_pilots)
                tmp_log.debug(f"limiting max_queued_workers to min(n_activated={n_activated}, queue_limit={max_queued_workers})")
                max_queued_workers = _min_ignoring_none(n_activated, max_queued_workers)

        if max_queued_workers is None:  # no value found, use default value
            max_queued_workers = 1

        # new workers
        n_new_workers = max(max_queued_workers - n_queue, 0)
        tmp_log.debug(f"setting n_new_workers to {n_new_workers} in max_queued_workers calculation")
        if limits.max_workers > 0:
            n_new_workers = min(n_new_workers, max(limits.max_workers - n_queue - n_ready - n_running, 0))
            tmp_log.debug(f"setting n_new_workers to {n_new_workers} to respect max_workers")
        rtype_request_cores, rtype_request_memory = rtype_request
        if limits.queue_limit_cores:
            new_worker_cores_max = max(limits.queue_limit_cores - limits.cores_queue, 0)
            n_new_workers = min(n_new_workers, math.ceil(new_worker_cores_max / rtype_request_cores))
            tmp_log.debug(f"setting n_new_workers to {n_new_workers} to respect queue_limit_cores")
        if limits.queue_limit_memory:
            new_worker_memory_max = max(limits.queue_limit_memory - limits.memory_queue, 0)
            n_new_workers = min(n_new_workers, math.ceil(new_worker_memory_max / rtype_request_memory))
            tmp_log.debug(f"setting n_new_workers to {n_new_workers} to respect queue_limit_memory")
        if queue_config.maxNewWorkersPerCycle > 0:
            n_new_workers = min(n_new_workers, queue_config.maxNewWorkersPerCycle)
            tmp_log.debug(f"setting n_new_workers to {n_new_workers} in order to respect maxNewWorkersPerCycle")
        if self.maxNewWorkers is not None and self.maxNewWorkers > 0:
            n_new_workers = min(n_new_workers, self.maxNewWorkers)
            tmp_log.debug(f"setting n_new_workers to {n_new_workers} in order to respect universal maxNewWorkers")
        return n_new_workers

    # scale down nNewWorkers of resource types and pilot types of a queue with several job types, so that their sum respects the limits of the queue
    def _apply_aggregation_limits(self, queue_name, queue_config, queue_num_workers, limits, n_totals, tmp_log):
        n_queue_total, n_ready_total, n_running_total = n_totals
        max_new_workers_per_cycle = queue_config.maxNewWorkersPerCycle
        total_new_workers_rts = 0
        for _jt, rt_map in queue_num_workers.items():
            for _rt, pt_map in rt_map.items():
                if _jt != "ANY" and _rt != "ANY":
                    for pt_val in pt_map.values():
                        total_new_workers_rts += pt_val["nNewWorkers"]

        n_new_workers_max_agg = min(max(limits.n_queue_limit - n_queue_total, 0), max(limits.max_workers - n_queue_total - n_ready_total - n_running_total, 0))
        if max_new_workers_per_cycle >= 0:
            n_new_workers_max_agg = min(n_new_workers_max_agg, max_new_workers_per_cycle)
        if self.maxNewWorkers is not None and self.maxNewWorkers > 0:
            n_new_workers_max_agg = min(n_new_workers_max_agg, self.maxNewWorkers)

        # not exceeded max
        if total_new_workers_rts <= n_new_workers_max_agg:
            return
        if n_new_workers_max_agg == 0:
            for rt_map in queue_num_workers.values():
                for pt_map in rt_map.values():
                    for pt_val in pt_map.values():
                        pt_val["nNewWorkers"] = 0
            tmp_log.debug("No n_new_workers since n_new_workers_max_agg=0 for UCORE")
        else:
            tmp_log.debug(f"n_new_workers_max_agg={n_new_workers_max_agg} for UCORE")
            prioritized_pilot_types = self._get_prioritized_pilot_types(queue_config)
            # TODO: needs to be recalculated
            # share n_new_workers_max_agg in proportion to nNewWorkers, as [(resource_type, job_type, pilot_type), n_new_workers_orig, remainder]
            simple_rt_nw_list = []
            for job_type, rt_map in queue_num_workers.items():
                if job_type == "ANY":
                    continue
                for resource_type, pt_map in rt_map.items():
                    for pilot_type, pt_val in pt_map.items():
                        simple_rt_nw_list.append([(resource_type, job_type, pilot_type), pt_val.get("nNewWorkers", 0), 0])
            _countdown = n_new_workers_max_agg
            for _rt_list in simple_rt_nw_list:
                (resource_type, job_type, pilot_type), n_new_workers_orig, _r = _rt_list
                n_new_workers, remainder = divmod(n_new_workers_orig * n_new_workers_max_agg, total_new_workers_rts)
                queue_num_workers[job_type][resource_type][pilot_type]["nNewWorkers"] = n_new_workers
                _rt_list[2] = remainder
                _countdown -= n_new_workers
            # give the rest one by one, sorted by whether n_new_workers_orig > 0 (favor positive over 0), then by pilot_type (favor prioritized),
            # then by remainder (descending), then by original n_new_workers (favor smaller ones)
            sorted_rt_nw_list = sorted(simple_rt_nw_list, key=(lambda x: (not (x[1] > 0), x[0][2] not in prioritized_pilot_types, -x[2], x[1])))
            for (resource_type, job_type, pilot_type), n_new_workers_orig, remainder in sorted_rt_nw_list:
                if _countdown <= 0:
                    break
                queue_num_workers[job_type][resource_type][pilot_type]["nNewWorkers"] += 1
                _countdown -= 1
        for job_type, rt_map in queue_num_workers.items():
            for resource_type, pt_map in rt_map.items():
                if job_type == "ANY" or resource_type == "ANY":
                    continue
                for pilot_type, pt_val in pt_map.items():
                    tmp_log.debug(
                        f"setting n_new_workers to {pt_val['nNewWorkers']} of job_type={job_type} resource_type={resource_type} pilot_type={pilot_type} "
                        "in order to respect rtype aggregations for UCORE"
                    )

    # define nNewWorkers of all resource types and pilot types of a queue in dyn_num_workers, and report to APF monitoring
    def _define_queue_new_workers(
        self, queue_name, queue_config, queue_dict, queue_num_workers, queue_dyn_num_workers, queue_stat, job_stats, rt_mapper, tmp_log
    ):
        job_type = DEFAULT_JOB_TYPE
        # checks for the whole queue
        apf_msg = None
        zero_reason = None
        queue_status = (queue_stat.get(queue_name) or {}).get("status")
        if queue_status in ["offline", "standby", "maintenance"]:
            # the queue is disabled
            zero_reason = f"status={queue_status}"
            apf_msg = f"Not submitting workers since queue status = {queue_status}"
        elif queue_config is None:
            # protection against not-up-to-date queue config
            tmp_log.warning(f"missing queue_config for queue: {queue_name}")
            zero_reason = "missing queue_config"
            apf_msg = "Not submitting workers because of missing queue_config"
        else:
            # check throttler
            throttler = self._get_throttler(queue_name, queue_config)
            if throttler is not None:
                to_throttle, tmp_msg = throttler.to_be_throttled(queue_config, queue_config_mapper=self.queue_configMapper)
                if to_throttle:
                    zero_reason = f"{throttler.__class__.__name__}:{tmp_msg}"
        if zero_reason is not None:
            # set 0 to num of new workers of all resource types and pilot types
            for pilot_type_dict in queue_dyn_num_workers[job_type].values():
                for dyn_val in pilot_type_dict.values():
                    dyn_val["nNewWorkers"] = 0
            tmp_log.debug(f"set n_new_workers=0 for all resource types and pilot types by {zero_reason}")
            self.apf_mon.update_label(queue_name, apf_msg, None if apf_msg else copy.deepcopy(queue_dyn_num_workers))
            return

        limits = self._get_queue_limits(queue_name, queue_config)
        n_queue_total, n_ready_total, n_running_total = 0, 0, 0
        # loop over resource types and pilot types to define nNewWorkers
        for resource_type, pilot_type_dict in queue_num_workers[job_type].items():
            for pilot_type, tmp_val in pilot_type_dict.items():
                tmp_log.debug(
                    f"Processing queue={queue_name} job_type={job_type} resource_type={resource_type} pilot_type={pilot_type} with static_num_workers={tmp_val}"
                )
                dyn_val = queue_dyn_num_workers[job_type][resource_type][pilot_type]

                # check stats
                if resource_type != "ANY":
                    n_queue_total += tmp_val["nQueue"]
                    n_ready_total += tmp_val["nReady"]
                    n_running_total += tmp_val["nRunning"]
                n_new_workers_def = None
                if queue_config.runMode == "slave" or pilot_type != DEFAULT_PILOT_TYPE:
                    # number of new workers given by panda in slave mode, or set for the pilot type
                    n_new_workers_def = tmp_val["nNewWorkers"]
                    if n_new_workers_def == 0:
                        dyn_val["nNewWorkers"] = 0
                        if queue_config.runMode == "slave":
                            tmp_log.debug("set n_new_workers=0 by panda in slave mode")
                        else:
                            tmp_log.debug(f"got n_new_workers=0 for non-{DEFAULT_PILOT_TYPE} pilot_type in self mode; skipped")
                        continue

                # define num of new workers based on static site config
                rtype_request = rt_mapper.calculate_worker_requirements(resource_type, queue_dict)
                dyn_val["nNewWorkers"] = self._calculate_n_new_workers(
                    queue_name, queue_config, queue_dict, job_type, resource_type, tmp_val, n_new_workers_def, limits, job_stats, rtype_request, tmp_log
                )

        # adjust n_new_workers for UCORE to let aggregations over rtype respect nQueueLimitWorker and max_workers
        if len(queue_dyn_num_workers) > 1:
            self._apply_aggregation_limits(queue_name, queue_config, queue_dyn_num_workers, limits, (n_queue_total, n_ready_total, n_running_total), tmp_log)

        self.apf_mon.update_label(queue_name, None, copy.deepcopy(queue_dyn_num_workers))

    # define number of workers to submit based on various information
    def define_num_workers(self, static_num_workers, site_name) -> dict | None:
        """
        Define number of workers to submit based on various information, including static site config, queue status, job statistics, and throttler if defined. The function also updates APF monitoring with the decision and the reason.

        Args:
            static_num_workers (dict): A dict of the form {queue_name: {job_type: {resource_type: {pilot_type: {"nQueue": int, "nReady": int, "nRunning": int, "nNewWorkers": int}}}}} defining the static number of workers to submit for each queue, job type, resource type and pilot type.
            site_name (str): The name of the site for which to define the number of workers.

        Returns:
            (dict|None): The updated static_num_workers dict with the defined number of new workers to submit in the "nNewWorkers" field, or None if an error occurred.
        """
        tmp_log = core_utils.make_logger(_logger, f"site={site_name}", method_name="define_num_workers")
        tmp_log.debug("start")
        tmp_log.debug(f"static_num_workers: {static_num_workers}")
        try:
            static_num_workers = copy.deepcopy(static_num_workers)
            for queue_dict in static_num_workers.values():
                self._normalize_job_type_any(queue_dict, tmp_log)

            queue_stat, job_stats, job_stats_new_df = self._get_demand_inputs(static_num_workers.keys(), tmp_log)

            # get panda queues dict from CRIC
            panda_queues_dict = PandaQueuesDict()

            # get resource type mapper
            rt_mapper = ResourceTypeMapper()

            # get queue configs
            queue_config_map = {queue_name: self.queue_configMapper.get_queue(queue_name) for queue_name in static_num_workers}

            # distribute new workers to pilot types
            for queue_name, queue_config in queue_config_map.items():
                self._distribute_new_workers_to_pilot_types(
                    static_num_workers, queue_name, queue_config, panda_queues_dict.get(queue_name, {}), job_stats_new_df, tmp_log
                )

            # define num of new workers
            dyn_num_workers = copy.deepcopy(static_num_workers)
            for queue_name, queue_config in queue_config_map.items():
                self._define_queue_new_workers(
                    queue_name,
                    queue_config,
                    panda_queues_dict.get(queue_name, {}),
                    static_num_workers[queue_name],
                    dyn_num_workers[queue_name],
                    queue_stat,
                    job_stats,
                    rt_mapper,
                    tmp_log,
                )

            # dump
            tmp_log.debug(f"defined {dyn_num_workers}")
            # print result in table
            self._format_result_dataframe(dyn_num_workers, tmp_log)
            return dyn_num_workers
        except Exception:
            # dump error
            core_utils.dump_error_message(tmp_log)
            return None
