import copy
import math
import traceback

import polars as pl

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory
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
            is_unified_queue = queue_dict.get("capability", "") == "ucore"
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
                # tmp_log.debug("queue_config.submitter:%s" % str(queue_config.submitter))
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

    def _build_new_workers_df(self, static_num_workers, queue_name, tmp_log):
        """Build and filter workers dataframe for a given queue."""
        return (
            self._num_workers_dict_to_df(static_num_workers)
            .filter(pl.col("queue_name") == queue_name)
            .filter(pl.col("resource_type").is_not_null())
            .filter(pl.col("pilot_type").is_not_null())
            .with_columns(
                [
                    pl.col("queue_name").fill_null(pl.lit(queue_name)),
                    pl.col("nQueue").fill_null(0),
                    pl.col("nReady").fill_null(0),
                    pl.col("nRunning").fill_null(0),
                    pl.col("nNewWorkers").fill_null(0),
                ]
            )
        )

    def _build_activated_df(self, job_stats_new_df, queue_name, tmp_log):
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

    def _join_workers_activated_dfs(self, activated_df, tmp_new_workers_df, queue_name, tmp_log):
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

    def _build_master_df(self, joined_df, queue_name, static_num_workers, tmp_log):
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

    def _sync_master_df_to_static_workers(self, tmp_master_df, queue_name, tmp_static_num_workers, tmp_log):
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
        self, tmp_master_df, tmp_static_num_workers, static_num_workers, queue_name, master_df, queue_dict, queue_config, prioritized_pilot_types, tmp_log
    ):
        """Set initial nNewWorkers for pilot types based on activated jobs and activation factor."""
        for job_type in tmp_static_num_workers[queue_name]:
            for resource_type, pilot_type_dict in tmp_static_num_workers[queue_name][job_type].items():
                total_n_new_workers = pilot_type_dict["ANY"]["nNewWorkers"]
                if total_n_new_workers <= 0:
                    continue
                # calculate the total number of new workers needed for prioritized pilot types
                remaining_n_new_workers = total_n_new_workers
                activate_worker_factor = self.get_activate_worker_factor(queue_name, job_type, resource_type, queue_dict, queue_config)
                prio_ptype_result = tmp_master_df.filter(
                    (pl.col("queue_name") == queue_name)
                    & (pl.col("job_type") == job_type)
                    & (pl.col("resource_type") == resource_type)
                    & (pl.col("pilot_type").is_in(prioritized_pilot_types))
                ).select([pl.col("n_activated_jobs").sum(), pl.col("nQueue").sum()])
                if prio_ptype_result.shape[0] > 0:
                    total_prio_ptype_n_activated_jobs, total_prio_ptype_nQueue = prio_ptype_result.row(0)
                else:
                    total_prio_ptype_n_activated_jobs, total_prio_ptype_nQueue = 0, 0
                total_prio_ptype_calculated_n_new_workers = max(int(total_prio_ptype_n_activated_jobs * activate_worker_factor) - total_prio_ptype_nQueue, 0)
                if total_prio_ptype_calculated_n_new_workers > 0:
                    adjust_ratio = min(total_n_new_workers / total_prio_ptype_calculated_n_new_workers, 1)
                    for pilot_type, tmp_val in pilot_type_dict.items():
                        if pilot_type in prioritized_pilot_types:
                            pt_result = tmp_master_df.filter(
                                (pl.col("queue_name") == queue_name)
                                & (pl.col("job_type") == job_type)
                                & (pl.col("resource_type") == resource_type)
                                & (pl.col("pilot_type") == pilot_type)
                            ).select([pl.col("n_activated_jobs"), pl.col("nQueue")])
                            if pt_result.shape[0] > 0:
                                n_activated_jobs, nQueue = pt_result.row(0)
                            else:
                                n_activated_jobs, nQueue = 0, 0
                            calculated_n_new_workers = int(max(int(n_activated_jobs * activate_worker_factor) - nQueue, 0) * adjust_ratio)
                            if calculated_n_new_workers <= 0:
                                continue
                            tmp_static_num_workers[queue_name][job_type][resource_type][pilot_type]["nNewWorkers"] = calculated_n_new_workers
                            static_num_workers[queue_name].setdefault(job_type, {}).setdefault(resource_type, {}).setdefault(
                                pilot_type, {"nReady": 0, "nRunning": 0, "nQueue": 0, "nNewWorkers": 0}
                            )["nNewWorkers"] = calculated_n_new_workers
                            remaining_n_new_workers -= calculated_n_new_workers
                            master_df = master_df.with_columns(
                                pl.when(
                                    (pl.col("queue_name") == queue_name)
                                    & (pl.col("job_type") == job_type)
                                    & (pl.col("resource_type") == resource_type)
                                    & (pl.col("pilot_type") == pilot_type)
                                )
                                .then(pl.lit(calculated_n_new_workers))
                                .otherwise(pl.col("nNewWorkers"))
                                .alias("nNewWorkers")
                            )
                            tmp_log.debug(
                                f"Set initial nNewWorkers to {calculated_n_new_workers} for queue={queue_name} job_type={job_type} resource_type={resource_type} pilot_type={pilot_type}"
                            )
                if remaining_n_new_workers > 0:
                    # allocate remaining n_new_workers to DEFAULT_PILOT_TYPE PR
                    tmp_static_num_workers[queue_name][job_type][resource_type][DEFAULT_PILOT_TYPE]["nNewWorkers"] += remaining_n_new_workers
                    static_num_workers[queue_name].setdefault(job_type, {}).setdefault(resource_type, {}).setdefault(
                        DEFAULT_PILOT_TYPE, {"nReady": 0, "nRunning": 0, "nQueue": 0, "nNewWorkers": 0}
                    )["nNewWorkers"] = tmp_static_num_workers[queue_name][job_type][resource_type][DEFAULT_PILOT_TYPE]["nNewWorkers"]
                    tmp_log.debug(
                        f"Set remaining nNewWorkers to {remaining_n_new_workers} for queue={queue_name} job_type={job_type} resource_type={resource_type} pilot_type={DEFAULT_PILOT_TYPE}"
                    )
                    master_df = master_df.with_columns(
                        pl.when(
                            (pl.col("queue_name") == queue_name)
                            & (pl.col("job_type") == job_type)
                            & (pl.col("resource_type") == resource_type)
                            & (pl.col("pilot_type") == DEFAULT_PILOT_TYPE)
                        )
                        .then(pl.lit(remaining_n_new_workers))
                        .otherwise(pl.col("nNewWorkers"))
                        .alias("nNewWorkers")
                    )
        return master_df

    def _format_result_dataframe(self, dyn_num_workers, queue_name, tmp_log):
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

        def _normalize_job_type_any(queue_dict):
            tmp_log.debug(f"normalize_job_type_any got: {queue_dict}")
            if DEFAULT_JOB_TYPE in queue_dict and len(queue_dict) == 1:
                tmp_log.debug(f"normalize_job_type_any returned: {queue_dict}")
                return
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

        static_num_workers = copy.deepcopy(static_num_workers)
        for queue_name, queue_dict in static_num_workers.items():
            _normalize_job_type_any(queue_dict)

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

            job_stats_new_df = self._job_stats_to_df(None)
            job_stats_new = self.dbProxy.get_cache("job_statistics_new.json", None)
            if job_stats_new is not None:
                job_stats_new_df = self._job_stats_to_df(job_stats_new.data)

            # get panda queues dict from CRIC
            panda_queues_dict = PandaQueuesDict()

            # get resource type mapper
            rt_mapper = ResourceTypeMapper()

            for queue_name in static_num_workers:
                queue_dict = panda_queues_dict.get(queue_name, {})
                queue_config = self.queue_configMapper.get_queue(queue_name)

                # prioritized prod_source_labels for pilot submission
                prioritized_pslabels = DEFAULT_PRIORITIZED_PROD_SOURCE_LABELS
                if queue_config:
                    prioritized_pslabels = getattr(queue_config, "prioritizedProdSourceLabels", DEFAULT_PRIORITIZED_PROD_SOURCE_LABELS)
                else:
                    tmp_log.warning(f"missing queue_config for queue: {queue_name}")

                prioritized_pilot_types = [core_utils.prod_source_label_to_pilot_type(label) for label in prioritized_pslabels]

                tmp_new_workers_df = self._build_new_workers_df(static_num_workers, queue_name, tmp_log)
                activated_df = self._build_activated_df(job_stats_new_df, queue_name, tmp_log)
                joined_df = self._join_workers_activated_dfs(activated_df, tmp_new_workers_df, queue_name, tmp_log)

                tmp_master_df = self._build_master_df(joined_df, queue_name, static_num_workers, tmp_log)
                master_df = tmp_master_df.clone()
                tmp_static_num_workers = copy.deepcopy(static_num_workers)

                # update tmp_static_num_workers with tmp_master_df
                self._sync_master_df_to_static_workers(tmp_master_df, queue_name, tmp_static_num_workers, tmp_log)

                # set initial nNewWorkers for pilot types based on number of activated jobs and the activate worker factor
                master_df = self._set_initial_new_workers(
                    tmp_master_df, tmp_static_num_workers, static_num_workers, queue_name, master_df, queue_dict, queue_config, prioritized_pilot_types, tmp_log
                )
                display_master_df = master_df.select(
                    ["job_type", "resource_type", "pilot_type", "nQueue", "nReady", "nRunning", "nNewWorkers", "n_activated_jobs"]
                )
                tmp_log.debug(f"master_df: \n{display_master_df}")
                # remove pilot type ANY
                for job_type in static_num_workers[queue_name]:
                    for resource_type, pilot_type_dict in static_num_workers[queue_name][job_type].items():
                        if "ANY" in pilot_type_dict:
                            del pilot_type_dict["ANY"]

            dyn_num_workers = copy.deepcopy(static_num_workers)
            for queue_name in dyn_num_workers:
                for job_type in dyn_num_workers[queue_name]:
                    for resource_type, pilot_type_dict in dyn_num_workers[queue_name][job_type].items():
                        if "ANY" in pilot_type_dict:
                            del pilot_type_dict["ANY"]

            # define num of new workers
            for queue_name in static_num_workers:
                # get queue
                queue_config = self.queue_configMapper.get_queue(queue_name)
                worker_limits_dict = {}
                worker_stats_map = {}
                worker_stats_map.setdefault("queue", {"n": 0, "core": 0, "mem": 0})
                if queue_config:
                    worker_limits_dict, worker_stats_map = self.dbProxy.get_worker_limits(queue_name, queue_config)
                else:
                    tmp_log.warning(f"missing queue_config for queue: {queue_name}")
                # prioritized prod_source_labels for pilot submission
                prioritized_pslabels = getattr(queue_config, "prioritizedProdSourceLabels", DEFAULT_PRIORITIZED_PROD_SOURCE_LABELS)
                prioritized_pilot_types = [core_utils.prod_source_label_to_pilot_type(label) for label in prioritized_pslabels]
                # get limits from queue config
                max_workers = worker_limits_dict.get("maxWorkers", 0)
                n_queue_limit = worker_limits_dict.get("nQueueLimitWorker", 0)
                n_queue_limit_per_rt = n_queue_limit
                queue_limit_cores = worker_limits_dict.get("nQueueWorkerCores")
                queue_limit_memory = worker_limits_dict.get("nQueueWorkerMemory")
                cores_queue = worker_stats_map["queue"]["core"]
                memory_queue = worker_stats_map["queue"]["mem"]
                n_queue_total, n_ready_total, n_running_total = 0, 0, 0
                apf_msg = None
                apf_data = None
                job_type = DEFAULT_JOB_TYPE
                # loop over resource types and pilot types to define nNewWorkers
                for resource_type, pilot_type_dict in static_num_workers[queue_name][job_type].items():
                    for pilot_type, tmp_val in pilot_type_dict.items():
                        tmp_log.debug(
                            f"Processing queue={queue_name} job_type={job_type} resource_type={resource_type} pilot_type={pilot_type} with static_num_workers={tmp_val}"
                        )

                        # get cores and memory request per worker of this resource_type
                        queue_dict = panda_queues_dict.get(queue_name, {})
                        rtype_request_cores, rtype_request_memory = rt_mapper.calculate_worker_requirements(resource_type, queue_dict)

                        # set 0 to num of new workers when the queue is disabled
                        if queue_name in queue_stat and queue_stat[queue_name]["status"] in ["offline", "standby", "maintenance"]:
                            dyn_num_workers[queue_name][job_type][resource_type][pilot_type]["nNewWorkers"] = 0
                            ret_msg = f"set n_new_workers=0 since status={queue_stat[queue_name]['status']}"
                            tmp_log.debug(ret_msg)
                            apf_msg = f"Not submitting workers since queue status = {queue_stat[queue_name]['status']}"
                            continue

                        # protection against not-up-to-date queue config
                        if queue_config is None:
                            dyn_num_workers[queue_name][job_type][resource_type][pilot_type]["nNewWorkers"] = 0
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
                                dyn_num_workers[queue_name][job_type][resource_type][pilot_type]["nNewWorkers"] = 0
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
                                dyn_num_workers[queue_name][job_type][resource_type][pilot_type]["nNewWorkers"] = 0
                                ret_msg = "set n_new_workers=0 by panda in slave mode"
                                tmp_log.debug(ret_msg)
                                continue
                        else:
                            n_new_workers_def = None
                            if pilot_type != DEFAULT_PILOT_TYPE:
                                n_new_workers_def = tmp_val["nNewWorkers"]
                                if n_new_workers_def == 0:
                                    dyn_num_workers[queue_name][job_type][resource_type][pilot_type]["nNewWorkers"] = 0
                                    ret_msg = f"got n_new_workers=0 for non-{DEFAULT_PILOT_TYPE} pilot_type in self mode; skipped"
                                    tmp_log.debug(ret_msg)
                                    continue

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

                                        tmp_n_activated_jobs = job_stats[queue_name]["activated"]
                                        tmp_log.debug(f"available activated panda jobs {tmp_n_activated_jobs}")

                                        activate_worker_factor = self.get_activate_worker_factor(queue_name, job_type, resource_type, queue_dict, queue_config)
                                        if tmp_n_activated_jobs * activate_worker_factor > 0:
                                            n_min_pilots = 1
                                        n_activated = max(int(tmp_n_activated_jobs * activate_worker_factor), n_min_pilots)  # avoid no activity queues
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
                            dyn_num_workers[queue_name][job_type][resource_type][pilot_type]["nNewWorkers"] = n_new_workers

                # adjust n_new_workers for UCORE to let aggregations over rtype respect nQueueLimitWorker and max_workers
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
                                for _pt in dyn_num_workers[queue_name][_jt][_rt]:
                                    total_new_workers_rts = total_new_workers_rts + dyn_num_workers[queue_name][_jt][_rt][_pt]["nNewWorkers"]

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
                                    for pilot_type in dyn_num_workers[queue_name][job_type][resource_type]:
                                        dyn_num_workers[queue_name][job_type][resource_type][pilot_type]["nNewWorkers"] = 0
                            tmp_log.debug("No n_new_workers since n_new_workers_max_agg=0 for UCORE")
                        else:
                            tmp_log.debug(f"n_new_workers_max_agg={n_new_workers_max_agg} for UCORE")
                            _d = dyn_num_workers[queue_name].copy()
                            del _d["ANY"]

                            # TODO: needs to be recalculated
                            simple_rt_nw_list = []
                            for job_type in _d:  # jt: job type
                                for resource_type in _d[job_type]:  # rt: resource type
                                    for pilot_type in _d[job_type][resource_type]:  # pt: pilot type
                                        simple_rt_nw_list.append(
                                            [(resource_type, job_type, pilot_type), _d[job_type][resource_type][pilot_type].get("nNewWorkers", 0), 0]
                                        )

                            _countdown = n_new_workers_max_agg
                            for _rt_list in simple_rt_nw_list:
                                (resource_type, job_type, pilot_type), n_new_workers_orig, _r = _rt_list
                                n_new_workers, remainder = divmod(n_new_workers_orig * n_new_workers_max_agg, total_new_workers_rts)
                                dyn_num_workers[queue_name][job_type].setdefault(resource_type, {})
                                dyn_num_workers[queue_name][job_type][resource_type].setdefault(
                                    pilot_type, {"nReady": 0, "nRunning": 0, "nQueue": 0, "nNewWorkers": 0}
                                )
                                dyn_num_workers[queue_name][job_type][resource_type][pilot_type]["nNewWorkers"] = n_new_workers
                                _rt_list[2] = remainder
                                _countdown -= n_new_workers
                            # sort by whether n_new_workers_orig > 0 (favor positive over 0), then by pilot_type (favor prioritized), then by remainder (descending), then by original n_new_workers (favor smaller ones)
                            sorted_rt_nw_list = sorted(simple_rt_nw_list, key=(lambda x: (not (x[1] > 0), x[0][2] not in prioritized_pilot_types, -x[2], x[1])))
                            for (resource_type, job_type, pilot_type), n_new_workers_orig, remainder in sorted_rt_nw_list:
                                if _countdown <= 0:
                                    break
                                dyn_num_workers[queue_name][job_type][resource_type][pilot_type]["nNewWorkers"] += 1
                                _countdown -= 1
                        for job_type in dyn_num_workers[queue_name]:
                            for resource_type in dyn_num_workers[queue_name][job_type]:
                                if job_type == "ANY" or resource_type == "ANY":
                                    continue
                                for pilot_type in dyn_num_workers[queue_name][job_type][resource_type]:
                                    n_new_workers = dyn_num_workers[queue_name][job_type][resource_type][pilot_type]["nNewWorkers"]
                                    tmp_log.debug(
                                        f"setting n_new_workers to {n_new_workers} of job_type={job_type} resource_type={resource_type} pilot_type={pilot_type} in order to respect rtype aggregations for UCORE"
                                    )

                if not apf_msg:
                    apf_data = copy.deepcopy(dyn_num_workers[queue_name])

                self.apf_mon.update_label(queue_name, apf_msg, apf_data)

            # dump
            tmp_log.debug(f"defined {str(dyn_num_workers)}")
            # print result in table
            self._format_result_dataframe(dyn_num_workers, queue_name, tmp_log)
            return dyn_num_workers
        except Exception:
            # dump error
            err_msg = core_utils.dump_error_message(tmp_log)
            tmp_log.error(err_msg)
            return None
