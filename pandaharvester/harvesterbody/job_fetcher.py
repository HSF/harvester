import random
import socket

from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict

# logger
_logger = core_utils.setup_logger("job_fetcher")

# file statuses meaning that another job already takes care of staging in the file
STAGE_IN_TAKEN_STATUSES = ("ready", "preparing", "to_prepare", "triggered")


# class to fetch jobs
class JobFetcher(AgentBase):
    # constructor
    def __init__(self, communicator, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.communicator = communicator
        self.nodeName = socket.gethostname()
        self.queueConfigMapper = queue_config_mapper
        self.pluginFactory = PluginFactory()

    # main loop
    def run(self):
        while True:
            main_log = self.make_logger(_logger, f"id={self.get_pid()}", method_name="run")
            try:
                self.execute(main_log)
            except Exception:
                core_utils.dump_error_message(main_log)
            # check if being terminated
            if self.terminated(harvester_config.jobfetcher.sleepTime):
                main_log.debug("terminated")
                return

    # fetch jobs for queues which need them
    def execute(self, main_log):
        main_log.debug("getting number of jobs to be fetched")
        job_limit_to_fetch_dict = self.dbProxy.get_num_jobs_to_fetch(
            harvester_config.jobfetcher.nQueues, harvester_config.jobfetcher.lookupTime, self.queueConfigMapper
        )
        main_log.debug(f"got {len(job_limit_to_fetch_dict)} queues")
        # get up to date queue configuration
        panda_queue_dict = PandaQueuesDict()
        # loop over all queues; a failure in one queue does not skip the others
        for queue_name, value_dict in job_limit_to_fetch_dict.items():
            if not self.queueConfigMapper.has_queue(queue_name):
                continue
            tmp_log = self.make_logger(_logger, f"queueName={queue_name}", method_name="fetch_jobs")
            try:
                self.fetch_jobs(queue_name, value_dict["jobs"], panda_queue_dict, tmp_log)
            except Exception:
                core_utils.dump_error_message(tmp_log)
        main_log.debug("done")

    # fetch jobs for a queue
    def fetch_jobs(self, queue_name, n_jobs, panda_queue_dict, tmp_log):
        queue_config = self.queueConfigMapper.get_queue(queue_name)
        site_name = queue_config.siteName
        # upper limit
        n_jobs = min(n_jobs, harvester_config.jobfetcher.maxJobs)
        if n_jobs <= 0:
            tmp_log.debug("no job to fetch; skip")
            return
        # prod_source_label
        try:
            is_grandly_unified_queue = panda_queue_dict.is_grandly_unified_queue(site_name)
        except Exception:
            is_grandly_unified_queue = False
        default_prod_source_label = queue_config.get_source_label(is_gu=is_grandly_unified_queue)
        # randomize prod_source_label if configured
        pdpm = getattr(queue_config, "prodSourceLabelRandomWeightsPermille", {})
        prod_source_label = random.choice(core_utils.make_choice_list(pdpm=pdpm, default=default_prod_source_label))
        # get jobs with custom criteria from queue config
        tmp_log.debug(f"getting {n_jobs} jobs for prodSourceLabel={prod_source_label}")
        sw = core_utils.get_stopwatch()
        jobs, err_str = self.communicator.get_jobs(site_name, self.nodeName, prod_source_label, self.nodeName, n_jobs, queue_config.getJobCriteria)
        tmp_log.info(f"got {len(jobs)} jobs for prodSourceLabel={prod_source_label} with {err_str}" + sw.get_elapsed_time())
        if not jobs:
            return
        # convert to JobSpecs; a job failing to convert does not stop the others
        sw.reset()
        extractor_core = self.pluginFactory.get_plugin(queue_config.extractor) if hasattr(queue_config, "extractor") else None
        file_stat_map = {}
        job_specs = []
        for job in jobs:
            try:
                job_specs.append(self.make_job_spec(job, queue_name, queue_config, extractor_core, file_stat_map))
            except Exception:
                tmp_log.error(f"failed to convert PandaID={job.get('PandaID') if isinstance(job, dict) else None}")
                core_utils.dump_error_message(tmp_log)
        tmp_log.debug(f"converted {len(job_specs)}/{len(jobs)} jobs" + sw.get_elapsed_time())
        # insert to DB
        sw.reset()
        if self.dbProxy.insert_jobs(job_specs):
            tmp_log.debug(f"inserted {len(job_specs)} jobs" + sw.get_elapsed_time())
        else:
            tmp_log.error(f"failed to insert {len(job_specs)} jobs with PandaIDs={[job_spec.PandaID for job_spec in job_specs]}")

    # make a JobSpec with input FileSpecs from job json
    def make_job_spec(self, job, queue_name, queue_config, extractor_core, file_stat_map):
        time_now = core_utils.naive_utcnow()
        job_spec = JobSpec()
        job_spec.convert_job_json(job)
        job_spec.computingSite = queue_name
        job_spec.status = "starting"
        job_spec.subStatus = "fetched"
        job_spec.creationTime = time_now
        job_spec.stateChangeTime = time_now
        job_spec.configID = queue_config.configID
        job_spec.set_one_attribute("schedulerID", f"harvester-{harvester_config.master.harvester_id}")
        if queue_config.zipPerMB is not None and job_spec.zipPerMB is None:
            job_spec.zipPerMB = queue_config.zipPerMB
        file_group_dict_list = [job_spec.get_input_file_attributes()]
        if extractor_core is not None:
            file_group_dict_list.append(extractor_core.get_aux_inputs(job_spec))
        for file_group_dict in file_group_dict_list:
            for lfn, file_attrs in file_group_dict.items():
                # make file spec
                file_spec = FileSpec()
                file_spec.PandaID = job_spec.PandaID
                file_spec.taskID = job_spec.taskID
                file_spec.lfn = lfn
                file_spec.endpoint = queue_config.ddmEndpointIn
                file_spec.scope = file_attrs["scope"]
                if "INTERNAL_FileType" in file_attrs:
                    file_spec.fileType = file_attrs["INTERNAL_FileType"]
                    job_spec.auxInput = JobSpec.AUX_hasAuxInput
                else:
                    file_spec.fileType = "input"
                # check file status
                if lfn not in file_stat_map:
                    file_stat_map[lfn] = self.dbProxy.get_file_status(lfn, file_spec.fileType, queue_config.ddmEndpointIn, "starting")
                # set preparing to skip stage-in if the file is (being) taken care of by another job
                if any(status in file_stat_map[lfn] for status in STAGE_IN_TAKEN_STATUSES):
                    file_spec.status = "preparing"
                else:
                    file_spec.status = "to_prepare"
                file_stat_map[lfn].setdefault(file_spec.status, None)
                if "INTERNAL_URL" in file_attrs:
                    file_spec.url = file_attrs["INTERNAL_URL"]
                job_spec.add_in_file(file_spec)
        job_spec.trigger_propagation()
        return job_spec
