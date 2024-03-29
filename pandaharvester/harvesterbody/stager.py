from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.pilot_errors import PilotErrors
from pandaharvester.harvestercore.plugin_factory import PluginFactory

# logger
_logger = core_utils.setup_logger("stager")


# class for stage-out
class Stager(AgentBase):
    # constructor
    def __init__(self, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.queueConfigMapper = queue_config_mapper
        self.pluginFactory = PluginFactory()

    # main loop
    def run(self):
        lockedBy = f"stager-{self.get_pid()}"
        while True:
            sw = core_utils.get_stopwatch()
            mainLog = self.make_logger(_logger, f"id={lockedBy}", method_name="run")
            mainLog.debug("try to get jobs to check")
            # get jobs to check preparation
            try:
                maxFilesPerJob = harvester_config.stager.maxFilesPerJobToCheck
            except Exception:
                maxFilesPerJob = None
            jobsToCheck = self.dbProxy.get_jobs_for_stage_out(
                harvester_config.stager.maxJobsToCheck,
                harvester_config.stager.checkInterval,
                harvester_config.stager.lockInterval,
                lockedBy,
                "transferring",
                JobSpec.HO_hasTransfer,
                max_files_per_job=maxFilesPerJob,
            )
            mainLog.debug(f"got {len(jobsToCheck)} jobs to check")
            # loop over all jobs
            for jobSpec in jobsToCheck:
                tmpLog = self.make_logger(_logger, f"PandaID={jobSpec.PandaID}", method_name="run")
                try:
                    tmpLog.debug("start checking")
                    # configID
                    configID = jobSpec.configID
                    if not core_utils.dynamic_plugin_change():
                        configID = None
                    # get queue
                    if not self.queueConfigMapper.has_queue(jobSpec.computingSite, configID):
                        tmpLog.error(f"queue config for {jobSpec.computingSite}/{configID} not found")
                        continue
                    queueConfig = self.queueConfigMapper.get_queue(jobSpec.computingSite, configID)
                    # get plugin
                    stagerCore = self.pluginFactory.get_plugin(queueConfig.stager)
                    if stagerCore is None:
                        # not found
                        tmpLog.error(f"plugin for {jobSpec.computingSite} not found")
                        continue
                    # lock job again
                    lockedAgain = self.dbProxy.lock_job_again(jobSpec.PandaID, "stagerTime", "stagerLock", lockedBy)
                    if not lockedAgain:
                        tmpLog.debug("skip since locked by another thread")
                        continue
                    tmpLog.debug(f"plugin={stagerCore.__class__.__name__}")
                    tmpStat, tmpStr = stagerCore.check_stage_out_status(jobSpec)
                    # check result
                    if tmpStat is True:
                        # succeeded
                        newSubStatus = self.dbProxy.update_job_for_stage_out(jobSpec, True, lockedBy)
                        tmpLog.debug(f"succeeded new subStatus={newSubStatus}")
                    elif tmpStat is False:
                        # fatal error
                        tmpLog.debug(f"fatal error when checking status with {tmpStr}")
                        # update job
                        for fileSpec in jobSpec.outFiles:
                            if fileSpec.status != "finished":
                                fileSpec.status = "failed"
                        errStr = f"stage-out failed with {tmpStr}"
                        jobSpec.set_pilot_error(PilotErrors.STAGEOUTFAILED, errStr)
                        jobSpec.trigger_propagation()
                        newSubStatus = self.dbProxy.update_job_for_stage_out(jobSpec, True, lockedBy)
                        tmpLog.debug(f"updated new subStatus={newSubStatus}")
                    else:
                        # on-going
                        tmpLog.debug(f"try to check later since {tmpStr}")
                except Exception:
                    core_utils.dump_error_message(tmpLog)
            # get jobs to trigger stage-out
            try:
                maxFilesPerJob = harvester_config.stager.maxFilesPerJobToTrigger
            except Exception:
                maxFilesPerJob = None
            jobsToTrigger = self.dbProxy.get_jobs_for_stage_out(
                harvester_config.stager.maxJobsToTrigger,
                harvester_config.stager.triggerInterval,
                harvester_config.stager.lockInterval,
                lockedBy,
                "to_transfer",
                JobSpec.HO_hasOutput,
                [JobSpec.HO_hasZipOutput, JobSpec.HO_hasPostZipOutput],
                max_files_per_job=maxFilesPerJob,
            )
            mainLog.debug(f"got {len(jobsToTrigger)} jobs to trigger")
            # loop over all jobs
            for jobSpec in jobsToTrigger:
                tmpLog = self.make_logger(_logger, f"PandaID={jobSpec.PandaID}", method_name="run")
                try:
                    tmpLog.debug("try to trigger stage-out")
                    # configID
                    configID = jobSpec.configID
                    if not core_utils.dynamic_plugin_change():
                        configID = None
                    # get queue
                    if not self.queueConfigMapper.has_queue(jobSpec.computingSite, configID):
                        tmpLog.error(f"queue config for {jobSpec.computingSite}/{configID} not found")
                        continue
                    queueConfig = self.queueConfigMapper.get_queue(jobSpec.computingSite, configID)
                    # get plugin
                    stagerCore = self.pluginFactory.get_plugin(queueConfig.stager)
                    if stagerCore is None:
                        # not found
                        tmpLog.error(f"plugin for {jobSpec.computingSite} not found")
                        continue
                    # lock job again
                    lockedAgain = self.dbProxy.lock_job_again(jobSpec.PandaID, "stagerTime", "stagerLock", lockedBy)
                    if not lockedAgain:
                        tmpLog.debug("skip since locked by another thread")
                        continue
                    # trigger stage-out
                    tmpLog.debug(f"plugin={stagerCore.__class__.__name__}")
                    tmpStat, tmpStr = stagerCore.trigger_stage_out(jobSpec)
                    # check result
                    if tmpStat is True:
                        # succeeded
                        jobSpec.trigger_stage_out()
                        jobSpec.all_files_triggered_to_stage_out()
                        newSubStatus = self.dbProxy.update_job_for_stage_out(jobSpec, True, lockedBy)
                        tmpLog.debug(f"triggered new subStatus={newSubStatus}")
                    elif tmpStat is False:
                        # fatal error
                        tmpLog.debug(f"fatal error to trigger with {tmpStr}")
                        # update job
                        for fileSpec in jobSpec.outFiles:
                            if fileSpec.status != "finished":
                                fileSpec.status = "failed"
                        errStr = f"stage-out failed with {tmpStr}"
                        jobSpec.set_pilot_error(PilotErrors.STAGEOUTFAILED, errStr)
                        jobSpec.trigger_propagation()
                        newSubStatus = self.dbProxy.update_job_for_stage_out(jobSpec, True, lockedBy)
                        tmpLog.debug(f"updated new subStatus={newSubStatus}")
                    else:
                        # temporary error
                        tmpLog.debug(f"try to trigger later since {tmpStr}")
                except Exception:
                    core_utils.dump_error_message(tmpLog)
            # get jobs to zip output
            if hasattr(harvester_config, "zipper"):
                pluginConf = harvester_config.zipper
            else:
                pluginConf = harvester_config.stager
            try:
                maxFilesPerJob = pluginConf.maxFilesPerJobToZip
            except Exception:
                maxFilesPerJob = None
            try:
                zipInterval = pluginConf.zipInterval
            except Exception:
                zipInterval = pluginConf.triggerInterval
            try:
                usePostZipping = pluginConf.usePostZipping
            except Exception:
                usePostZipping = False
            jobsToZip = self.dbProxy.get_jobs_for_stage_out(
                pluginConf.maxJobsToZip,
                zipInterval,
                pluginConf.lockInterval,
                lockedBy,
                "to_transfer",
                JobSpec.HO_hasZipOutput,
                [JobSpec.HO_hasOutput, JobSpec.HO_hasPostZipOutput],
                max_files_per_job=maxFilesPerJob,
            )
            mainLog.debug(f"got {len(jobsToZip)} jobs to zip")
            # loop over all jobs
            for jobSpec in jobsToZip:
                tmpLog = self.make_logger(_logger, f"PandaID={jobSpec.PandaID}", method_name="run")
                try:
                    tmpLog.debug("try to zip output")
                    # configID
                    configID = jobSpec.configID
                    if not core_utils.dynamic_plugin_change():
                        configID = None
                    # get queue
                    if not self.queueConfigMapper.has_queue(jobSpec.computingSite, configID):
                        tmpLog.error(f"queue config for {jobSpec.computingSite}/{configID} not found")
                        continue
                    queueConfig = self.queueConfigMapper.get_queue(jobSpec.computingSite, configID)
                    # get plugin
                    if hasattr(queueConfig, "zipper"):
                        zipperCore = self.pluginFactory.get_plugin(queueConfig.zipper)
                    else:
                        zipperCore = self.pluginFactory.get_plugin(queueConfig.stager)
                    if zipperCore is None:
                        # not found
                        tmpLog.error(f"plugin for {jobSpec.computingSite} not found")
                        continue
                    # lock job again
                    lockedAgain = self.dbProxy.lock_job_again(jobSpec.PandaID, "stagerTime", "stagerLock", lockedBy)
                    if not lockedAgain:
                        tmpLog.debug("skip since locked by another thread")
                        continue
                    # zipping
                    tmpLog.debug(f"plugin={zipperCore.__class__.__name__}")
                    if usePostZipping:
                        tmpStat, tmpStr = zipperCore.async_zip_output(jobSpec)
                    else:
                        tmpStat, tmpStr = zipperCore.zip_output(jobSpec)
                    # succeeded
                    if tmpStat is True:
                        # update job
                        jobSpec.trigger_stage_out()
                        jobSpec.all_files_zipped(usePostZipping)
                        newSubStatus = self.dbProxy.update_job_for_stage_out(jobSpec, False, lockedBy)
                        if usePostZipping:
                            tmpLog.debug(f"async zipped new subStatus={newSubStatus}")
                        else:
                            tmpLog.debug(f"zipped new subStatus={newSubStatus}")
                    elif tmpStat is None:
                        tmpLog.debug(f"try later since {tmpStr}")
                    else:
                        # failed
                        tmpLog.debug(f"fatal error to zip with {tmpStr}")
                        # update job
                        for fileSpec in jobSpec.outFiles:
                            if fileSpec.status == "zipping":
                                fileSpec.status = "failed"
                        errStr = f"zip-output failed with {tmpStr}"
                        jobSpec.set_pilot_error(PilotErrors.STAGEOUTFAILED, errStr)
                        jobSpec.trigger_propagation()
                        newSubStatus = self.dbProxy.update_job_for_stage_out(jobSpec, True, lockedBy)
                        tmpLog.debug(f"updated new subStatus={newSubStatus}")
                except Exception:
                    core_utils.dump_error_message(tmpLog)
            if usePostZipping:
                jobsToPostZip = self.dbProxy.get_jobs_for_stage_out(
                    pluginConf.maxJobsToZip,
                    zipInterval,
                    pluginConf.lockInterval,
                    lockedBy,
                    "to_transfer",
                    JobSpec.HO_hasPostZipOutput,
                    [JobSpec.HO_hasOutput, JobSpec.HO_hasZipOutput],
                    max_files_per_job=maxFilesPerJob,
                )
                mainLog.debug(f"got {len(jobsToPostZip)} jobs to post-zip")
                # loop over all jobs
                for jobSpec in jobsToPostZip:
                    tmpLog = self.make_logger(_logger, f"PandaID={jobSpec.PandaID}", method_name="run")
                    try:
                        tmpLog.debug("try to post-zip output")
                        # configID
                        configID = jobSpec.configID
                        if not core_utils.dynamic_plugin_change():
                            configID = None
                        # get queue
                        if not self.queueConfigMapper.has_queue(jobSpec.computingSite, configID):
                            tmpLog.error(f"queue config for {jobSpec.computingSite}/{configID} not found")
                            continue
                        queueConfig = self.queueConfigMapper.get_queue(jobSpec.computingSite, configID)
                        # get plugin
                        if hasattr(queueConfig, "zipper"):
                            zipperCore = self.pluginFactory.get_plugin(queueConfig.zipper)
                        else:
                            zipperCore = self.pluginFactory.get_plugin(queueConfig.stager)
                        if zipperCore is None:
                            # not found
                            tmpLog.error(f"plugin for {jobSpec.computingSite} not found")
                            continue
                        # lock job again
                        lockedAgain = self.dbProxy.lock_job_again(jobSpec.PandaID, "stagerTime", "stagerLock", lockedBy)
                        if not lockedAgain:
                            tmpLog.debug("skip since locked by another thread")
                            continue
                        # post-zipping
                        tmpLog.debug(f"plugin={zipperCore.__class__.__name__}")
                        tmpStat, tmpStr = zipperCore.post_zip_output(jobSpec)
                        # succeeded
                        if tmpStat is True:
                            # update job
                            jobSpec.trigger_stage_out()
                            jobSpec.all_files_zipped()
                            newSubStatus = self.dbProxy.update_job_for_stage_out(jobSpec, False, lockedBy)
                            tmpLog.debug(f"post-zipped new subStatus={newSubStatus}")
                        elif tmpStat is None:
                            # pending
                            tmpLog.debug(f"try to post-zip later since {tmpStr}")
                        else:
                            # fatal error
                            tmpLog.debug(f"fatal error to post-zip since {tmpStr}")
                            # update job
                            for fileSpec in jobSpec.outFiles:
                                if fileSpec.status == "post_zipping":
                                    fileSpec.status = "failed"
                            errStr = f"post-zipping failed with {tmpStr}"
                            jobSpec.set_pilot_error(PilotErrors.STAGEOUTFAILED, errStr)
                            jobSpec.trigger_propagation()
                            newSubStatus = self.dbProxy.update_job_for_stage_out(jobSpec, True, lockedBy)
                            tmpLog.debug(f"updated new subStatus={newSubStatus}")
                    except Exception:
                        core_utils.dump_error_message(tmpLog)

            mainLog.debug("done" + sw.get_elapsed_time())
            # check if being terminated
            if self.terminated(harvester_config.stager.sleepTime):
                mainLog.debug("terminated")
                return
