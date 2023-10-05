import datetime

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvestercore.pilot_errors import PilotErrors
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.file_spec import FileSpec

# logger
_logger = core_utils.setup_logger("preparator")


# class to prepare jobs
class Preparator(AgentBase):
    # constructor
    def __init__(self, communicator, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.communicator = communicator
        self.queueConfigMapper = queue_config_mapper
        self.pluginFactory = PluginFactory()

    # main loop

    def run(self):
        lockedBy = "preparator-{0}".format(self.get_pid())
        while True:
            sw = core_utils.get_stopwatch()
            mainLog = self.make_logger(_logger, "id={0}".format(lockedBy), method_name="run")
            mainLog.debug("try to get jobs to check")
            # get jobs to check preparation
            try:
                maxFilesPerJob = harvester_config.preparator.maxFilesPerJobToCheck
                if maxFilesPerJob <= 0:
                    maxFilesPerJob = None
            except Exception:
                maxFilesPerJob = None
            jobsToCheck = self.dbProxy.get_jobs_in_sub_status(
                "preparing",
                harvester_config.preparator.maxJobsToCheck,
                "preparatorTime",
                "lockedBy",
                harvester_config.preparator.checkInterval,
                harvester_config.preparator.lockInterval,
                lockedBy,
                max_files_per_job=maxFilesPerJob,
                ng_file_status_list=["ready"],
            )
            mainLog.debug("got {0} jobs to check".format(len(jobsToCheck)))
            # loop over all jobs
            for jobSpec in jobsToCheck:
                tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobSpec.PandaID), method_name="run")
                try:
                    tmpLog.debug("start checking")
                    # configID
                    configID = jobSpec.configID
                    if not core_utils.dynamic_plugin_change():
                        configID = None
                    # get queue
                    if not self.queueConfigMapper.has_queue(jobSpec.computingSite, configID):
                        tmpLog.error("queue config for {0}/{1} not found".format(jobSpec.computingSite, configID))
                        continue
                    queueConfig = self.queueConfigMapper.get_queue(jobSpec.computingSite, jobSpec.configID)
                    oldSubStatus = jobSpec.subStatus
                    # get plugin
                    if jobSpec.auxInput in [None, JobSpec.AUX_allTriggered]:
                        preparatorCore = self.pluginFactory.get_plugin(queueConfig.preparator)
                    else:
                        preparatorCore = self.pluginFactory.get_plugin(queueConfig.aux_preparator)
                    if preparatorCore is None:
                        # not found
                        tmpLog.error("plugin for {0} not found".format(jobSpec.computingSite))
                        continue
                    tmpLog.debug("plugin={0}".format(preparatorCore.__class__.__name__))
                    # lock job again
                    lockedAgain = self.dbProxy.lock_job_again(jobSpec.PandaID, "preparatorTime", "lockedBy", lockedBy)
                    if not lockedAgain:
                        tmpLog.debug("skip since locked by another thread")
                        continue
                    tmpStat, tmpStr = preparatorCore.check_stage_in_status(jobSpec)
                    # still running
                    if tmpStat is None:
                        # update job
                        jobSpec.lockedBy = None
                        self.dbProxy.update_job(jobSpec, {"lockedBy": lockedBy, "subStatus": oldSubStatus})
                        tmpLog.debug("try to check later since still preparing with {0}".format(tmpStr))
                        continue
                    # succeeded
                    if tmpStat is True:
                        # resolve path
                        tmpStat, tmpStr = preparatorCore.resolve_input_paths(jobSpec)
                        if tmpStat is False:
                            jobSpec.lockedBy = None
                            self.dbProxy.update_job(jobSpec, {"lockedBy": lockedBy, "subStatus": oldSubStatus})
                            tmpLog.error("failed to resolve input file paths : {0}".format(tmpStr))
                            continue
                        # manipulate container-related job params
                        jobSpec.manipulate_job_params_for_container()
                        # update job
                        jobSpec.lockedBy = None
                        jobSpec.set_all_input_ready()
                        if (maxFilesPerJob is None and jobSpec.auxInput is None) or (
                            len(jobSpec.inFiles) == 0 and jobSpec.auxInput in [None, JobSpec.AUX_inReady]
                        ):
                            # all done
                            allDone = True
                            jobSpec.subStatus = "prepared"
                            jobSpec.preparatorTime = None
                            if jobSpec.auxInput is not None:
                                jobSpec.auxInput = JobSpec.AUX_allReady
                        else:
                            # immediate next lookup since there could be more files to check
                            allDone = False
                            jobSpec.trigger_preparation()
                            # change auxInput flag to check auxiliary inputs
                            if len(jobSpec.inFiles) == 0 and jobSpec.auxInput == JobSpec.AUX_allTriggered:
                                jobSpec.auxInput = JobSpec.AUX_inReady
                        self.dbProxy.update_job(jobSpec, {"lockedBy": lockedBy, "subStatus": oldSubStatus}, update_in_file=True)
                        if allDone:
                            tmpLog.debug("succeeded")
                        else:
                            tmpLog.debug("partially succeeded")
                    else:
                        # update job
                        jobSpec.status = "failed"
                        jobSpec.subStatus = "failed_to_prepare"
                        jobSpec.lockedBy = None
                        jobSpec.preparatorTime = None
                        jobSpec.stateChangeTime = datetime.datetime.utcnow()
                        errStr = "stage-in failed with {0}".format(tmpStr)
                        jobSpec.set_pilot_error(PilotErrors.STAGEINFAILED, errStr)
                        jobSpec.trigger_propagation()
                        self.dbProxy.update_job(jobSpec, {"lockedBy": lockedBy, "subStatus": oldSubStatus})
                        tmpLog.error("failed with {0}".format(tmpStr))
                except Exception:
                    core_utils.dump_error_message(tmpLog)
            # get jobs to trigger preparation
            mainLog.debug("try to get jobs to prepare")
            try:
                maxFilesPerJob = harvester_config.preparator.maxFilesPerJobToPrepare
                if maxFilesPerJob <= 0:
                    maxFilesPerJob = None
            except Exception:
                maxFilesPerJob = None
            jobsToTrigger = self.dbProxy.get_jobs_in_sub_status(
                "fetched",
                harvester_config.preparator.maxJobsToTrigger,
                "preparatorTime",
                "lockedBy",
                harvester_config.preparator.triggerInterval,
                harvester_config.preparator.lockInterval,
                lockedBy,
                "preparing",
                max_files_per_job=maxFilesPerJob,
                ng_file_status_list=["triggered", "ready"],
            )
            mainLog.debug("got {0} jobs to prepare".format(len(jobsToTrigger)))
            # loop over all jobs
            fileStatMap = dict()
            for jobSpec in jobsToTrigger:
                tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobSpec.PandaID), method_name="run")
                try:
                    tmpLog.debug("try to trigger preparation")
                    # configID
                    configID = jobSpec.configID
                    if not core_utils.dynamic_plugin_change():
                        configID = None
                    # get queue
                    if not self.queueConfigMapper.has_queue(jobSpec.computingSite, configID):
                        tmpLog.error("queue config for {0}/{1} not found".format(jobSpec.computingSite, configID))
                        continue
                    queueConfig = self.queueConfigMapper.get_queue(jobSpec.computingSite, configID)
                    oldSubStatus = jobSpec.subStatus
                    # get plugin
                    if jobSpec.auxInput in [None, JobSpec.AUX_hasAuxInput]:
                        preparatorCore = self.pluginFactory.get_plugin(queueConfig.preparator)
                        fileType = "input"
                    else:
                        preparatorCore = self.pluginFactory.get_plugin(queueConfig.aux_preparator)
                        fileType = FileSpec.AUX_INPUT
                    if preparatorCore is None:
                        # not found
                        tmpLog.error("plugin for {0} not found".format(jobSpec.computingSite))
                        continue
                    tmpLog.debug("plugin={0}".format(preparatorCore.__class__.__name__))
                    # lock job again
                    lockedAgain = self.dbProxy.lock_job_again(jobSpec.PandaID, "preparatorTime", "lockedBy", lockedBy)
                    if not lockedAgain:
                        tmpLog.debug("skip since locked by another thread")
                        continue
                    # check file status
                    if queueConfig.ddmEndpointIn not in fileStatMap:
                        fileStatMap[queueConfig.ddmEndpointIn] = dict()
                    # check if has to_prepare
                    hasToPrepare = False
                    for fileSpec in jobSpec.inFiles:
                        if fileSpec.status == "to_prepare":
                            hasToPrepare = True
                            break
                    newFileStatusData = []
                    toWait = False
                    newInFiles = []
                    for fileSpec in jobSpec.inFiles:
                        if fileSpec.status in ["preparing", "to_prepare"]:
                            newInFiles.append(fileSpec)
                            updateStatus = False
                            if fileSpec.lfn not in fileStatMap[queueConfig.ddmEndpointIn]:
                                fileStatMap[queueConfig.ddmEndpointIn][fileSpec.lfn] = self.dbProxy.get_file_status(
                                    fileSpec.lfn, fileType, queueConfig.ddmEndpointIn, "starting"
                                )
                            if "ready" in fileStatMap[queueConfig.ddmEndpointIn][fileSpec.lfn]:
                                # the file is ready
                                fileSpec.status = "ready"
                                if fileStatMap[queueConfig.ddmEndpointIn][fileSpec.lfn]["ready"]["path"]:
                                    fileSpec.path = list(fileStatMap[queueConfig.ddmEndpointIn][fileSpec.lfn]["ready"]["path"])[0]
                                # set group info if any
                                groupInfo = self.dbProxy.get_group_for_file(fileSpec.lfn, fileType, queueConfig.ddmEndpointIn)
                                if groupInfo is not None:
                                    fileSpec.groupID = groupInfo["groupID"]
                                    fileSpec.groupStatus = groupInfo["groupStatus"]
                                    fileSpec.groupUpdateTime = groupInfo["groupUpdateTime"]
                                updateStatus = True
                            elif (not hasToPrepare and "to_prepare" in fileStatMap[queueConfig.ddmEndpointIn][fileSpec.lfn]) or "triggered" in fileStatMap[
                                queueConfig.ddmEndpointIn
                            ][fileSpec.lfn]:
                                # the file is being prepared by another
                                toWait = True
                                if fileSpec.status != "preparing":
                                    fileSpec.status = "preparing"
                                    updateStatus = True
                            else:
                                # change file status if the file is not prepared by another
                                if fileSpec.status != "to_prepare":
                                    fileSpec.status = "to_prepare"
                                    updateStatus = True
                            # set new status
                            if updateStatus:
                                newFileStatusData.append((fileSpec.fileID, fileSpec.lfn, fileSpec.status))
                                fileStatMap[queueConfig.ddmEndpointIn][fileSpec.lfn].setdefault(fileSpec.status, None)
                    if len(newFileStatusData) > 0:
                        self.dbProxy.change_file_status(jobSpec.PandaID, newFileStatusData, lockedBy)
                    # wait since files are being prepared by another
                    if toWait:
                        # update job
                        jobSpec.lockedBy = None
                        self.dbProxy.update_job(jobSpec, {"lockedBy": lockedBy, "subStatus": oldSubStatus})
                        tmpLog.debug("wait since files are being prepared by another job")
                        continue
                    # trigger preparation
                    tmpStat, tmpStr = preparatorCore.trigger_preparation(jobSpec)
                    # check result
                    if tmpStat is True:
                        # succeeded
                        jobSpec.lockedBy = None
                        if (maxFilesPerJob is None and jobSpec.auxInput is None) or (
                            len(jobSpec.inFiles) == 0 and jobSpec.auxInput in [None, JobSpec.AUX_inTriggered]
                        ):
                            # all done
                            allDone = True
                            jobSpec.subStatus = "preparing"
                            jobSpec.preparatorTime = None
                            if jobSpec.auxInput is not None:
                                jobSpec.auxInput = JobSpec.AUX_allTriggered
                        else:
                            # change file status but not change job sub status since
                            # there could be more files to prepare
                            allDone = False
                            for fileSpec in jobSpec.inFiles:
                                if fileSpec.status == "to_prepare":
                                    fileSpec.status = "triggered"
                            # immediate next lookup
                            jobSpec.trigger_preparation()
                            # change auxInput flag to prepare auxiliary inputs
                            if len(jobSpec.inFiles) == 0 and jobSpec.auxInput == JobSpec.AUX_hasAuxInput:
                                jobSpec.auxInput = JobSpec.AUX_inTriggered
                        self.dbProxy.update_job(jobSpec, {"lockedBy": lockedBy, "subStatus": oldSubStatus}, update_in_file=True)
                        if allDone:
                            tmpLog.debug("triggered")
                        else:
                            tmpLog.debug("partially triggered")
                    elif tmpStat is False:
                        # fatal error
                        jobSpec.status = "failed"
                        jobSpec.subStatus = "failed_to_prepare"
                        jobSpec.lockedBy = None
                        jobSpec.preparatorTime = None
                        jobSpec.stateChangeTime = datetime.datetime.utcnow()
                        errStr = "stage-in failed with {0}".format(tmpStr)
                        jobSpec.set_pilot_error(PilotErrors.STAGEINFAILED, errStr)
                        jobSpec.trigger_propagation()
                        self.dbProxy.update_job(jobSpec, {"lockedBy": lockedBy, "subStatus": oldSubStatus})
                        tmpLog.debug("failed to trigger with {0}".format(tmpStr))
                    else:
                        # temporary error
                        jobSpec.lockedBy = None
                        self.dbProxy.update_job(jobSpec, {"lockedBy": lockedBy, "subStatus": oldSubStatus})
                        tmpLog.debug("try to prepare later since {0}".format(tmpStr))
                except Exception:
                    core_utils.dump_error_message(tmpLog)
            mainLog.debug("done" + sw.get_elapsed_time())
            # check if being terminated
            if self.terminated(harvester_config.preparator.sleepTime):
                mainLog.debug("terminated")
                return
