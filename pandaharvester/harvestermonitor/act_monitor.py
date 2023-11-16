from act.atlas.aCTDBPanda import aCTDBPanda
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.worker_errors import WorkerErrors

# json for job report
jsonJobReport = harvester_config.payload_interaction.jobReportFile

# logger
baseLogger = core_utils.setup_logger("act_monitor")


# monitor for aCT plugin
class ACTMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        # Set up aCT DB connection
        self.log = core_utils.make_logger(baseLogger, "aCT submitter", method_name="__init__")
        try:
            self.actDB = aCTDBPanda(self.log)
        except Exception as e:
            self.log.error(f"Could not connect to aCT database: {str(e)}")
            self.actDB = None

    # check workers
    def check_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = core_utils.make_logger(baseLogger, f"workerID={workSpec.workerID}", method_name="check_workers")

            queueconfigmapper = QueueConfigMapper()
            queueconfig = queueconfigmapper.get_queue(workSpec.computingSite)
            try:
                tmpLog.debug(f"Querying aCT for id {workSpec.batchID}")
                columns = ["actpandastatus", "pandastatus", "computingElement", "node", "error"]
                actjobs = self.actDB.getJobs(f"id={workSpec.batchID}", columns)
            except Exception as e:
                if self.actDB:
                    tmpLog.error(f"Failed to query aCT DB: {str(e)}")
                # send back current status
                retList.append((workSpec.status, ""))
                continue

            if not actjobs:
                tmpLog.error(f"Job with id {workSpec.batchID} not found in aCT")
                # send back current status
                retList.append((WorkSpec.ST_failed, "Job not found in aCT"))
                continue

            actstatus = actjobs[0]["actpandastatus"]
            workSpec.nativeStatus = actstatus
            newStatus = WorkSpec.ST_running
            errorMsg = ""
            if actstatus in ["waiting", "sent", "starting"]:
                newStatus = WorkSpec.ST_submitted

            # Handle post running states
            if queueconfig.truePilot:
                # True pilot: keep in running until really done
                if actstatus in ["done", "donecancelled"]:
                    newStatus = WorkSpec.ST_finished
                elif actstatus == "donefailed":
                    # set failed here with workspec sup error
                    errorMsg = actjobs[0]["error"] or "Unknown error"
                    error_code = WorkerErrors.error_codes.get("GENERAL_ERROR")
                    workSpec.set_supplemental_error(error_code=error_code, error_diag=errorMsg)
                    newStatus = WorkSpec.ST_failed
                    tmpLog.info(f"ID {workSpec.batchID} failed with error {errorMsg})")
            elif actstatus in ["done", "donefailed", "donecancelled", "transferring", "tovalidate"]:
                # NG mode: all post processing is now done in the stager
                newStatus = WorkSpec.ST_finished

            if newStatus != workSpec.status:
                tmpLog.info(f"ID {workSpec.batchID} updated status {workSpec.status} -> {newStatus} ({actstatus})")
            else:
                tmpLog.debug(f"batchStatus {actstatus} -> workerStatus {newStatus}")

            if actjobs[0]["computingElement"]:
                workSpec.computingElement = actjobs[0]["computingElement"]
            if actjobs[0]["node"]:
                try:
                    pandaid = workSpec.get_jobspec_list()[0].PandaID
                    workSpec.set_work_attributes({pandaid: {"node": actjobs[0]["node"]}})
                except BaseException:
                    tmpLog.warning(f"Could not extract panda ID for worker {workSpec.batchID}")

            retList.append((newStatus, errorMsg))

        return True, retList
