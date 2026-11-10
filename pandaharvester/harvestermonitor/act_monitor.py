from act.atlas.aCTDBPanda import aCTDBPanda
from act.atlas.dbModels import PandaJob
from sqlalchemy import select

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
        tmpLog = core_utils.make_logger(baseLogger, method_name="check_workers")
        batchids = [w.batchID for w in workspec_list]

        try:
            tmpLog.debug(f"Querying aCT for ids {batchids}")
            with self.actDB.Session() as session:
                rows = session.execute(
                    select(PandaJob.id, PandaJob.actpandastatus, PandaJob.computingElement, PandaJob.node, PandaJob.error).where(PandaJob.id.in_(batchids))
                ).all()
        except Exception as e:
            if self.actDB:
                tmpLog.error(f"Failed to query aCT DB for ids {batchids}: {str(e)}")
            # send back current status for all workers
            return True, [(w.status, "") for w in workspec_list]

        actjobs = {str(r.id): r for r in rows}
        queueconfigmapper = QueueConfigMapper()

        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = core_utils.make_logger(baseLogger, f"workerID={workSpec.workerID}", method_name="check_workers")
            queueconfig = queueconfigmapper.get_queue(workSpec.computingSite)

            actjob = actjobs.get(workSpec.batchID)
            if not actjob:
                tmpLog.error(f"Job with id {workSpec.batchID} not found in aCT")
                # send back current status
                retList.append((WorkSpec.ST_failed, "Job not found in aCT"))
                continue

            actstatus = actjob.actpandastatus
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
                    errorMsg = actjob.error or "Unknown error"
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

            if actjob.computingElement:
                workSpec.computingElement = actjob.computingElement
            if actjob.node:
                try:
                    pandaid = workSpec.get_jobspec_list()[0].PandaID
                    workSpec.set_work_attributes({pandaid: {"node": actjob.node}})
                except BaseException:
                    tmpLog.warning(f"Could not extract panda ID for worker {workSpec.batchID}")

            retList.append((newStatus, errorMsg))

        return True, retList
