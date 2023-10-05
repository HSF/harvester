import json
import os

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.worker_errors import WorkerErrors
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvesterconfig import harvester_config
from .base_stager import BaseStager

from act.atlas.aCTDBPanda import aCTDBPanda

# logger
baseLogger = core_utils.setup_logger("act_stager")

# json for job report
jsonJobReport = harvester_config.payload_interaction.jobReportFile

# aCT stager plugin


class ACTStager(BaseStager):
    # constructor
    def __init__(self, **kwarg):
        BaseStager.__init__(self, **kwarg)

        # Set up aCT DB connection
        self.log = core_utils.make_logger(baseLogger, "aCT stager", method_name="__init__")
        try:
            self.actDB = aCTDBPanda(self.log)
        except Exception as e:
            self.log.error("Could not connect to aCT database: {0}".format(str(e)))
            self.actDB = None

    # check status
    def check_stage_out_status(self, jobspec):
        """Check the status of stage-out procedure.
        Checks aCT job status and sets output file status to finished or failed
        once aCT jobs is done. All error handling and post-processing needs to
        be done here.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True: transfer success, False: fatal transfer failure,
                 None: on-going or temporary failure) and error dialog
        :rtype: (bool, string)
        """

        workSpec = jobspec.get_workspec_list()[0]
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, "workerID={0}".format(workSpec.workerID), method_name="check_workers")
        try:
            tmpLog.debug("Querying aCT for id {0}".format(workSpec.batchID))
            columns = ["actpandastatus", "error"]
            actjobs = self.actDB.getJobs("id={0}".format(workSpec.batchID), columns)
        except Exception as e:
            if self.actDB:
                tmpLog.error("Failed to query aCT DB: {0}".format(str(e)))
            # try again later
            return None, "Failed to query aCT DB"

        if not actjobs:
            tmpLog.error("Job with id {0} not found in aCT".format(workSpec.batchID))
            return False, "Job not found in aCT"

        actstatus = actjobs[0]["actpandastatus"]
        # Only check for final states
        if actstatus == "done":
            # Do post processing
            self.post_processing(workSpec, jobspec)
        elif actstatus == "donefailed":
            # Call post processing to collect attributes set by aCT for failed jobs
            self.post_processing(workSpec, jobspec)
            # Set error reported by aCT
            errorMsg = actjobs[0]["error"] or "Unknown error"
            error_code = WorkerErrors.error_codes.get("GENERAL_ERROR")
            jobspec.status = "failed"
            # No way to update workspec here
            # workSpec.set_supplemental_error(error_code=error_code, error_diag=errorMsg)
            jobspec.set_pilot_error(error_code, errorMsg)
            tmpLog.info("Job {0} failed with error {1}".format(jobspec.PandaID, errorMsg))
        elif actstatus == "donecancelled":
            # Nothing to do
            pass
        else:
            # Still staging
            return None, "still staging"

        tmpLog.info("ID {0} completed in state {1}".format(workSpec.batchID, actstatus))

        # Set dummy output file to finished
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            fileSpec.status = "finished"
        return True, ""

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        """Trigger the stage-out procedure for the job.
        Create a dummy output file to force harvester to wait until aCT
        job is done

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True: success, False: fatal failure, None: temporary failure)
                 and error dialog
        :rtype: (bool, string)
        """
        fileSpec = FileSpec()
        fileSpec.PandaID = jobspec.PandaID
        fileSpec.taskID = jobspec.taskID
        fileSpec.lfn = "dummy.{0}".format(jobspec.PandaID)
        fileSpec.scope = "dummy"
        fileSpec.fileType = "output"
        jobspec.add_in_file(fileSpec)

        return True, ""

    # zip output files
    def zip_output(self, jobspec):
        """Dummy"""
        return True, ""

    def post_processing(self, workspec, jobspec):
        """
        Take the jobReport placed by aCT in the access point and fill metadata
        attributes of the jobspec.
        """

        # get logger
        tmpLog = core_utils.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="post_processing")
        # look for job report
        jsonFilePath = os.path.join(workspec.get_access_point(), jsonJobReport)
        tmpLog.debug("looking for job report file {0}".format(jsonFilePath))
        try:
            with open(jsonFilePath) as jsonFile:
                jobreport = json.load(jsonFile)
        except BaseException:
            # Assume no job report available means true pilot or push mode
            # If job report is not available in full push mode aCT would have failed the job
            tmpLog.debug("no job report at {0}".format(jsonFilePath))
            return

        tmpLog.debug("got {0} kB of job report".format(os.stat(jsonFilePath).st_size / 1024))
        tmpLog.debug("pilot info for {0}: {1}".format(jobspec.PandaID, jobreport))

        # Set info for final heartbeat and final status
        jobspec.set_attributes({jobspec.PandaID: jobreport})
        jobspec.set_one_attribute("jobStatus", jobreport.get("state", "failed"))
        jobspec.status = jobreport.get("state", "failed")
