import re

from globus_compute_sdk import Executor, Client
from globus_compute_sdk.sdk.shell_function import ShellFunction, ShellResult

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec
from globus_sdk.services.compute.errors import ComputeAPIError
from globus_compute_sdk.errors.error_types import TaskExecutionFailed

# logger
baseLogger = core_utils.setup_logger("globus_compute_slurm_monitor")


# monitor for Globus Compute on SLURM system
class GlobusComputeSlurmMonitor(PluginBase):
    # constructor
    def __init__(self, **kwargs):
        PluginBase.__init__(self, **kwargs)
        self.uep_id         = kwargs.get("uep_id")
        self.gc_client      = Client()
        self.gc_executor    = Executor(endpoint_id=self.uep_id)

        self.cmd_get_slurm_id = r'''
#!/usr/bin/env bash

if [ ! -e "{gc_sandbox_dir}/slurm_job_id" ]; then
  exit 1
fi

read -r slurm_id < "{gc_sandbox_dir}/slurm_job_id"
printf '%s' "$slurm_id"

exit 0
'''
        self.bf_get_slurm_id = ShellFunction(self.cmd_get_slurm_id)

        self.cmd_sacct = r"sacct --job={slurm_job_id}"
        self.bf_sacct = ShellFunction(self.cmd_sacct)


    # Helper function, given a slurm job id, return the status of slurm sandbox as in local mode
    def get_status_sacct(self, tmplog, slurm_job_id):
        tmplog.debug(f"Running get_status_sacct() for slurm_job_id = {slurm_job_id}")
        future = self.gc_executor.submit(self.bf_sacct, slurm_job_id=slurm_job_id)
        sr = future.result()

        stdOut = sr.stdout
        stdErr = sr.stderr
        retCode = sr.returncode
        exception_name = sr.exception_name

        tmplog.debug(f"for slurm_job_id = {slurm_job_id}, stdOut={stdOut}")
        tmplog.debug(f"for slurm_job_id = {slurm_job_id}, stdErr={stdErr}")
        tmplog.debug(f"for slurm_job_id = {slurm_job_id}, retCode={retCode}")
        tmplog.debug(f"for slurm_job_id = {slurm_job_id}, exception_name={exception_name}")

        errStr = ""
        stdOut_str = stdOut if (isinstance(stdOut, str) or stdOut is None) else stdOut.decode()
        stdErr_str = stdErr if (isinstance(stdErr, str) or stdErr is None) else stdErr.decode()

        newStatus = WorkSpec.ST_failed
        if retCode == 0:
            for tmpLine in stdOut_str.split("\n"):
                tmpMatch = re.search(f"{slurm_job_id} ", tmpLine)
                if tmpMatch is not None:
                    errStr = tmpLine
                    batchStatus = tmpLine.split()[5]
                    if batchStatus in ["RUNNING", "COMPLETING", "STOPPED", "SUSPENDED"]:
                        newStatus = WorkSpec.ST_running
                    elif batchStatus in ["COMPLETED", "PREEMPTED", "TIMEOUT"]:
                        newStatus = WorkSpec.ST_finished
                    elif batchStatus in ["CANCELLED"]:
                        newStatus = WorkSpec.ST_cancelled
                    elif batchStatus in ["CONFIGURING", "PENDING"]:
                        newStatus = WorkSpec.ST_submitted
                    else:
                        newStatus = WorkSpec.ST_failed
                    tmplog.debug(f"batchStatus {batchStatus} -> workerStatus {newStatus}")
                    break
            return newStatus, errStr
        else:
            # failed
            errStr = f"{stdOut_str} {stdErr_str}"
            tmplog.error(errStr)
            if "slurm_load_jobs error: Invalid job id specified" in errStr:
                newStatus = WorkSpec.ST_failed
            return newStatus, errStr

    # Helper function, given a gc sandbox dir which includes stdout/stderr/slurm_job_id, check if slurm_job_id exist and try to return slurm_job_id
    # return code = 0: success, stdout is the slurm_job_id
    # return code = 1: file does not exist, should try again later
    def get_slurm_job_id(self, tmplog, gc_sandbox_dir):
        tmplog.debug(f"Running get_slurm_job_id() for gc_sandbox_dir = {gc_sandbox_dir}")
        future = self.gc_executor.submit(self.bf_get_slurm_id, gc_sandbox_dir=gc_sandbox_dir)
        sr = future.result()

        stdOut = sr.stdout
        stdErr = sr.stderr
        retCode = sr.returncode
        exception_name = sr.exception_name

        tmplog.debug(f"for gc_sandbox_dir = {gc_sandbox_dir}, stdOut={stdOut}")
        tmplog.debug(f"for gc_sandbox_dir = {gc_sandbox_dir}, stdErr={stdErr}")
        tmplog.debug(f"for gc_sandbox_dir = {gc_sandbox_dir}, retCode={retCode}")
        tmplog.debug(f"for gc_sandbox_dir = {gc_sandbox_dir}, exception_name={exception_name}")

        return retCode, stdOut

    # check workers
    # First check if slurm_job_id is already fetched. 
    # If already have, use sacct to query job info
    # If not, try to fetch slurm job id, if fetch successfully, skip the rest and continue, otherwise crosscheck with the status of the job using gc API
    def check_workers(self, workspec_list):
        retList = []
        for work_spec in workspec_list:
            tmplog = self.make_logger(baseLogger, f"workerID={work_spec.workerID}", method_name="check_workers")
            if work_spec.slurmID:
                tmplog.debug(f"for workerID={work_spec.workerID}, slurm_job_id is fetched successfully with {work_spec.slurmID}, so invoke get_status_sacct")
                new_status, err_str = self.get_status_sacct(tmplog, work_spec.slurmID)
            else:
                tmplog.debug(f"for workerID={work_spec.workerID}, slurm_job_id has not been fetched, so try to fetch it")
                retCode, stdOut = self.get_slurm_job_id(tmplog, work_spec.gc_sandbox_dir)
                new_status = None
                err_str = ""
                status_dict = None

                if retCode == 0:
                    work_spec.slurmID = stdOut.strip()
                    new_status = work_spec.ST_running
                    err_str = ""
                else:
                    if retCode == 1:
                        tmplog.debug(f"File {work_spec.gc_sandbox_dir} does not exist, need to check if the job is still query")
                    else:
                        tmplog.error(f"Unknown return code {retCode} for {work_spec.gc_sandbox_dir}, need to check")
                    
                    task_list = [work_spec.batchID]
                    try:
                        status_dict = self.gc_client.get_batch_result(task_list)
                    except ComputeAPIError as e:
                        tmplog.error("ComputeAPIError occurred while retrieving batch results: %s", e)
                        new_status = work_spec.ST_failed
                        err_str = "ComputeAPIError"
                    except TaskExecutionFailed as e:
                        tmplog.error("TaskExecutionFailed occurred while retrieving batch results: %s", e)
                        new_status = work_spec.ST_failed
                        err_str = "TaskExecutionFailed"
                    except Exception as e:
                        tmplog.exception("An unexpected error occurred while retrieving batch results")
                        new_status = work_spec.ST_failed
                        err_str = "Unexpected error"

                    if status_dict:
                        tmplog.debug(f"Status dictionary received: {status_dict}")
                        worker_status = status_dict.get(work_spec.batchID, {})
                        pending = worker_status.get('pending')
                        status = worker_status.get('status')
                        err_str = ""
                        if pending:
                            new_status = work_spec.ST_running
                        else:
                            if status == "success":
                                new_status = work_spec.ST_finished
                            else:
                                tmplog.debug("Unexpected state: pending=%s, status=%s", pending, status)
                                new_status = work_spec.ST_running
                        tmplog.debug(f"Final status for worker {work_spec.workerID}: pending={pending}, status={status}, new_status={new_status}")
                    else:
                        new_status = work_spec.ST_failed
                        err_str = "Careful! It might not finished! This is testing empty dict!"

            retList.append((new_status, err_str))

        return True, retList
