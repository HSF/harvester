import os
import re
import stat
import subprocess
import time
import yaml
from typing import Optional
from math import ceil

from globus_compute_sdk import Executor, Client
from globus_compute_sdk.sdk.shell_function import ShellFunction, ShellResult

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper

# logger
baseLogger = core_utils.setup_logger("globus_compute_slurm_submitter")


class CustomShellFunction(ShellFunction):
    def __init__(self, cmd: str, log_dir: Optional[str] = None, **kwargs):
        self.log_dir = log_dir
        if not self.log_dir:
            self.log_dir = "~/.globus_compute/CustomShellFunctionLog"
        super().__init__(cmd=cmd, **kwargs)

    def execute_cmd_line(self, cmd: str) -> ShellResult:
        import os
        import subprocess

        sandbox_error_message = None

        try:
            task_uuid = str(os.environ["GC_TASK_UUID"])
        except KeyError:
            raise RuntimeError("Environment variable GC_TASK_UUID is required")

        run_dir = os.path.join(self.log_dir, task_uuid)
        if run_dir:
            os.makedirs(run_dir, exist_ok=True)
            os.chdir(run_dir)

        stdout_path = self.stdout or os.path.join(run_dir, "stdout")
        stderr_path = self.stderr or os.path.join(run_dir, "stderr")
        std_out = self.open_std_fd(stdout_path, mode="w+")
        std_err = self.open_std_fd(stderr_path, mode="w+")

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=std_out,
                stderr=std_err,
                shell=True,
                executable="/bin/bash",
                close_fds=False,
            )
            proc.wait(timeout=self.walltime)
            returncode = proc.returncode
            exception_name = None if returncode == 0 else "subprocess.CalledProcessError"
        except subprocess.TimeoutExpired:
            returncode = 124
            exception_name = "subprocess.TimeoutExpired"
        finally:
            stdout_snippet, stderr_snippet = self.get_and_close_streams(std_out, std_err)

        return ShellResult(
            cmd=cmd,
            stdout=stdout_snippet,
            stderr=stderr_snippet,
            returncode=returncode,
            exception_name=exception_name,
        )




class GlobusComputeSlurmSubmitter(PluginBase):
    def __init__(self, **kwargs):
        self.uploadLog = False
        self.logBaseURL = None
        PluginBase.__init__(self, **kwargs)
        if not hasattr(self, "localQueueName"):
            self.localQueueName = "grid"
        try:
            if hasattr(self, "nCoreFactor"):
                if type(self.nCoreFactor) in [dict]:
                    # self.nCoreFactor is a dict for ucore
                    # self.nCoreFactor = self.nCoreFactor
                    pass
                else:
                    self.nCoreFactor = int(self.nCoreFactor)
                    if (not self.nCoreFactor) or (self.nCoreFactor < 1):
                        self.nCoreFactor = 1
            else:
                self.nCoreFactor = 1
        except AttributeError:
            self.nCoreFactor = 1

        self.mep_id         = kwargs.get("mep_id")
        self.template_file  = kwargs.get("templateFile")
        self.gc_client      = Client()
        self.config_file    = kwargs.get("config_file")
        self.slurm_log_dir  = kwargs.get("slurm_log_dir")

        with open(self.config_file, "r") as cf:
            self.config = yaml.safe_load(cf)

        with open(self.template_file, 'r') as file:
            self.template_init = file.read()
    
    def get_core_factor(self, workspec, logger):
        try:
            if type(self.nCoreFactor) in [dict]:
                n_core_factor = self.nCoreFactor.get(workspec.jobType, {}).get(workspec.resourceType, 1)
                return int(n_core_factor)
            return int(self.nCoreFactor)
        except Exception as ex:
            logger.warning(f"Failed to get core factor: {ex}")
        return 1

    def render_template(self, workspec, logger):
        timeNow = core_utils.naive_utcnow()

        this_panda_queue_dict = dict()
        n_core_per_node_from_queue = this_panda_queue_dict.get("corecount", 1) if this_panda_queue_dict.get("corecount", 1) else 1
        try:
            n_core_per_node = self.nCorePerNode if self.nCorePerNode else n_core_per_node_from_queue
        except AttributeError:
            n_core_per_node = n_core_per_node_from_queue
        if not n_core_per_node:
            n_core_per_node = self.nCore

        n_core_factor = self.get_core_factor(workspec, logger)
        n_core_total = workspec.nCore if workspec.nCore else n_core_per_node
        n_core_total_factor = n_core_total * n_core_factor
        request_ram = max(workspec.minRamCount, 1 * n_core_total) if workspec.minRamCount else 1 * n_core_total
        request_disk = workspec.maxDiskCount * 1024 if workspec.maxDiskCount else 1
        request_walltime = workspec.maxWalltime if workspec.maxWalltime else 0

        n_node = ceil(n_core_total / n_core_per_node)
        request_ram_factor = request_ram * n_core_factor
        request_ram_bytes = request_ram * 2**20
        request_ram_bytes_factor = request_ram * 2**20 * n_core_factor
        request_ram_per_core = ceil(request_ram * n_node / n_core_total)
        request_ram_bytes_per_core = ceil(request_ram_bytes * n_node / n_core_total)
        request_cputime = request_walltime * n_core_total
        request_walltime_minute = ceil(request_walltime / 60)
        request_cputime_minute = ceil(request_cputime / 60)

        # For remote working dir
        if self.remote_workdir:
            remote_accessPoint = os.path.join(self.remote_workdir, self.queueName, str(workspec.workerID))
        else:
            remote_accessPoint = workspec.accessPoint

        variables = {}

        variables['nCorePerNode']           = n_core_per_node
        variables['nCoreTotal']             = n_core_total_factor
        variables['nCoreFactor']            = n_core_factor
        variables['nNode']                  = n_node
        variables['requestRam']             = request_ram_factor,
        variables['requestRamBytes']        = request_ram_bytes_factor,
        variables['requestRamPerCore']      = request_ram_per_core,
        variables['requestRamBytesPerCore'] = request_ram_bytes_per_core,
        variables['requestDisk']            = request_disk,
        variables['requestWalltime']        = request_walltime,
        variables['requestWalltimeMinute']  = request_walltime_minute,
        variables['requestCputime']         = request_cputime,
        variables['requestCputimeMinute']   = request_cputime_minute,
        variables['accessPoint']            = workspec.accessPoint
        variables['remote_accessPoint']     = remote_accessPoint
        variables['harvesterID']            = harvester_config.master.harvester_id 
        variables['workerID']               = workspec.workerID
        variables['computingSite']          = workspec.computingSite
        variables['pandaQueueName']         = self.queueName
        variables['localQueueName']         = self.localQueueName
        variables['jobType']                = workspec.jobType
        variables['tokenOrigin']            = self.tokenOrigin
        variables['tokenDir']               = self.tokenDir
        variables['tokenName']              = self.tokenName
        # For locating wrapper wrapper file
        variables['harvester_dir']          = self.config.get('harvester_dir', '/global/common/software/m2616/harvester-perlmutter')
        # For how many pilots per node
        variables['harvester_tasks_per_node']   = self.config.get('tasks_per_node', 4)
        # For ATHENA related conf
        ATHENA_COMPUTE_POLICY               = self.config.get('ATHENA_COMPUTE_POLICY', 'normal')
        if ATHENA_COMPUTE_POLICY == 'normal':
            variables['ATHENA_nCorePerNode']    = self.config.get('ATHENA_nCorePerNode', 256)
            variables['ATHENA_PROC_NUMBER_JOB'] = variables['ATHENA_nCorePerNode'] // variables['harvester_tasks_per_node']
            variables['ATHENA_PROC_NUMBER']     = variables['ATHENA_PROC_NUMBER_JOB']
            variables['ATHENA_CORE_NUMBER']     = variables['ATHENA_PROC_NUMBER_JOB']
        else:   # Currently only support normal policy for ATHENA 
            exit(1)
    
        task_content = self.template_init.format(**variables)
        user_endpoint_config = {
            "account":           self.config["account"],
            "partition":         self.config["partition"],
            "parallelism":       n_node,
            "nodes_per_block":   n_node,
            "walltime":          self.config["walltime"],
            "scheduler_options": self.config["scheduler_options"],
            "worker_init_extra": self.config["worker_init_extra"],
            "max_blocks":        self.config["max_blocks"],
        }
        return task_content, user_endpoint_config

    def submit_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            tmpLog = self.make_logger(baseLogger, f"workerID={workSpec.workerID}", method_name="submit_workers")
            tmpLog.debug(f"Step 1: Start submission with workSpec with detail of {workSpec.__dict__}")
            try:
                task_content, user_endpoint_config = self.render_template(workSpec, tmpLog)
                tmpLog.debug(f"Step 2: Rendering finished. \nGet task_content: \n{task_content}\nGet user_endpoint_config: \n{user_endpoint_config}")
    
                batch = self.gc_client.create_batch(user_endpoint_config=user_endpoint_config)
    
                self.bf = CustomShellFunction(cmd=task_content, snippet_lines=2000, log_dir=self.slurm_log_dir)
                self.func_id = self.gc_client.register_function(self.bf)
                tmpLog.debug(f"Finish registration of function with func_id = {self.func_id}")
    
                batch.add(function_id=self.func_id)
                tmpLog.debug(f"Step 3: Finish creating batch, now going to submit batch with GC.")
                batch_res = self.gc_client.batch_run(batch=batch, endpoint_id=self.mep_id)
                tmpLog.debug(f"Step 4: Made a batch submission to GC. Got batch_res = \n{batch_res}")
                for func_id, each_task_list in batch_res['tasks'].items():
                    workSpec.batchID = each_task_list[0]
                    globus_compute_attr_dict = {}
                    globus_compute_attr_dict["sandbox_dir"] = os.path.join(self.slurm_log_dir, workSpec.batchID)
                    globus_compute_attr_dict["slurmID"] = None
                    workSpec.set_work_attributes({"globus_compute_attr": globus_compute_attr_dict}) 
                    tmpLog.debug(f"Now setting: \nbatchID = {workSpec.batchID}, \nGC sandbox dir = {globus_compute_attr_dict['sandbox_dir']}")
                tmpRetVal = (True, "")
            except Exception as e:
                tmpLog.error(f"Error during submit workers: {e}")
                tmpRetVal = (False, str(e))
            retList.append(tmpRetVal)
        return retList
