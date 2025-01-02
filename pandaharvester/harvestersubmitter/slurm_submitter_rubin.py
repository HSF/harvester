import os
import re
import stat
import subprocess
import tempfile
from math import ceil

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger("slurm_submitter_rubin")


# submitter for SLURM batch system
class SlurmSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.uploadLog = False
        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)
        if not hasattr(self, "localQueueName"):
            self.localQueueName = "grid"
        # ncore factor
        try:
            if type(self.nCoreFactor) in [dict]:
                # self.nCoreFactor is a dict for ucore
                # self.nCoreFactor = self.nCoreFactor
                pass
            else:
                self.nCoreFactor = int(self.nCoreFactor)
        except AttributeError:
            self.nCoreFactor = 1
        else:
            self.nCoreFactor = int(self.nCoreFactor)
            if (not self.nCoreFactor) or (self.nCoreFactor < 1):
                self.nCoreFactor = 1
        # num workers to check the partition
        try:
            self.nWorkersToCheckPartition = int(self.nWorkersToCheckPartition)
        except AttributeError:
            self.nWorkersToCheckPartition = 10
        if (not self.nWorkersToCheckPartition) or (self.nWorkersToCheckPartition < 1):
            self.nWorkersToCheckPartition = 1
        # partition configuration
        try:
            if not isinstance(self.partitions, (list, tuple)):
                self.partitions = self.partitions.split(",")
            self.partitions = [p.strip() for p in self.partitions]
        except AttributeError:
            self.partitions = None

    def get_queued_jobs(self, partition, logger):
        status = False
        num_pending_jobs = 0

        username = os.getlogin()
        # command = f"squeue -u {username} --partition={partition} | grep -e PD -e CF"
        command = f"squeue -u {username} --partition={partition}"
        logger.debug(f"check jobs in partition {partition} command: {command.split()}")
        p = subprocess.Popen(command.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        ret_code = p.returncode

        if ret_code == 0:
            stdout_str = stdout if (isinstance(stdout, str) or stdout is None) else stdout.decode()
            # stderr_str = stderr if (isinstance(stderr, str) or stderr is None) else stderr.decode()

            num_pending_jobs = 0
            for line in stdout_str.split("\n"):
                if len(line) == 0 or line.startswith("JobID") or line.startswith("--"):
                    continue

                batch_status = line.split()[4].strip()
                if batch_status in ["CF", "PD"]:
                    num_pending_jobs += 1
            logger.debug(f"number of pending jobs in partition {partition} with user {username}: {num_pending_jobs}")
            status = True
        else:
            logger.error(f"returncode: {ret_code}, stdout: {stdout}, stderr: {stderr}")

        return status, num_pending_jobs

    # get partition
    def get_partition(self, logger):
        if not self.partitions:
            return None

        logger.debug(f"partitions: {self.partitions}")
        num_pending_by_partition = {}
        for partition in self.partitions:
            status, num_pending_jobs = self.get_queued_jobs(partition, logger)
            if status:
                num_pending_by_partition[partition] = num_pending_jobs
        logger.debug(f"num_pending_by_partition: {num_pending_by_partition}")

        sorted_num_pending = dict(sorted(num_pending_by_partition.items(), key=lambda item: item[1]))
        if sorted_num_pending:
            selected_partition = list(sorted_num_pending.keys())[0]
            return selected_partition
        return None

    def get_core_factor(self, workspec, logger):
        try:
            if type(self.nCoreFactor) in [dict]:
                n_core_factor = self.nCoreFactor.get(workspec.jobType, {}).get(workspec.resourceType, 1)
                return int(n_core_factor)
            return int(self.nCoreFactor)
        except Exception as ex:
            logger.warn(f"Failed to get core factor: {ex}")
        return 1

    # submit workers
    def submit_workers(self, workspec_list):
        retList = []
        num_workSpec = 0
        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, f"workerID={workSpec.workerID}", method_name="submit_workers")
            # set nCore
            if self.nCore > 0:
                workSpec.nCore = self.nCore
            if num_workSpec % self.nWorkersToCheckPartition == 0:
                partition = self.get_partition(tmpLog)
                num_workSpec += 1
            # make batch script
            batchFile = self.make_batch_script(workSpec, partition, tmpLog)
            # command
            comStr = f"sbatch -D {workSpec.get_access_point()} {batchFile}"
            # submit
            tmpLog.debug(f"submit with {batchFile}")
            p = subprocess.Popen(comStr.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug(f"retCode={retCode}")
            stdOut_str = stdOut if (isinstance(stdOut, str) or stdOut is None) else stdOut.decode()
            stdErr_str = stdErr if (isinstance(stdErr, str) or stdErr is None) else stdErr.decode()
            if retCode == 0:
                # extract batchID
                workSpec.batchID = re.search("[^0-9]*([0-9]+)[^0-9]*$", f"{stdOut_str}").group(1)
                tmpLog.debug(f"batchID={workSpec.batchID}")
                # set log files
                if self.logBaseURL and self.logDir:
                    stdOut, stdErr = self.get_log_file_names(workSpec.accessPoint, workSpec.workerID)
                    rel_stdOut = os.path.relpath(stdOut, self.logDir)
                    rel_stdErr = os.path.relpath(stdErr, self.logDir)
                    log_stdOut = os.path.join(self.logBaseURL, rel_stdOut)
                    log_stdErr = os.path.join(self.logBaseURL, rel_stdErr)
                    workSpec.set_log_file("stdout", log_stdOut)
                    workSpec.set_log_file("stderr", log_stdErr)
                tmpRetVal = (True, "")
            else:
                # failed
                errStr = f"{stdOut_str} {stdErr_str}"
                tmpLog.error(errStr)
                tmpRetVal = (False, errStr)
            retList.append(tmpRetVal)
        return retList

    def make_placeholder_map(self, workspec, partition, logger):
        timeNow = core_utils.naive_utcnow()

        panda_queue_name = self.queueName
        this_panda_queue_dict = dict()

        # get default information from queue info
        n_core_per_node_from_queue = this_panda_queue_dict.get("corecount", 1) if this_panda_queue_dict.get("corecount", 1) else 1

        # get override requirements from queue configured
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

        placeholder_map = {
            "nCorePerNode": n_core_per_node,
            "nCoreTotal": n_core_total_factor,
            "nCoreFactor": n_core_factor,
            "nNode": n_node,
            "requestRam": request_ram_factor,
            "requestRamBytes": request_ram_bytes_factor,
            "requestRamPerCore": request_ram_per_core,
            "requestRamBytesPerCore": request_ram_bytes_per_core,
            "requestDisk": request_disk,
            "requestWalltime": request_walltime,
            "requestWalltimeMinute": request_walltime_minute,
            "requestCputime": request_cputime,
            "requestCputimeMinute": request_cputime_minute,
            "accessPoint": workspec.accessPoint,
            "harvesterID": harvester_config.master.harvester_id,
            "workerID": workspec.workerID,
            "computingSite": workspec.computingSite,
            "pandaQueueName": panda_queue_name,
            "localQueueName": self.localQueueName,
            # 'x509UserProxy': x509_user_proxy,
            "logDir": self.logDir,
            "logSubDir": os.path.join(self.logDir, timeNow.strftime("%y-%m-%d_%H")),
            "jobType": workspec.jobType,
            "partition": partition,
        }
        for k in ["tokenDir", "tokenName", "tokenOrigin", "submitMode"]:
            try:
                placeholder_map[k] = getattr(self, k)
            except Exception:
                pass
        return placeholder_map

    # make batch script
    def make_batch_script(self, workspec, partition, logger):
        # template for batch script
        with open(self.templateFile) as f:
            template = f.read()
        tmpFile = tempfile.NamedTemporaryFile(delete=False, suffix="_submit.sh", dir=workspec.get_access_point())
        placeholder = self.make_placeholder_map(workspec, partition, logger)
        tmpFile.write(str(template.format_map(core_utils.SafeDict(placeholder))).encode("latin_1"))
        tmpFile.close()

        # set execution bit and group permissions on the temp file
        st = os.stat(tmpFile.name)
        os.chmod(tmpFile.name, st.st_mode | stat.S_IEXEC | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)

        return tmpFile.name

    # get log file names
    def get_log_file_names_old(self, batch_script, batch_id):
        stdOut = None
        stdErr = None
        with open(batch_script) as f:
            for line in f:
                if not line.startswith("#SBATCH"):
                    continue
                items = line.split()
                if "-o" in items:
                    stdOut = items[-1].replace("$SLURM_JOB_ID", batch_id)
                elif "-e" in items:
                    stdErr = items[-1].replace("$SLURM_JOB_ID", batch_id)
        return stdOut, stdErr

    def get_log_file_names(self, access_point, worker_id):
        stdOut = os.path.join(access_point, '%s.out' % worker_id)
        stdErr = os.path.join(access_point, '%s.err' % worker_id)
        return stdOut, stdErr
