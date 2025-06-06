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
baseLogger = core_utils.setup_logger("slurm_submitter")


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

    # submit workers
    def submit_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, f"workerID={workSpec.workerID}", method_name="submit_workers")
            # set nCore
            if self.nCore > 0:
                workSpec.nCore = self.nCore
            # make batch script
            batchFile = self.make_batch_script(workSpec, tmpLog)
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

    def get_core_factor(self, workspec, logger):
        try:
            if type(self.nCoreFactor) in [dict]:
                n_core_factor = self.nCoreFactor.get(workspec.jobType, {}).get(workspec.resourceType, 1)
                return int(n_core_factor)
            return int(self.nCoreFactor)
        except Exception as ex:
            logger.warning(f"Failed to get core factor: {ex}")
        return 1

    def make_placeholder_map(self, workspec, logger):
        timeNow = core_utils.naive_utcnow()

        panda_queue_name = self.queueName
        this_panda_queue_dict = dict()

        # get default information from queue info
        n_core_per_node_from_queue = this_panda_queue_dict.get("corecount", 1) if this_panda_queue_dict.get("corecount", 1) else 1

        # get override requirements from queue configured
        try:
            n_core_per_node = self.nCorePerNode if self.nCorePerNode is not None else n_core_per_node_from_queue
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

        if not n_core_per_node or n_core_per_node < 1:
            n_node = 1
        else:
            n_node = ceil(n_core_total / n_core_per_node)
        request_ram_factor = request_ram * n_core_factor
        request_ram_bytes = request_ram * 2**20
        request_ram_bytes_factor = request_ram * 2**20 * n_core_factor
        request_ram_per_core = ceil(request_ram * n_node / n_core_total)
        request_ram_bytes_per_core = ceil(request_ram_bytes * n_node / n_core_total)
        request_cputime = request_walltime * n_core_total
        request_walltime_minute = ceil(request_walltime / 60)
        request_cputime_minute = ceil(request_cputime / 60)

        # GTAG
        if self.logBaseURL and self.logDir:
            stdOut, stdErr = self.get_log_file_names(workspec.accessPoint, workspec.workerID)
            rel_stdOut = os.path.relpath(stdOut, self.logDir)
            log_stdOut = os.path.join(self.logBaseURL, rel_stdOut)
            gtag = log_stdOut
        else:
            gtag = "unknown"

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
            "gtag": gtag,
        }
        for k in ["tokenDir", "tokenName", "tokenOrigin", "submitMode"]:
            try:
                placeholder_map[k] = getattr(self, k)
            except Exception:
                pass
        return placeholder_map

    # make batch script
    def make_batch_script(self, workspec, logger):
        # template for batch script
        with open(self.templateFile) as f:
            template = f.read()
        tmpFile = tempfile.NamedTemporaryFile(delete=False, suffix="_submit.sh", dir=workspec.get_access_point())
        placeholder = self.make_placeholder_map(workspec, logger)
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
        stdOut = os.path.join(access_point, f"{worker_id}.out")
        stdErr = os.path.join(access_point, f"{worker_id}.err")
        return stdOut, stdErr
