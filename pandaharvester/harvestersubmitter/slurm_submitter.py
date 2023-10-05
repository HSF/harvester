import tempfile
import re

import six
import os
import stat
import datetime
from math import ceil

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

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

    # submit workers
    def submit_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workSpec.workerID), method_name="submit_workers")
            # set nCore
            workSpec.nCore = self.nCore
            # make batch script
            batchFile = self.make_batch_script(workSpec)
            # command
            comStr = "sbatch -D {0} {1}".format(workSpec.get_access_point(), batchFile)
            # submit
            tmpLog.debug("submit with {0}".format(batchFile))
            p = subprocess.Popen(comStr.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug("retCode={0}".format(retCode))
            stdOut_str = stdOut if (isinstance(stdOut, str) or stdOut is None) else stdOut.decode()
            stdErr_str = stdErr if (isinstance(stdErr, str) or stdErr is None) else stdErr.decode()
            if retCode == 0:
                # extract batchID
                workSpec.batchID = re.search("[^0-9]*([0-9]+)[^0-9]*$", "{0}".format(stdOut_str)).group(1)
                tmpLog.debug("batchID={0}".format(workSpec.batchID))
                # set log files
                if self.uploadLog:
                    if self.logBaseURL is None:
                        baseDir = workSpec.get_access_point()
                    else:
                        baseDir = self.logBaseURL
                    stdOut, stdErr = self.get_log_file_names(batchFile, workSpec.batchID)
                    if stdOut is not None:
                        workSpec.set_log_file("stdout", "{0}/{1}".format(baseDir, stdOut))
                    if stdErr is not None:
                        workSpec.set_log_file("stderr", "{0}/{1}".format(baseDir, stdErr))
                tmpRetVal = (True, "")
            else:
                # failed
                errStr = "{0} {1}".format(stdOut_str, stdErr_str)
                tmpLog.error(errStr)
                tmpRetVal = (False, errStr)
            retList.append(tmpRetVal)
        return retList

    def make_placeholder_map(self, workspec):
        timeNow = datetime.datetime.utcnow()

        panda_queue_name = self.queueName
        this_panda_queue_dict = dict()

        # get default information from queue info
        n_core_per_node_from_queue = this_panda_queue_dict.get("corecount", 1) if this_panda_queue_dict.get("corecount", 1) else 1

        # get override requirements from queue configured
        try:
            n_core_per_node = self.nCorePerNode if self.nCorePerNode else n_core_per_node_from_queue
        except AttributeError:
            n_core_per_node = n_core_per_node_from_queue

        n_core_total = workspec.nCore if workspec.nCore else n_core_per_node
        request_ram = max(workspec.minRamCount, 1 * n_core_total) if workspec.minRamCount else 1 * n_core_total
        request_disk = workspec.maxDiskCount * 1024 if workspec.maxDiskCount else 1
        request_walltime = workspec.maxWalltime if workspec.maxWalltime else 0

        n_node = ceil(n_core_total / n_core_per_node)
        request_ram_bytes = request_ram * 2**20
        request_ram_per_core = ceil(request_ram * n_node / n_core_total)
        request_ram_bytes_per_core = ceil(request_ram_bytes * n_node / n_core_total)
        request_cputime = request_walltime * n_core_total
        request_walltime_minute = ceil(request_walltime / 60)
        request_cputime_minute = ceil(request_cputime / 60)

        placeholder_map = {
            "nCorePerNode": n_core_per_node,
            "nCoreTotal": n_core_total,
            "nNode": n_node,
            "requestRam": request_ram,
            "requestRamBytes": request_ram_bytes,
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
        }
        for k in ["tokenDir", "tokenName", "tokenOrigin", "submitMode"]:
            try:
                placeholder_map[k] = getattr(self, k)
            except Exception:
                pass
        return placeholder_map

    # make batch script
    def make_batch_script(self, workspec):
        # template for batch script
        with open(self.templateFile) as f:
            template = f.read()
        tmpFile = tempfile.NamedTemporaryFile(delete=False, suffix="_submit.sh", dir=workspec.get_access_point())
        placeholder = self.make_placeholder_map(workspec)
        tmpFile.write(six.b(template.format_map(core_utils.SafeDict(placeholder))))
        tmpFile.close()

        # set execution bit and group permissions on the temp file
        st = os.stat(tmpFile.name)
        os.chmod(tmpFile.name, st.st_mode | stat.S_IEXEC | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)

        return tmpFile.name

    # get log file names
    def get_log_file_names(self, batch_script, batch_id):
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
