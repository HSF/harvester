import os
import json
import stat
import tempfile
import requests
import time
from math import ceil

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.superfacility_utils import SuperfacilityClient

# logger
baseLogger = core_utils.setup_logger("superfacility_submitter")


# submitter for SuperFacility API
class SuperfacilitySubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.uploadLog = False
        self.logBaseURL = None  #Need to fix the logic with that!
        PluginBase.__init__(self, **kwarg)
        self.num_retry_get_batch_id = kwarg.get("num_retry_get_batch_id", 5)
        self.time_interval_retry_get_batch_id = kwarg.get("time_interval_retry_get_batch_id", 5)
        self.cred_dir = kwarg.get("superfacility_cred_dir")
        self.sf_client = SuperfacilityClient(self.cred_dir)
        
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
            # make batch script, here we create batch script at where harvester install
            batchFile = self.make_batch_script(workSpec, tmpLog)
            try:
                with open(batchFile, 'r') as f:
                    script_string = f.read()
            except (FileNotFoundError, IOError) as e:
                err = f"Failed to open batch script '{batchFile}': {e}"
                tmpLog.error(err)
                retList.append((False, err))
                continue
            try:
                r = self.sf_client.post("/compute/jobs/perlmutter", 
                                        data = {"job": script_string, "isPath": False})
                data = r.json()
            except requests.HTTPError as e:
                err = f"Superfacility submit error: {e}"
                tmpLog.error(err)
                retList.append((False, err))
                continue

            task_id = data.get("task_id")
            if not task_id:
                err = f"Superfacility job submission return no task_id: {data}"
                tmpLog.error(err)
                retList.append((False, err))
                continue

            # Where shall I put those logic? Better to look like slurm_submitter so that we get slurm_job_id here
            # Introduce a configuration parameter (sleep time, num_try), document for latency
            stop = False
            for num_try in range(self.num_retry_get_batch_id):
                try:
                    r = self.sf_client.get(f"/tasks/{task_id}")
                    data = r.json()
                except requests.HTTPError as e:
                    err = f"Superfacility get batch submission task error: {e}"
                    tmpLog.error(err)
                    stop = True
                    retList.append((False, err))
                    break
                
                if data.get('status') == 'completed':
                    inner_r = json.loads(data.get('result'))
                    tmpLog.debug(f"Command status: {inner_r['status']}")
                    tmpLog.debug(f"Command error:  {inner_r['error']}")
                    tmpLog.debug(f"Assigned batchID: {inner_r['jobid']}")
                    workSpec.batchID = inner_r['jobid']
                    break
                time.sleep(self.time_interval_retry_get_batch_id)

            if stop == True:
                continue
            retList.append((True, ""))

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
        request_ram_bytes = request_ram * (2**20)
        request_ram_bytes_factor = request_ram_bytes * n_core_factor
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

