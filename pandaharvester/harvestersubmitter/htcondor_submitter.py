import errno
import json
import os
import random
import re
import socket
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from math import ceil

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.resource_type_mapper import ResourceTypeMapper
from pandaharvester.harvestermessenger.base_messenger import BaseMessenger
from pandaharvester.harvestermisc.htcondor_utils import (
    CondorJobSubmit,
    get_job_id_tuple_from_batchid,
)
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestermisc.token_utils import endpoint_to_filename
from pandaharvester.harvestersubmitter import submitter_common

# logger
baseLogger = core_utils.setup_logger("htcondor_submitter")

# base messenger instance
base_messenger = BaseMessenger()


def _condor_macro_replace(string, **kwarg):
    """
    Replace condor Macro from SDF file, return string
    """
    new_string = string
    macro_map = {
        "\$\(Cluster\)": str(kwarg["ClusterId"]),
        "\$\(Process\)": str(kwarg["ProcId"]),
    }
    for k, v in macro_map.items():
        new_string = re.sub(k, v, new_string)
    return new_string


def submit_bag_of_workers(data_list):
    """
    submit a bag of workers
    """
    # make logger
    tmpLog = core_utils.make_logger(baseLogger, method_name="submit_bag_of_workers")
    # keep order of workers in data_list
    workerIDs_list = [data["workspec"].workerID for data in data_list]
    # initialization
    worker_retval_map = {}
    worker_data_map = {}
    host_jdl_list_workerid_map = {}
    # go
    for data in data_list:
        workspec = data["workspec"]
        workerID = workspec.workerID
        worker_data_map[workerID] = data
        to_submit = data["to_submit"]
        # no need to submit bad worker
        if not to_submit:
            errStr = f"{workerID} not submitted due to incomplete data of the worker"
            tmpLog.warning(errStr)
            tmpRetVal = (None, errStr)
            # return tmpRetVal, workspec.get_changed_attributes()
            worker_retval_map[workerID] = (tmpRetVal, workspec.get_changed_attributes())
        # attributes
        try:
            use_spool = data["use_spool"]
        except KeyError:
            errStr = f"{workerID} not submitted due to incomplete data of the worker"
            tmpLog.warning(errStr)
            tmpRetVal = (None, errStr)
            # return tmpRetVal, workspec.get_changed_attributes()
            worker_retval_map[workerID] = (tmpRetVal, workspec.get_changed_attributes())
        else:
            workspec.reset_changed_list()
            # fill in host_jdl_list_workerid_map
            a_jdl, placeholder_map = make_a_jdl(**data)
            val = (workspec, a_jdl, placeholder_map)
            try:
                host_jdl_list_workerid_map[workspec.submissionHost].append(val)
            except KeyError:
                host_jdl_list_workerid_map[workspec.submissionHost] = [val]
    # loop over submissionHost
    for host, val_list in host_jdl_list_workerid_map.items():
        # make jdl string of workers
        jdl_list = [val[1] for val in val_list]
        # condor job submit object
        tmpLog.debug(f"submitting to submissionHost={host}")
        # submit
        try:
            condor_job_submit = CondorJobSubmit(id=host)
            batchIDs_list, ret_err_str = condor_job_submit.submit(jdl_list, use_spool=use_spool)
        except Exception as e:
            batchIDs_list = None
            ret_err_str = f"Exception {e.__class__.__name__}: {e}"
        # result
        if batchIDs_list:
            # submitted
            n_workers = len(val_list)
            tmpLog.debug(f"submitted {n_workers} workers to submissionHost={host}")
            for val_i in range(n_workers):
                val = val_list[val_i]
                workspec = val[0]
                placeholder_map = val[2]
                # got batchID
                workspec.batchID = batchIDs_list[val_i]
                tmpLog.debug(f"workerID={workspec.workerID} submissionHost={workspec.submissionHost} batchID={workspec.batchID}")
                # get worker data
                data = worker_data_map[workspec.workerID]
                # set computingElement
                ce_info_dict = data["ce_info_dict"]
                workspec.computingElement = ce_info_dict.get("ce_endpoint", "")
                # set log
                batch_log_dict = data["batch_log_dict"]
                (clusterid, procid) = get_job_id_tuple_from_batchid(workspec.batchID)
                batch_log = _condor_macro_replace(batch_log_dict["batch_log"], ClusterId=clusterid, ProcId=procid).format(**placeholder_map)
                batch_stdout = _condor_macro_replace(batch_log_dict["batch_stdout"], ClusterId=clusterid, ProcId=procid).format(**placeholder_map)
                batch_stderr = _condor_macro_replace(batch_log_dict["batch_stderr"], ClusterId=clusterid, ProcId=procid).format(**placeholder_map)
                try:
                    batch_jdl = f"{batch_stderr[:-4]}.jdl"
                except Exception:
                    batch_jdl = None
                workspec.set_log_file("batch_log", batch_log)
                workspec.set_log_file("stdout", batch_stdout)
                workspec.set_log_file("stderr", batch_stderr)
                workspec.set_log_file("jdl", batch_jdl)
                if not workspec.get_jobspec_list():
                    tmpLog.debug(f"No jobspec associated in the worker of workerID={workspec.workerID}")
                else:
                    for jobSpec in workspec.get_jobspec_list():
                        # using batchLog and stdOut URL as pilotID and pilotLog
                        jobSpec.set_one_attribute("pilotID", workspec.workAttributes["stdOut"])
                        jobSpec.set_one_attribute("pilotLog", workspec.workAttributes["batchLog"])
                tmpLog.debug(f"Done set_log_file after submission of workerID={workspec.workerID}")
                tmpRetVal = (True, "")
                worker_retval_map[workspec.workerID] = (tmpRetVal, workspec.get_changed_attributes())
        else:
            # failed
            tmpLog.debug(f"failed to submit workers to submissionHost={host} ; {ret_err_str}")
            for val in val_list:
                workspec = val[0]
                errStr = f"submission failed: {ret_err_str}"
                tmpLog.error(errStr)
                tmpRetVal = (None, errStr)
                worker_retval_map[workspec.workerID] = (tmpRetVal, workspec.get_changed_attributes())
    # make return list
    retValList = [worker_retval_map[w_id] for w_id in workerIDs_list]
    return retValList


def make_a_jdl(
    workspec,
    template,
    n_core_per_node,
    log_dir,
    panda_queue_name,
    executable_file,
    x509_user_proxy,
    log_subdir=None,
    ce_info_dict=dict(),
    batch_log_dict=dict(),
    pilot_url=None,
    pilot_args="",
    is_unified_dispatch=False,
    special_par="",
    harvester_queue_config=None,
    is_unified_queue=False,
    pilot_version="unknown",
    python_version="unknown",
    prod_rc_permille=0,
    token_dir=None,
    panda_token_filename=None,
    panda_token_dir=None,
    panda_token_key_path=None,
    is_gpu_resource=False,
    n_core_factor=1,
    custom_submit_attr_dict=None,
    **kwarg,
):
    """
    make a condor jdl for a worker
    """
    # make logger
    tmpLog = core_utils.make_logger(baseLogger, f"workerID={workspec.workerID} resourceType={workspec.resourceType}", method_name="make_a_jdl")
    # Note: In workspec, unit of minRamCount and of maxDiskCount are both MB.
    #       In HTCondor SDF, unit of request_memory is MB, and request_disk is KB.
    n_core_total = workspec.nCore if workspec.nCore else n_core_per_node
    request_ram = max(workspec.minRamCount, 1 * n_core_total) if workspec.minRamCount else 1 * n_core_total
    request_disk = workspec.maxDiskCount * 1024 if workspec.maxDiskCount else 1
    request_walltime = workspec.maxWalltime if workspec.maxWalltime else 0
    io_intensity = workspec.ioIntensity if workspec.ioIntensity else 0
    ce_info_dict = ce_info_dict.copy()
    batch_log_dict = batch_log_dict.copy()
    custom_submit_attr_dict = dict() if custom_submit_attr_dict is None else custom_submit_attr_dict.copy()
    # possible override by CRIC special_par
    if special_par:
        special_par_attr_list = [
            "queue",
            "maxWallTime",
            "xcount",
        ]
        _match_special_par_dict = {attr: re.search(f"\\({attr}=([^)]+)\\)", special_par) for attr in special_par_attr_list}
        for attr, _match in _match_special_par_dict.items():
            if not _match:
                continue
            elif attr == "queue":
                ce_info_dict["ce_queue_name"] = str(_match.group(1))
            elif attr == "maxWallTime":
                request_walltime = int(_match.group(1))
            elif attr == "xcount":
                n_core_total = int(_match.group(1))
            tmpLog.debug(f"job attributes override by CRIC special_par: {attr}={str(_match.group(1))}")
    # derived job attributes
    n_core_total_factor = n_core_total * n_core_factor
    n_node = ceil(n_core_total / n_core_per_node)
    request_ram_factor = request_ram * n_core_factor
    request_ram_bytes = request_ram * 2**20
    request_ram_bytes_factor = request_ram * 2**20 * n_core_factor
    request_ram_per_core = ceil(request_ram * n_node / n_core_total)
    request_ram_bytes_per_core = ceil(request_ram_bytes * n_node / n_core_total)
    request_cputime = request_walltime * n_core_total
    request_walltime_minute = ceil(request_walltime / 60)
    request_cputime_minute = ceil(request_cputime / 60)
    # decide prodSourceLabel
    pilot_opt_dict = submitter_common.get_complicated_pilot_options(
        pilot_type=workspec.pilotType,
        pilot_url=pilot_url,
        pilot_version=pilot_version,
        prod_source_label=harvester_queue_config.get_source_label(workspec.jobType),
        prod_rc_permille=prod_rc_permille,
    )
    prod_source_label = pilot_opt_dict["prod_source_label"]
    pilot_type_opt = pilot_opt_dict["pilot_type_opt"]
    pilot_url_str = pilot_opt_dict["pilot_url_str"]
    pilot_debug_str = pilot_opt_dict["pilot_debug_str"]
    tmpLog.debug(f"pilot options: {pilot_opt_dict}")
    # get token filename according to CE
    token_filename = None
    if token_dir is not None and ce_info_dict.get("ce_endpoint"):
        token_filename = endpoint_to_filename(ce_info_dict["ce_endpoint"])
    token_path = None
    if token_dir is not None and token_filename is not None:
        token_path = os.path.join(token_dir, token_filename)
    else:
        tmpLog.warning(f"token_path is None: site={panda_queue_name}, token_dir={token_dir} , token_filename={token_filename}")
    # get pilot-pandaserver token
    panda_token_path = None
    if panda_token_dir is not None and panda_token_filename is not None:
        panda_token_path = os.path.join(panda_token_dir, panda_token_filename)
    else:
        # tmpLog.warning(f"panda_token_path is None: panda_token_dir={panda_token_dir} , panda_token_filename={panda_token_filename}")
        pass
    # get panda token key
    panda_token_key_filename = None
    if panda_token_key_path is not None:
        panda_token_key_filename = os.path.basename(panda_token_key_path)
    # custom submit attributes (+key1 = value1 ; +key2 = value2 in JDL)
    custom_submit_attr_str_list = []
    for attr_key, attr_value in custom_submit_attr_dict.items():
        custom_submit_attr_str_list.append(f"+{attr_key} = {attr_value}")
    custom_submit_attr_str = "\n".join(custom_submit_attr_str_list)
    # open tmpfile as submit description file
    tmpFile = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix="_submit.sdf", dir=workspec.get_access_point())

    # instance of resource type mapper
    rt_mapper = ResourceTypeMapper()
    all_resource_types = rt_mapper.get_all_resource_types()

    # jobspec filename
    jobspec_filename = harvester_queue_config.messenger.get("jobSpecFileName", base_messenger.jobSpecFileName)

    # placeholder map
    placeholder_map = {
        "sdfPath": tmpFile.name,
        "executableFile": executable_file,
        "jobSpecFileName": jobspec_filename,
        "nCorePerNode": n_core_per_node,
        "nCoreTotal": n_core_total_factor,
        "nNode": n_node,
        "nCoreFactor": n_core_factor,
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
        "x509UserProxy": x509_user_proxy,
        "ceEndpoint": ce_info_dict.get("ce_endpoint", ""),
        "ceHostname": ce_info_dict.get("ce_hostname", ""),
        "ceFlavour": ce_info_dict.get("ce_flavour", ""),
        "ceJobmanager": ce_info_dict.get("ce_jobmanager", ""),
        "ceQueueName": ce_info_dict.get("ce_queue_name", ""),
        "ceVersion": ce_info_dict.get("ce_version", ""),
        "logDir": log_dir,
        "logSubdir": log_subdir,
        "prodSourceLabel": prod_source_label,
        "jobType": workspec.jobType,
        "resourceType": submitter_common.get_resource_type(workspec.resourceType, is_unified_queue, all_resource_types),
        "pilotResourceTypeOption": submitter_common.get_resource_type(workspec.resourceType, is_unified_queue, all_resource_types, is_pilot_option=True),
        "ioIntensity": io_intensity,
        "pilotType": pilot_type_opt,
        "pilotUrlOption": pilot_url_str,
        "pilotVersion": pilot_version,
        "pilotPythonOption": submitter_common.get_python_version_option(python_version, prod_source_label),
        "pilotDebugOption": pilot_debug_str,
        "pilotArgs": pilot_args,
        "submissionHost": workspec.submissionHost,
        "submissionHostShort": workspec.submissionHost.split(".")[0],
        "ceARCGridType": ce_info_dict.get("ce_grid_type", "arc"),
        "tokenDir": token_dir,
        "tokenFilename": token_filename,
        "tokenPath": token_path,
        "pandaTokenFilename": panda_token_filename,
        "pandaTokenPath": panda_token_path,
        "pandaTokenKeyFilename": panda_token_key_filename,
        "pandaTokenKeyPath": panda_token_key_path,
        "pilotJobLabel": submitter_common.get_joblabel(prod_source_label, is_unified_dispatch),
        "pilotJobType": submitter_common.get_pilot_job_type(workspec.jobType, is_unified_dispatch),
        "requestGpus": 1 if is_gpu_resource else 0,
        "requireGpus": is_gpu_resource,
        "customSubmitAttributes": custom_submit_attr_str,
    }

    gtag = batch_log_dict.get("gtag", "fake_GTAG_string").format(**placeholder_map)
    placeholder_map["gtag"] = gtag

    # fill in template string
    jdl_str = template.format(**placeholder_map)
    # save jdl to submit description file
    tmpFile.write(jdl_str)
    tmpFile.close()
    tmpLog.debug(f"saved sdf at {tmpFile.name}")
    tmpLog.debug("done")
    return jdl_str, placeholder_map


def parse_batch_job_filename(value_str, file_dir, batchID, guess=False):
    """
    parse log, stdout, stderr filename
    """
    _filename = os.path.basename(value_str)
    if guess:
        # guess file name before files really created; possibly containing condor macros
        return _filename
    else:
        _sanitized_list = re.sub("\{(\w+)\}|\[(\w+)\]|\((\w+)\)|#(\w+)#|\$", "", _filename).split(".")
        _prefix = _sanitized_list[0]
        _suffix = _sanitized_list[-1] if len(_sanitized_list) > 1 else ""

        for _f in os.listdir(file_dir):
            if re.match(f"{_prefix}(.*)\\.{batchID}\\.(.*)\\.{_suffix}", _f):
                return _f
        return None


# submitter for HTCondor batch system
class HTCondorSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        tmpLog = core_utils.make_logger(baseLogger, method_name="__init__")
        self.logBaseURL = None
        if hasattr(self, "useFQDN") and self.useFQDN:
            self.hostname = socket.getfqdn()
        else:
            self.hostname = socket.gethostname().split(".")[0]
        PluginBase.__init__(self, **kwarg)
        # extra plugin configs
        extra_plugin_configs = {}
        try:
            extra_plugin_configs = harvester_config.master.extraPluginConfigs["HTCondorSubmitter"]
        except AttributeError:
            pass
        except KeyError:
            pass
        # number of processes
        try:
            self.nProcesses
        except AttributeError:
            self.nProcesses = 1
        else:
            if (not self.nProcesses) or (self.nProcesses < 1):
                self.nProcesses = 1
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
        # executable file
        try:
            self.executableFile
        except AttributeError:
            self.executableFile = None
        # condor log directory
        try:
            self.logDir
            if "$hostname" in self.logDir or "${hostname}" in self.logDir:
                self.logDir = self.logDir.replace("$hostname", self.hostname).replace("${hostname}", self.hostname)
                try:
                    if not os.path.exists(self.logDir):
                        os.mkdir(self.logDir)
                except Exception as ex:
                    tmpLog.debug(f"Failed to create logDir({self.logDir}): {str(ex)}")
        except AttributeError:
            self.logDir = os.getenv("TMPDIR") or "/tmp"
        # log base url
        try:
            self.logBaseURL
            if "$hostname" in self.logBaseURL or "${hostname}" in self.logBaseURL:
                self.logBaseURL = self.logBaseURL.replace("$hostname", self.hostname).replace("${hostname}", self.hostname)
        except AttributeError:
            self.logBaseURL = None
        if self.logBaseURL and "${harvester_id}" in self.logBaseURL:
            self.logBaseURL = self.logBaseURL.replace("${harvester_id}", harvester_config.master.harvester_id)
        # Default x509 proxy for a queue
        try:
            self.x509UserProxy
        except AttributeError:
            self.x509UserProxy = os.getenv("X509_USER_PROXY")
        # x509 proxy for analysis jobs in grandly unified queues
        try:
            self.x509UserProxyAnalysis
        except AttributeError:
            self.x509UserProxyAnalysis = os.getenv("X509_USER_PROXY_ANAL")
        # Default token directory for a queue
        try:
            self.tokenDir
        except AttributeError:
            self.tokenDir = None
        # token directory for analysis jobs in grandly unified queues
        try:
            self.tokenDirAnalysis
        except AttributeError:
            self.tokenDirAnalysis = None
        # pilot-pandaserver token
        self.pandaTokenFilename = getattr(self, "pandaTokenFilename", None)
        self.pandaTokenDir = getattr(self, "pandaTokenDir", None)
        self.pandaTokenKeyPath = getattr(self, "pandaTokenKeyPath", None)
        # CRIC
        try:
            self.useCRIC = bool(self.useCRIC)
        except AttributeError:
            # Try the old parameter name useAtlasCRIC
            try:
                self.useCRIC = bool(self.useAtlasCRIC)
            except AttributeError:
                # Try the old parameter name useAtlasAGIS
                try:
                    self.useCRIC = bool(self.useAtlasAGIS)
                except AttributeError:
                    self.useCRIC = False
        # Grid CE, requiring CRIC
        try:
            self.useCRICGridCE = bool(self.useCRICGridCE)
        except AttributeError:
            # Try the old parameter name useAtlasGridCE
            try:
                self.useCRICGridCE = bool(self.useAtlasGridCE)
            except AttributeError:
                self.useCRICGridCE = False
        finally:
            self.useCRIC = self.useCRIC or self.useCRICGridCE
        # sdf template
        try:
            self.templateFile
        except AttributeError:
            self.templateFile = None
        # sdf template directories of CEs; ignored if templateFile is set
        try:
            self.CEtemplateDir
        except AttributeError:
            self.CEtemplateDir = ""
        # remote condor schedd and pool name (collector)
        try:
            self.condorSchedd
            if "$hostname" in self.condorSchedd or "${hostname}" in self.condorSchedd:
                self.condorSchedd = self.condorSchedd.replace("$hostname", self.hostname).replace("${hostname}", self.hostname)
        except AttributeError:
            self.condorSchedd = None
        try:
            self.condorPool
            if "$hostname" in self.condorPool or "${hostname}" in self.condorPool:
                self.condorPool = self.condorPool.replace("$hostname", self.hostname).replace("${hostname}", self.hostname)
        except AttributeError:
            self.condorPool = None
        # json config file of remote condor host: schedd/pool and weighting. If set, condorSchedd and condorPool are overwritten
        try:
            self.condorHostConfig
        except AttributeError:
            self.condorHostConfig = False
        if self.condorHostConfig:
            try:
                self.condorSchedd = []
                self.condorPool = []
                self.condorHostWeight = []
                with open(self.condorHostConfig, "r") as f:
                    condor_host_config_map = json.load(f)
                    for _schedd, _cm in condor_host_config_map.items():
                        _pool = _cm["pool"]
                        _weight = int(_cm["weight"])
                        self.condorSchedd.append(_schedd)
                        self.condorPool.append(_pool)
                        self.condorHostWeight.append(_weight)
            except Exception as e:
                tmpLog.error(f"error when parsing condorHostConfig json file; {e.__class__.__name__}: {e}")
                raise
        else:
            if isinstance(self.condorSchedd, list):
                self.condorHostWeight = [1] * len(self.condorSchedd)
            else:
                self.condorHostWeight = [1]
        # condor spool mechanism. If False, need shared FS across remote schedd
        try:
            self.useSpool
        except AttributeError:
            self.useSpool = False
        # number of workers less than this number will be bulkily submitted in only one schedd
        try:
            self.minBulkToRandomizedSchedd
        except AttributeError:
            self.minBulkToRandomizedSchedd = 20
        # try to use analysis credentials first
        try:
            self.useAnalysisCredentials
        except AttributeError:
            self.useAnalysisCredentials = False
        # probability permille to randomly run PR pilot with RC pilot url
        try:
            self.rcPilotRandomWeightPermille
        except AttributeError:
            self.rcPilotRandomWeightPermille = 0
        # submission to ARC CE's with nordugrid (gridftp) or arc (REST) grid type
        self.submit_arc_grid_type = "arc"
        if extra_plugin_configs.get("submit_arc_grid_type") == "nordugrid":
            self.submit_arc_grid_type = "nordugrid"
        # record of information of CE statistics
        self.ceStatsLock = threading.Lock()
        self.ceStats = dict()
        # allowed associated parameters and parameter prefixes from CRIC
        self._allowed_cric_attrs = [
            "pilot_url",
            "pilot_args",
            "unified_dispatch",
        ]
        self._allowed_cric_attr_prefixes = [
            "jdl.plusattr.",
        ]

    # get CE statistics of a site
    def get_ce_statistics(self, site_name, queue_config, n_new_workers, time_window=21600):
        if site_name in self.ceStats:
            return self.ceStats[site_name]
        with self.ceStatsLock:
            if site_name in self.ceStats:
                return self.ceStats[site_name]
            else:
                worker_limits_dict, _ = self.dbInterface.get_worker_limits(self.queueName, queue_config)
                worker_ce_stats_dict = self.dbInterface.get_worker_ce_stats(self.queueName)
                worker_ce_backend_throughput_dict = self.dbInterface.get_worker_ce_backend_throughput(self.queueName, time_window=time_window)
                return (worker_limits_dict, worker_ce_stats_dict, worker_ce_backend_throughput_dict, time_window, n_new_workers)

    # submit workers
    def submit_workers(self, workspec_list):
        tmpLog = self.make_logger(baseLogger, f"site={self.queueName}", method_name="submit_workers")

        nWorkers = len(workspec_list)
        tmpLog.debug(f"start nWorkers={nWorkers}")

        # whether to submit any worker
        to_submit_any = True

        # get log subdirectory name from timestamp
        timeNow = core_utils.naive_utcnow()
        log_subdir = timeNow.strftime("%y-%m-%d_%H")
        log_subdir_path = os.path.join(self.logDir, log_subdir)
        if self.condorSchedd is None or not self.useSpool:
            try:
                os.mkdir(log_subdir_path)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
                else:
                    pass

        # get info from harvester queue config
        _queueConfigMapper = QueueConfigMapper()
        harvester_queue_config = _queueConfigMapper.get_queue(self.queueName)

        # associated parameters dict
        associated_params_dict = {}

        is_grandly_unified_queue = False
        # get queue info from CRIC by cacher in db
        if self.useCRIC:
            panda_queues_dict = PandaQueuesDict()
            panda_queues_dict_last_refresh = core_utils.naive_utcfromtimestamp(panda_queues_dict.last_refresh_ts)
            tmpLog.debug(f"PandaQueuesDict last refresh at {panda_queues_dict_last_refresh}")
            panda_queue_name = panda_queues_dict.get_panda_queue_name(self.queueName)
            this_panda_queue_dict = panda_queues_dict.get(self.queueName, dict())
            is_grandly_unified_queue = panda_queues_dict.is_grandly_unified_queue(self.queueName)
            # tmpLog.debug('panda_queues_name and queue_info: {0}, {1}'.format(self.queueName, panda_queues_dict[self.queueName]))
            # associated params on CRIC
            for key, val in panda_queues_dict.get_harvester_params(self.queueName).items():
                if not isinstance(key, str):
                    continue
                if key in self._allowed_cric_attrs or any([key.startswith(the_prefix) for the_prefix in self._allowed_cric_attr_prefixes]):
                    if isinstance(val, str):
                        # sanitized list the value
                        val = re.sub(r"[;$~`]*", "", val)
                    associated_params_dict[key] = val
        else:
            panda_queues_dict = dict()
            panda_queue_name = self.queueName
            this_panda_queue_dict = dict()

        # get default information from queue info
        n_core_per_node_from_queue = this_panda_queue_dict.get("corecount", 1) if this_panda_queue_dict.get("corecount", 1) else 1
        is_unified_queue = this_panda_queue_dict.get("capability", "") == "ucore"
        is_unified_dispatch = associated_params_dict.get("unified_dispatch", False)
        pilot_url = associated_params_dict.get("pilot_url")
        pilot_args = associated_params_dict.get("pilot_args", "")
        pilot_version = str(this_panda_queue_dict.get("pilot_version", "current"))
        python_version = str(this_panda_queue_dict.get("python_version", "3"))
        is_gpu_resource = this_panda_queue_dict.get("resource_type", "") == "gpu"
        custom_submit_attr_dict = {}
        for k, v in associated_params_dict.items():
            # fill custom submit attributes for adding to JDL
            try:
                the_prefix = "jdl.plusattr."
                if k.startswith(the_prefix):
                    attr_key = k[len(the_prefix):]
                    attr_value = str(v)
                    if not re.fullmatch(r"[a-zA-Z_0-9][a-zA-Z_0-9.\-]*", attr_key):
                        # skip invalid key
                        continue
                    if not re.fullmatch(r"[a-zA-Z_0-9.\-,]+", attr_value):
                        # skip invalid value
                        continue
                    custom_submit_attr_dict[attr_key] = attr_value
            except Exception as e:
                tmpLog.warning(f'Got {e} with custom submit attributes "{k}: {v}"; skipped')
                continue

        # get override requirements from queue configured
        try:
            n_core_per_node = self.nCorePerNode if self.nCorePerNode else n_core_per_node_from_queue
        except AttributeError:
            n_core_per_node = n_core_per_node_from_queue

        # deal with Condor schedd and central managers; make a random list the choose
        n_bulks = ceil(nWorkers / self.minBulkToRandomizedSchedd)
        if isinstance(self.condorSchedd, list) and len(self.condorSchedd) > 0:
            orig_list = []
            if isinstance(self.condorPool, list) and len(self.condorPool) > 0:
                for _schedd, _pool, _weight in zip(self.condorSchedd, self.condorPool, self.condorHostWeight):
                    orig_list.extend([(_schedd, _pool)] * _weight)
            else:
                for _schedd, _weight in zip(self.condorSchedd, self.condorHostWeight):
                    orig_list.extend([(_schedd, self.condorPool)] * _weight)
            if n_bulks < len(orig_list):
                schedd_pool_choice_list = random.sample(orig_list, n_bulks)
            else:
                schedd_pool_choice_list = orig_list
        else:
            schedd_pool_choice_list = [(self.condorSchedd, self.condorPool)]

        # deal with CE
        special_par = ""
        ce_weighting = None
        if self.useCRICGridCE:
            # If CRIC Grid CE mode used
            tmpLog.debug("Using CRIC Grid CE mode...")
            queues_from_queue_list = this_panda_queue_dict.get("queues", [])
            special_par = this_panda_queue_dict.get("special_par", "")
            ce_auxiliary_dict = {}
            for _queue_dict in queues_from_queue_list:
                if not (
                    _queue_dict.get("ce_endpoint")
                    and str(_queue_dict.get("ce_state", "")).upper() == "ACTIVE"
                    and str(_queue_dict.get("ce_flavour", "")).lower() in set(["arc-ce", "cream-ce", "htcondor-ce"])
                ):
                    continue
                ce_info_dict = _queue_dict.copy()
                # ignore protocol prefix in ce_endpoint for cream and condor CE
                # check protocol prefix for ARC CE (gridftp or REST)
                _match_ce_endpoint = re.match("^(\w+)://(\w+)", ce_info_dict.get("ce_endpoint", ""))
                ce_endpoint_prefix = ""
                if _match_ce_endpoint:
                    ce_endpoint_prefix = _match_ce_endpoint.group(1)
                ce_endpoint_from_queue = re.sub("^\w+://", "", ce_info_dict.get("ce_endpoint", ""))
                ce_flavour_str = str(ce_info_dict.get("ce_flavour", "")).lower()
                ce_version_str = str(ce_info_dict.get("ce_version", "")).lower()
                # grid type of htcondor grid universe to use; empty string as default
                ce_info_dict["ce_grid_type"] = ""
                if ce_flavour_str == "arc-ce":
                    ce_info_dict["ce_grid_type"] = self.submit_arc_grid_type
                ce_info_dict["ce_hostname"] = re.sub(":\w*", "", ce_endpoint_from_queue)
                if ce_info_dict["ce_grid_type"] == "arc":
                    default_port = None
                    if ce_info_dict["ce_hostname"] == ce_endpoint_from_queue:
                        # default port
                        default_port = 443
                    else:
                        # change port 2811 to 443
                        ce_endpoint_from_queue = re.sub(r":2811$", ":443", ce_endpoint_from_queue)
                    ce_info_dict["ce_endpoint"] = f"{ce_endpoint_from_queue}{f':{default_port}' if default_port is not None else ''}"
                else:
                    if ce_info_dict["ce_hostname"] == ce_endpoint_from_queue:
                        # add default port to ce_endpoint if missing
                        default_port_map = {
                            "cream-ce": 8443,
                            "arc-ce": 2811,
                            "htcondor-ce": 9619,
                        }
                        if ce_flavour_str in default_port_map:
                            default_port = default_port_map[ce_flavour_str]
                            ce_info_dict["ce_endpoint"] = f"{ce_endpoint_from_queue}:{default_port}"
                    if ce_flavour_str == "arc-ce":
                        ce_info_dict["ce_endpoint"] = f"{ce_endpoint_from_queue}"
                tmpLog.debug(f'Got pilot version: "{pilot_version}"; CE endpoint: "{ce_endpoint_from_queue}", flavour: "{ce_flavour_str}"')
                ce_endpoint = ce_info_dict.get("ce_endpoint")
                if ce_endpoint in ce_auxiliary_dict and str(ce_info_dict.get("ce_queue_name", "")).lower() == "default":
                    pass
                else:
                    ce_auxiliary_dict[ce_endpoint] = ce_info_dict
            # qualified CEs from CRIC info
            n_qualified_ce = len(ce_auxiliary_dict)
            if n_qualified_ce > 0:
                # Get CE weighting
                tmpLog.debug("Get CE weighting")
                worker_ce_all_tuple = self.get_ce_statistics(self.queueName, harvester_queue_config, nWorkers)
                is_slave_queue = harvester_queue_config.runMode == "slave"
                ce_weighting = submitter_common.get_ce_weighting(
                    ce_endpoint_list=list(ce_auxiliary_dict.keys()), worker_ce_all_tuple=worker_ce_all_tuple, is_slave_queue=is_slave_queue
                )
                stats_weighting_display_str = submitter_common.get_ce_stats_weighting_display(ce_auxiliary_dict.keys(), worker_ce_all_tuple, ce_weighting)
                tmpLog.debug(f"CE stats and weighting: {stats_weighting_display_str}")
            else:
                tmpLog.error("No valid CE endpoint found")
                to_submit_any = False

        def _handle_one_worker(workspec, to_submit=to_submit_any):
            # make logger
            tmpLog = core_utils.make_logger(baseLogger, f"site={self.queueName} workerID={workspec.workerID}, resourceType={workspec.resourceType}", method_name="_handle_one_worker")

            def _choose_credential(workspec):
                """
                Choose the credential based on the job type
                """
                job_type = workspec.jobType
                proxy = self.x509UserProxy
                token_dir = self.tokenDir
                if (not is_unified_dispatch and is_grandly_unified_queue and job_type in ("user", "panda", "analysis")) or self.useAnalysisCredentials:
                    if self.x509UserProxyAnalysis:
                        tmpLog.debug("Taking analysis proxy")
                        proxy = self.x509UserProxyAnalysis
                    if self.tokenDirAnalysis:
                        tmpLog.debug("Taking analysis token_dir")
                        token_dir = self.tokenDirAnalysis
                else:
                    tmpLog.debug("Taking default proxy")
                    if self.tokenDir:
                        tmpLog.debug("Taking default token_dir")
                return proxy, token_dir

            def get_core_factor(workspec):
                try:
                    if type(self.nCoreFactor) in [dict]:
                        if workspec.jobType in self.nCoreFactor:
                            job_type = workspec.jobType
                        else:
                            job_type = 'Any'
                        if is_unified_queue:
                            resource_type = workspec.resourceType
                        else:
                            resource_type = 'Undefined'
                        n_core_factor = self.nCoreFactor.get(job_type, {}).get(resource_type, 1)
                        return int(n_core_factor)
                    else:
                        return int(self.nCoreFactor)
                except Exception as ex:
                    tmpLog.warning(f"Failed to get core factor: {ex}")
                return 1

            # initialize
            ce_info_dict = dict()
            batch_log_dict = dict()
            data = {
                "workspec": workspec,
                "to_submit": to_submit,
            }
            if to_submit:
                sdf_template_file = None
                if self.useCRICGridCE:
                    # choose a CE
                    tmpLog.info("choose a CE...")
                    ce_chosen = submitter_common.choose_ce(ce_weighting)
                    try:
                        ce_info_dict = ce_auxiliary_dict[ce_chosen].copy()
                    except KeyError:
                        tmpLog.info("Problem choosing CE with weighting. Choose an arbitrary CE endpoint")
                        ce_info_dict = random.choice(list(ce_auxiliary_dict.values())).copy()
                    ce_flavour_str = str(ce_info_dict.get("ce_flavour", "")).lower()
                    tmpLog.debug(f"Got pilot version: \"{pilot_version}\"; CE endpoint: \"{ce_info_dict['ce_endpoint']}\", flavour: \"{ce_flavour_str}\"")
                    if self.templateFile:
                        sdf_template_file = self.templateFile
                    elif os.path.isdir(self.CEtemplateDir) and ce_flavour_str:
                        sdf_suffix_str = ""
                        if ce_info_dict["ce_grid_type"] and ce_info_dict["ce_grid_type"] != "arc":
                            sdf_suffix_str = f"_{ce_info_dict['ce_grid_type']}"
                        sdf_template_filename = f"{ce_flavour_str}{sdf_suffix_str}.sdf"
                        sdf_template_file = os.path.join(self.CEtemplateDir, sdf_template_filename)
                else:
                    if self.templateFile:
                        sdf_template_file = self.templateFile
                    try:
                        # Manually define site condor schedd as ceHostname and central manager as ceEndpoint
                        if self.ceHostname and isinstance(self.ceHostname, list) and len(self.ceHostname) > 0:
                            if isinstance(self.ceEndpoint, list) and len(self.ceEndpoint) > 0:
                                ce_info_dict["ce_hostname"], ce_info_dict["ce_endpoint"] = random.choice(list(zip(self.ceHostname, self.ceEndpoint)))
                            else:
                                ce_info_dict["ce_hostname"] = random.choice(self.ceHostname)
                                ce_info_dict["ce_endpoint"] = self.ceEndpoint
                        else:
                            ce_info_dict["ce_hostname"] = self.ceHostname
                            ce_info_dict["ce_endpoint"] = self.ceEndpoint
                    except AttributeError:
                        pass
                    tmpLog.debug(f"Got pilot version: \"{pilot_version}\"; CE endpoint: \"{ce_info_dict.get('ce_endpoint')}\"")
                    try:
                        # Manually define ceQueueName
                        if self.ceQueueName:
                            ce_info_dict["ce_queue_name"] = self.ceQueueName
                    except AttributeError:
                        pass
                # template for batch script
                try:
                    tmpFile = open(sdf_template_file)
                    sdf_template_raw = tmpFile.read()
                    tmpFile.close()
                except AttributeError:
                    tmpLog.error("No valid templateFile found. Maybe templateFile, CEtemplateDir invalid, or no valid CE found")
                    to_submit = False
                    return data
                else:
                    # get batch_log, stdout, stderr filename, and remove commented lines
                    sdf_template_str_list = []
                    for _line in sdf_template_raw.split("\n"):
                        if _line.startswith("#"):
                            continue
                        sdf_template_str_list.append(_line)
                        _match_batch_log = re.match("log = (.+)", _line)
                        _match_stdout = re.match("output = (.+)", _line)
                        _match_stderr = re.match("error = (.+)", _line)
                        if _match_batch_log:
                            batch_log_value = _match_batch_log.group(1)
                            continue
                        if _match_stdout:
                            stdout_value = _match_stdout.group(1)
                            continue
                        if _match_stderr:
                            stderr_value = _match_stderr.group(1)
                            continue
                    sdf_template = "\n".join(sdf_template_str_list)
                    # Choose from Condor schedd and central managers
                    condor_schedd, condor_pool = random.choice(schedd_pool_choice_list)
                    # set submissionHost
                    if not condor_schedd and not condor_pool:
                        workspec.submissionHost = "LOCAL"
                    else:
                        workspec.submissionHost = f"{condor_schedd},{condor_pool}"
                    tmpLog.debug(f"set submissionHost={workspec.submissionHost}")
                    # Log Base URL
                    if self.logBaseURL and "[ScheddHostname]" in self.logBaseURL:
                        schedd_hostname = re.sub(
                            r"(?:[a-zA-Z0-9_.\-]*@)?([a-zA-Z0-9.\-]+)(?::[0-9]+)?",
                            lambda matchobj: matchobj.group(1) if matchobj.group(1) else "",
                            condor_schedd,
                        )
                        log_base_url = re.sub(r"\[ScheddHostname\]", schedd_hostname, self.logBaseURL)
                    else:
                        log_base_url = self.logBaseURL
                    # URLs for log files
                    if not (log_base_url is None):
                        if workspec.batchID:
                            batchID = workspec.batchID
                            guess = False
                        else:
                            batchID = ""
                            guess = True
                        batch_log_filename = parse_batch_job_filename(value_str=batch_log_value, file_dir=log_subdir_path, batchID=batchID, guess=guess)
                        stdout_path_file_name = parse_batch_job_filename(value_str=stdout_value, file_dir=log_subdir_path, batchID=batchID, guess=guess)
                        stderr_path_filename = parse_batch_job_filename(value_str=stderr_value, file_dir=log_subdir_path, batchID=batchID, guess=guess)
                        batch_log = f"{log_base_url}/{log_subdir}/{batch_log_filename}"
                        batch_stdout = f"{log_base_url}/{log_subdir}/{stdout_path_file_name}"
                        batch_stderr = f"{log_base_url}/{log_subdir}/{stderr_path_filename}"
                        workspec.set_log_file("batch_log", batch_log)
                        workspec.set_log_file("stdout", batch_stdout)
                        workspec.set_log_file("stderr", batch_stderr)
                        batch_log_dict["batch_log"] = batch_log
                        batch_log_dict["batch_stdout"] = batch_stdout
                        batch_log_dict["batch_stderr"] = batch_stderr
                        batch_log_dict["gtag"] = workspec.workAttributes["stdOut"]
                        tmpLog.debug("Done set_log_file before submission")
                    tmpLog.debug("Done jobspec attribute setting")

                # choose the x509 certificate based on the type of job (analysis or production)
                proxy, token_dir = _choose_credential(workspec)

                # set data dict
                data.update(
                    {
                        "workspec": workspec,
                        "to_submit": to_submit,
                        "template": sdf_template,
                        "executable_file": self.executableFile,
                        "log_dir": self.logDir,
                        "log_subdir": log_subdir,
                        "n_core_per_node": n_core_per_node,
                        "n_core_factor": get_core_factor(workspec),
                        "panda_queue_name": panda_queue_name,
                        "x509_user_proxy": proxy,
                        "ce_info_dict": ce_info_dict,
                        "batch_log_dict": batch_log_dict,
                        "special_par": special_par,
                        "harvester_queue_config": harvester_queue_config,
                        "is_unified_queue": is_unified_queue,
                        "condor_schedd": condor_schedd,
                        "condor_pool": condor_pool,
                        "use_spool": self.useSpool,
                        "pilot_url": pilot_url,
                        "pilot_args": pilot_args,
                        "pilot_version": pilot_version,
                        "python_version": python_version,
                        "token_dir": token_dir,
                        "panda_token_filename": self.pandaTokenFilename,
                        "panda_token_dir": self.pandaTokenDir,
                        "panda_token_key_path": self.pandaTokenKeyPath,
                        "is_unified_dispatch": is_unified_dispatch,
                        "prod_rc_permille": self.rcPilotRandomWeightPermille,
                        "is_gpu_resource": is_gpu_resource,
                        "custom_submit_attr_dict": custom_submit_attr_dict,
                    }
                )
            return data

        def _propagate_attributes(workspec, tmpVal):
            # make logger
            tmpLog = core_utils.make_logger(baseLogger, f"workerID={workspec.workerID}", method_name="_propagate_attributes")
            (retVal, tmpDict) = tmpVal
            workspec.set_attributes_with_dict(tmpDict)
            tmpLog.debug("Done workspec attributes propagation")
            return retVal

        tmpLog.debug("finished preparing worker attributes")

        # map(_handle_one_worker, workspec_list)
        with ThreadPoolExecutor(self.nProcesses * 4) as thread_pool:
            dataIterator = thread_pool.map(_handle_one_worker, workspec_list)
        tmpLog.debug(f"{nWorkers} workers handled")

        # submit
        retValList = submit_bag_of_workers(list(dataIterator))
        tmpLog.debug(f"{nWorkers} workers submitted")

        # propagate changed attributes
        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            retIterator = thread_pool.map(lambda _wv_tuple: _propagate_attributes(*_wv_tuple), zip(workspec_list, retValList))

        retList = list(retIterator)
        tmpLog.debug("done")

        return retList
