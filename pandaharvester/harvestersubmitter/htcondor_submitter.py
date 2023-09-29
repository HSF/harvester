import os
import errno
import datetime
import tempfile
import threading
import random
import json
import re
import socket

from concurrent.futures import ThreadPoolExecutor
from math import ceil

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestermisc.token_utils import endpoint_to_filename
from pandaharvester.harvestermisc.htcondor_utils import get_job_id_tuple_from_batchid
from pandaharvester.harvestermisc.htcondor_utils import CondorJobSubmit
from pandaharvester.harvestersubmitter import submitter_common

# logger
baseLogger = core_utils.setup_logger("htcondor_submitter")


# Replace condor Macro from SDF file, return string
def _condor_macro_replace(string, **kwarg):
    new_string = string
    macro_map = {
        "\$\(Cluster\)": str(kwarg["ClusterId"]),
        "\$\(Process\)": str(kwarg["ProcId"]),
    }
    for k, v in macro_map.items():
        new_string = re.sub(k, v, new_string)
    return new_string


# submit a bag of workers


def submit_bag_of_workers(data_list):
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
            errStr = "{0} not submitted due to incomplete data of the worker".format(workerID)
            tmpLog.warning(errStr)
            tmpRetVal = (None, errStr)
            # return tmpRetVal, workspec.get_changed_attributes()
            worker_retval_map[workerID] = (tmpRetVal, workspec.get_changed_attributes())
        # attributes
        try:
            use_spool = data["use_spool"]
        except KeyError:
            errStr = "{0} not submitted due to incomplete data of the worker".format(workerID)
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
        tmpLog.debug("submitting to submissionHost={0}".format(host))
        # submit
        try:
            condor_job_submit = CondorJobSubmit(id=host)
            batchIDs_list, ret_err_str = condor_job_submit.submit(jdl_list, use_spool=use_spool)
        except Exception as e:
            batchIDs_list = None
            ret_err_str = "Exception {0}: {1}".format(e.__class__.__name__, e)
        # result
        if batchIDs_list:
            # submitted
            n_workers = len(val_list)
            tmpLog.debug("submitted {0} workers to submissionHost={1}".format(n_workers, host))
            for val_i in range(n_workers):
                val = val_list[val_i]
                workspec = val[0]
                placeholder_map = val[2]
                # got batchID
                workspec.batchID = batchIDs_list[val_i]
                tmpLog.debug("workerID={0} submissionHost={1} batchID={2}".format(workspec.workerID, workspec.submissionHost, workspec.batchID))
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
                    batch_jdl = "{0}.jdl".format(batch_stderr[:-4])
                except Exception:
                    batch_jdl = None
                workspec.set_log_file("batch_log", batch_log)
                workspec.set_log_file("stdout", batch_stdout)
                workspec.set_log_file("stderr", batch_stderr)
                workspec.set_log_file("jdl", batch_jdl)
                if not workspec.get_jobspec_list():
                    tmpLog.debug("No jobspec associated in the worker of workerID={0}".format(workspec.workerID))
                else:
                    for jobSpec in workspec.get_jobspec_list():
                        # using batchLog and stdOut URL as pilotID and pilotLog
                        jobSpec.set_one_attribute("pilotID", workspec.workAttributes["stdOut"])
                        jobSpec.set_one_attribute("pilotLog", workspec.workAttributes["batchLog"])
                tmpLog.debug("Done set_log_file after submission of workerID={0}".format(workspec.workerID))
                tmpRetVal = (True, "")
                worker_retval_map[workspec.workerID] = (tmpRetVal, workspec.get_changed_attributes())
        else:
            # failed
            tmpLog.debug("failed to submit workers to submissionHost={0} ; {1}".format(host, ret_err_str))
            for val in val_list:
                workspec = val[0]
                errStr = "submission failed: {0}".format(ret_err_str)
                tmpLog.error(errStr)
                tmpRetVal = (None, errStr)
                worker_retval_map[workspec.workerID] = (tmpRetVal, workspec.get_changed_attributes())
    # make return list
    retValList = [worker_retval_map[w_id] for w_id in workerIDs_list]
    return retValList


# make a condor jdl for a worker


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
    is_gpu_resource=False,
    **kwarg
):
    # make logger
    tmpLog = core_utils.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="make_a_jdl")
    # Note: In workspec, unit of minRamCount and of maxDiskCount are both MB.
    #       In HTCondor SDF, unit of request_memory is MB, and request_disk is KB.
    n_core_total = workspec.nCore if workspec.nCore else n_core_per_node
    request_ram = max(workspec.minRamCount, 1 * n_core_total) if workspec.minRamCount else 1 * n_core_total
    request_disk = workspec.maxDiskCount * 1024 if workspec.maxDiskCount else 1
    request_walltime = workspec.maxWalltime if workspec.maxWalltime else 0
    io_intensity = workspec.ioIntensity if workspec.ioIntensity else 0
    ce_info_dict = ce_info_dict.copy()
    batch_log_dict = batch_log_dict.copy()
    # possible override by CRIC special_par
    if special_par:
        special_par_attr_list = [
            "queue",
            "maxWallTime",
            "xcount",
        ]
        _match_special_par_dict = {attr: re.search("\({attr}=([^)]+)\)".format(attr=attr), special_par) for attr in special_par_attr_list}
        for attr, _match in _match_special_par_dict.items():
            if not _match:
                continue
            elif attr == "queue":
                ce_info_dict["ce_queue_name"] = str(_match.group(1))
            elif attr == "maxWallTime":
                request_walltime = int(_match.group(1))
            elif attr == "xcount":
                n_core_total = int(_match.group(1))
            tmpLog.debug("job attributes override by CRIC special_par: {0}={1}".format(attr, str(_match.group(1))))
    # derived job attributes
    n_node = ceil(n_core_total / n_core_per_node)
    request_ram_bytes = request_ram * 2**20
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
    # get token filename according to CE
    token_filename = None
    if token_dir is not None and ce_info_dict.get("ce_endpoint"):
        token_filename = endpoint_to_filename(ce_info_dict["ce_endpoint"])
    token_path = None
    if token_dir is not None and token_filename is not None:
        token_path = os.path.join(token_dir, token_filename)
    else:
        tmpLog.warning("token_path is None: site={0}, token_dir={1} , token_filename={2}".format(panda_queue_name, token_dir, token_filename))
    # open tmpfile as submit description file
    tmpFile = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix="_submit.sdf", dir=workspec.get_access_point())
    # placeholder map
    placeholder_map = {
        "sdfPath": tmpFile.name,
        "executableFile": executable_file,
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
        "x509UserProxy": x509_user_proxy,
        "ceEndpoint": ce_info_dict.get("ce_endpoint", ""),
        "ceHostname": ce_info_dict.get("ce_hostname", ""),
        "ceFlavour": ce_info_dict.get("ce_flavour", ""),
        "ceJobmanager": ce_info_dict.get("ce_jobmanager", ""),
        "ceQueueName": ce_info_dict.get("ce_queue_name", ""),
        "ceVersion": ce_info_dict.get("ce_version", ""),
        "logDir": log_dir,
        "logSubdir": log_subdir,
        "gtag": batch_log_dict.get("gtag", "fake_GTAG_string"),
        "prodSourceLabel": prod_source_label,
        "jobType": workspec.jobType,
        "resourceType": submitter_common.get_resource_type(workspec.resourceType, is_unified_queue),
        "pilotResourceTypeOption": submitter_common.get_resource_type(workspec.resourceType, is_unified_queue, True),
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
        "pilotJobLabel": submitter_common.get_joblabel(prod_source_label, is_unified_dispatch),
        "pilotJobType": submitter_common.get_pilot_job_type(workspec.jobType, is_unified_dispatch),
        "requestGpus": 1 if is_gpu_resource else 0,
        "requireGpus": is_gpu_resource,
    }
    # fill in template string
    jdl_str = template.format(**placeholder_map)
    # save jdl to submit description file
    tmpFile.write(jdl_str)
    tmpFile.close()
    tmpLog.debug("saved sdf at {0}".format(tmpFile.name))
    tmpLog.debug("done")
    return jdl_str, placeholder_map


# parse log, stdout, stderr filename


def parse_batch_job_filename(value_str, file_dir, batchID, guess=False):
    _filename = os.path.basename(value_str)
    if guess:
        # guess file name before files really created; possibly containing condor macros
        return _filename
    else:
        _sanitized_list = re.sub("\{(\w+)\}|\[(\w+)\]|\((\w+)\)|#(\w+)#|\$", "", _filename).split(".")
        _prefix = _sanitized_list[0]
        _suffix = _sanitized_list[-1] if len(_sanitized_list) > 1 else ""

        for _f in os.listdir(file_dir):
            if re.match("{prefix}(.*)\.{batchID}\.(.*)\.{suffix}".format(prefix=_prefix, suffix=_suffix, batchID=batchID), _f):
                return _f
        return None


# submitter for HTCONDOR batch system
class HTCondorSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        tmpLog = core_utils.make_logger(baseLogger, method_name="__init__")
        self.logBaseURL = None
        self.templateFile = None
        if hasattr(self, "useFQDN") and self.useFQDN:
            self.hostname = socket.getfqdn()
        else:
            self.hostname = socket.gethostname().split(".")[0]
        PluginBase.__init__(self, **kwarg)
        # number of processes
        try:
            self.nProcesses
        except AttributeError:
            self.nProcesses = 1
        else:
            if (not self.nProcesses) or (self.nProcesses < 1):
                self.nProcesses = 1
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
                    tmpLog.debug("Failed to create logDir(%s): %s" % (self.logDir, str(ex)))
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
                tmpLog.error("error when parsing condorHostConfig json file; {0}: {1}".format(e.__class__.__name__, e))
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
            self.minBulkToRamdomizedSchedd
        except AttributeError:
            self.minBulkToRamdomizedSchedd = 20
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
        try:
            extra_plugin_configs = harvester_config.master.extraPluginConfigs["HTCondorSubmitter"]
        except AttributeError:
            pass
        except KeyError:
            pass
        else:
            if extra_plugin_configs.get("submit_arc_grid_type") == "nordugrid":
                self.submit_arc_grid_type = "nordugrid"
        # record of information of CE statistics
        self.ceStatsLock = threading.Lock()
        self.ceStats = dict()
        # allowed associated parameters from CRIC
        self._allowed_cric_attrs = (
            "pilot_url",
            "pilot_args",
            "unified_dispatch",
        )

    # get CE statistics of a site
    def get_ce_statistics(self, site_name, n_new_workers, time_window=21600):
        if site_name in self.ceStats:
            return self.ceStats[site_name]
        with self.ceStatsLock:
            if site_name in self.ceStats:
                return self.ceStats[site_name]
            else:
                worker_limits_dict = self.dbInterface.get_worker_limits(self.queueName)
                worker_ce_stats_dict = self.dbInterface.get_worker_ce_stats(self.queueName)
                worker_ce_backend_throughput_dict = self.dbInterface.get_worker_ce_backend_throughput(self.queueName, time_window=time_window)
                return (worker_limits_dict, worker_ce_stats_dict, worker_ce_backend_throughput_dict, time_window, n_new_workers)

    # submit workers
    def submit_workers(self, workspec_list):
        tmpLog = self.make_logger(baseLogger, "site={0}".format(self.queueName), method_name="submit_workers")

        nWorkers = len(workspec_list)
        tmpLog.debug("start nWorkers={0}".format(nWorkers))

        # whether to submit any worker
        to_submit_any = True

        # get log subdirectory name from timestamp
        timeNow = datetime.datetime.utcnow()
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
            panda_queue_name = panda_queues_dict.get_panda_queue_name(self.queueName)
            this_panda_queue_dict = panda_queues_dict.get(self.queueName, dict())
            is_grandly_unified_queue = panda_queues_dict.is_grandly_unified_queue(self.queueName)
            # tmpLog.debug('panda_queues_name and queue_info: {0}, {1}'.format(self.queueName, panda_queues_dict[self.queueName]))
            # associated params on CRIC
            for key, val in panda_queues_dict.get_harvester_params(self.queueName).items():
                if key in self._allowed_cric_attrs:
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
        python_version = str(this_panda_queue_dict.get("python_version", "2"))
        is_gpu_resource = this_panda_queue_dict.get("resource_type", "") == "gpu"

        # get override requirements from queue configured
        try:
            n_core_per_node = self.nCorePerNode if self.nCorePerNode else n_core_per_node_from_queue
        except AttributeError:
            n_core_per_node = n_core_per_node_from_queue

        # deal with Condor schedd and central managers; make a random list the choose
        n_bulks = ceil(nWorkers / self.minBulkToRamdomizedSchedd)
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
            ce_auxilary_dict = {}
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
                        # defaut port
                        default_port = 443
                    else:
                        # change port 2811 to 443
                        ce_endpoint_from_queue = re.sub(r":2811$", ":443", ce_endpoint_from_queue)
                    ce_info_dict["ce_endpoint"] = "{0}{1}".format(ce_endpoint_from_queue, ":{0}".format(default_port) if default_port is not None else "")
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
                            ce_info_dict["ce_endpoint"] = "{0}:{1}".format(ce_endpoint_from_queue, default_port)
                    if ce_flavour_str == "arc-ce":
                        ce_info_dict["ce_endpoint"] = "{0}".format(ce_endpoint_from_queue)
                tmpLog.debug('Got pilot version: "{0}"; CE endpoint: "{1}", flavour: "{2}"'.format(pilot_version, ce_endpoint_from_queue, ce_flavour_str))
                ce_endpoint = ce_info_dict.get("ce_endpoint")
                if ce_endpoint in ce_auxilary_dict and str(ce_info_dict.get("ce_queue_name", "")).lower() == "default":
                    pass
                else:
                    ce_auxilary_dict[ce_endpoint] = ce_info_dict
            # qualified CEs from CRIC info
            n_qualified_ce = len(ce_auxilary_dict)
            if n_qualified_ce > 0:
                # Get CE weighting
                tmpLog.debug("Get CE weighting")
                worker_ce_all_tuple = self.get_ce_statistics(self.queueName, nWorkers)
                is_slave_queue = harvester_queue_config.runMode == "slave"
                ce_weighting = submitter_common.get_ce_weighting(
                    ce_endpoint_list=list(ce_auxilary_dict.keys()), worker_ce_all_tuple=worker_ce_all_tuple, is_slave_queue=is_slave_queue
                )
                stats_weighting_display_str = submitter_common.get_ce_stats_weighting_display(ce_auxilary_dict.keys(), worker_ce_all_tuple, ce_weighting)
                tmpLog.debug("CE stats and weighting: {0}".format(stats_weighting_display_str))
            else:
                tmpLog.error("No valid CE endpoint found")
                to_submit_any = False

        def _handle_one_worker(workspec, to_submit=to_submit_any):
            # make logger
            tmpLog = core_utils.make_logger(baseLogger, "site={0} workerID={1}".format(self.queueName, workspec.workerID), method_name="_handle_one_worker")

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
                        ce_info_dict = ce_auxilary_dict[ce_chosen].copy()
                    except KeyError:
                        tmpLog.info("Problem choosing CE with weighting. Choose an arbitrary CE endpoint")
                        ce_info_dict = random.choice(list(ce_auxilary_dict.values())).copy()
                    ce_flavour_str = str(ce_info_dict.get("ce_flavour", "")).lower()
                    tmpLog.debug(
                        'Got pilot version: "{0}"; CE endpoint: "{1}", flavour: "{2}"'.format(pilot_version, ce_info_dict["ce_endpoint"], ce_flavour_str)
                    )
                    if self.templateFile:
                        sdf_template_file = self.templateFile
                    elif os.path.isdir(self.CEtemplateDir) and ce_flavour_str:
                        sdf_suffix_str = ""
                        if ce_info_dict["ce_grid_type"]:
                            sdf_suffix_str = "_{ce_grid_type}".format(ce_grid_type=ce_info_dict["ce_grid_type"])
                        sdf_template_filename = "{ce_flavour_str}{sdf_suffix_str}.sdf".format(ce_flavour_str=ce_flavour_str, sdf_suffix_str=sdf_suffix_str)
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
                    # get batch_log, stdout, stderr filename, and remobe commented liness
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
                        workspec.submissionHost = "{0},{1}".format(condor_schedd, condor_pool)
                    tmpLog.debug("set submissionHost={0}".format(workspec.submissionHost))
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
                        batch_log = "{0}/{1}/{2}".format(log_base_url, log_subdir, batch_log_filename)
                        batch_stdout = "{0}/{1}/{2}".format(log_base_url, log_subdir, stdout_path_file_name)
                        batch_stderr = "{0}/{1}/{2}".format(log_base_url, log_subdir, stderr_path_filename)
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
                        "is_unified_dispatch": is_unified_dispatch,
                        "prod_rc_permille": self.rcPilotRandomWeightPermille,
                        "is_gpu_resource": is_gpu_resource,
                    }
                )
            return data

        def _propagate_attributes(workspec, tmpVal):
            # make logger
            tmpLog = core_utils.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="_propagate_attributes")
            (retVal, tmpDict) = tmpVal
            workspec.set_attributes_with_dict(tmpDict)
            tmpLog.debug("Done workspec attributes propagation")
            return retVal

        tmpLog.debug("finished preparing worker attributes")

        # map(_handle_one_worker, workspec_list)
        with ThreadPoolExecutor(self.nProcesses * 4) as thread_pool:
            dataIterator = thread_pool.map(_handle_one_worker, workspec_list)
        tmpLog.debug("{0} workers handled".format(nWorkers))

        # submit
        retValList = submit_bag_of_workers(list(dataIterator))
        tmpLog.debug("{0} workers submitted".format(nWorkers))

        # propagate changed attributes
        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            retIterator = thread_pool.map(lambda _wv_tuple: _propagate_attributes(*_wv_tuple), zip(workspec_list, retValList))

        retList = list(retIterator)
        tmpLog.debug("done")

        return retList
