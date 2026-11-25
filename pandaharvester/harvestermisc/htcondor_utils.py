# === Imports ===================================================

import datetime
import functools
import multiprocessing
import os
import re
import socket
import subprocess
import tempfile
import threading
import time
import traceback
import xml.etree.ElementTree as ET
from threading import get_ident

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.core_utils import SingletonWithID
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy

# === Condor ====================================================

CONDOR_API_TYPE = "python"
CONDOR_API_VERSION = 1

try:
    # try to import htcondor version 2 for htcondor version >= 25
    import htcondor2 as htcondor

    CONDOR_API_VERSION = 2
except ImportError:
    import htcondor


# ===============================================================

# === Definitions ===============================================

# logger
baseLogger = core_utils.setup_logger("htcondor_utils")

# module level lock
moduleLock = threading.Lock()

# List of job ads required
CONDOR_JOB_ADS_LIST = [
    "ClusterId",
    "ProcId",
    "JobStatus",
    "LastJobStatus",
    "JobStartDate",
    "EnteredCurrentStatus",
    "ExitCode",
    "HoldReason",
    "LastHoldReason",
    "RemoveReason",
    "harvesterWorkerID",
]

# harvesterID
harvesterID = harvester_config.master.harvester_id

# minimum interval in sec between forced refreshes of the condor job cache of a submission host
_cache_force_refresh_min_interval = 60

# ===============================================================

# === Functions =================================================


def synchronize(func):
    """
    synchronize decorator
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with moduleLock:
            return func(*args, **kwargs)

    return wrapper


def _runShell(cmd):
    """
    Run shell function
    """
    cmd = str(cmd)
    p = subprocess.Popen(cmd.split(), shell=False, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdOut, stdErr = p.communicate()
    retCode = p.returncode
    return (retCode, stdOut, stdErr)


def condor_job_id_from_workspec(workspec):
    """
    Generate condor job id with schedd host from workspec
    """
    batchid_str = str(workspec.batchID)
    # backward compatibility if workspec.batchID does not contain ProcId
    if "." not in batchid_str:
        batchid_str += ".0"
    return f"{workspec.submissionHost}#{batchid_str}"


def get_host_batchid_map(workspec_list):
    """
    Get a dictionary of submissionHost: list of batchIDs from workspec_list
    return {submissionHost_1: {batchID_1_1, ...}, submissionHost_2: {...}, ...}
    """
    host_batchid_map = {}
    for workspec in workspec_list:
        host = workspec.submissionHost
        batchid = workspec.batchID
        if batchid is None:
            continue
        batchid_str = str(batchid)
        # backward compatibility if workspec.batchID does not contain ProcId
        if "." not in batchid_str:
            batchid_str += ".0"
        host_batchid_map.setdefault(host, {})
        host_batchid_map[host][batchid_str] = workspec
    return host_batchid_map


def get_batchid_from_job(job_ads_dict):
    """
    Get batchID string from condor job dict
    """
    batchid = f"{job_ads_dict['ClusterId']}.{job_ads_dict['ProcId']}"
    return batchid


def get_job_id_tuple_from_batchid(batchid):
    """
    Get tuple (ClusterId, ProcId) from batchID string
    """
    batchid_str_list = str(batchid).split(".")
    clusterid = batchid_str_list[0]
    procid = batchid_str_list[1]
    if not procid:
        procid = 0
    return (clusterid, procid)


# def jdl_to_map(jdl):
#     """
#     Transform jdl into dictionary
#     The "queue" line (e.g. "queue 1") will be omitted
#     """
#     # FIXME: not containing "+"
#     ret_map = {}
#     for line in jdl.split('\n'):
#         match = re.search('^(.+) = (.+)$', line)
#         if match:
#             ret_map[match(1)] = match(2)
#     return ret_map


def condor_submit_process(mp_queue, host, jdl_map_list, tmp_log):
    """
    Function for new process to submit condor
    """
    # initialization
    errStr = ""
    batchIDs_list = []
    # parse schedd and pool name
    condor_schedd, condor_pool = None, None
    if host in ("LOCAL", "None"):
        tmp_log.debug(f"submissionHost is {host}, treated as local schedd. Skipped")
    else:
        try:
            condor_schedd, condor_pool = host.split(",")[0:2]
        except ValueError:
            tmp_log.error(f"Invalid submissionHost: {host} . Skipped")
    # get schedd
    try:
        if condor_pool:
            collector = htcondor.Collector(condor_pool)
        else:
            collector = htcondor.Collector()
        if condor_schedd:
            scheddAd = collector.locate(htcondor.DaemonTypes.Schedd, condor_schedd)
        else:
            scheddAd = collector.locate(htcondor.DaemonTypes.Schedd)
        schedd = htcondor.Schedd(scheddAd)
    except Exception as e:
        errStr = f"create condor collector and schedd failed; {e.__class__.__name__}: {e}"
    else:
        submit_obj = htcondor.Submit()
        try:
            with schedd.transaction() as txn:
                # TODO: Currently spool is not supported in htcondor.Submit ...
                submit_result = submit_obj.queue_with_itemdata(txn, 1, iter(jdl_map_list))
                clusterid = submit_result.cluster()
                first_proc = submit_result.first_proc()
                num_proc = submit_result.num_procs()
                batchIDs_list.extend([f"{clusterid}.{procid}" for procid in range(first_proc, first_proc + num_proc)])
        except RuntimeError as e:
            errStr = f"submission failed; {e.__class__.__name__}: {e}"
    mp_queue.put((batchIDs_list, errStr))


# ===============================================================

# === Classes ===================================================


# Condor client
class CondorClient(object):
    @classmethod
    def renew_session_and_retry(cls, func):
        """
        If RuntimeError, call renew_session and retry
        """
        # FIXME: currently hard-coded
        to_retry = True
        # Wrapper

        def wrapper(self, *args, **kwargs):
            # Make logger
            tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorClient.renew_session_if_error")
            func_name = func.__name__
            try:
                self.schedd
            except AttributeError as e:
                if self.lock.acquire(False):
                    is_renewed = self.renew_session()
                    self.lock.release()
                    if not is_renewed:
                        errStr = f"failed to communicate with {self.submissionHost}"
                        tmpLog.error(errStr)
                        tmpLog.debug(f"got RuntimeError: {e}")
                        raise Exception(errStr)
            try:
                ret = func(self, *args, **kwargs)
            except RuntimeError as e:
                tmpLog.debug(f"got RuntimeError: {e}")
                if self.lock.acquire(False):
                    is_renewed = self.renew_session()
                    self.lock.release()
                    if is_renewed:
                        if to_retry:
                            tmpLog.debug(f"condor session renewed. Retrying {func_name}")
                            ret = func(self, *args, **kwargs)
                        else:
                            tmpLog.debug("condor session renewed")
                            raise
                    else:
                        tmpLog.error("failed to renew condor session")
                        raise
                else:
                    tmpLog.debug("another thread is renewing condor session; skipped...")
                    raise
                tmpLog.debug("done")
            return ret

        return wrapper

    def __init__(self, submissionHost, *args, **kwargs):
        self.submissionHost = submissionHost
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorClient.__init__")
        # Initialize
        tmpLog.debug("Initializing client")
        self.lock = threading.Lock()
        self.condor_api_type = CONDOR_API_TYPE
        self.condor_schedd = None
        self.condor_pool = None
        # Parse condor command remote options from workspec
        if self.submissionHost in ("LOCAL", "None"):
            tmpLog.debug(f"submissionHost is {self.submissionHost}, treated as local schedd. Skipped")
        else:
            try:
                self.condor_schedd, self.condor_pool = self.submissionHost.split(",")[0:2]
                if self.condor_schedd in ["None"]:
                    self.condor_schedd = None
                if self.condor_pool in ["None"]:
                    self.condor_pool = None
            except ValueError:
                tmpLog.error(f"Invalid submissionHost: {self.submissionHost} . Skipped")
        # Use Python API or fall back to command
        if self.condor_api_type == "python":
            try:
                if CONDOR_API_VERSION == 1:
                    self.secman = htcondor.SecMan()
                self.renew_session(init=True)
            except Exception as e:
                tmpLog.error(f"Error when using htcondor Python API. Exception {e.__class__.__name__}: {e}")
                raise
        tmpLog.debug("Initialized client")

    @synchronize
    def renew_session(self, retry=3, init=False):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorClient.renew_session")
        # Clear security session if not initialization
        if not init:
            tmpLog.info("Renew condor session")
            if CONDOR_API_VERSION == 1:
                self.secman.invalidateAllSessions()
        # Recreate collector and schedd object
        i_try = 1
        while i_try <= retry:
            try:
                tmpLog.info(f"Try {i_try}")
                if self.condor_pool:
                    self.collector = htcondor.Collector(self.condor_pool)
                else:
                    self.collector = htcondor.Collector()
                if self.condor_schedd:
                    self.scheddAd = self.collector.locate(htcondor.DaemonTypes.Schedd, self.condor_schedd)
                else:
                    self.scheddAd = self.collector.locate(htcondor.DaemonTypes.Schedd)
                self.schedd = htcondor.Schedd(self.scheddAd)
                tmpLog.info("Success")
                break
            except Exception as e:
                tmpLog.warning(f"Recreate condor collector and schedd failed: {e}")
                if i_try < retry:
                    tmpLog.warning("Failed. Retry...")
                else:
                    tmpLog.warning(f"Retry {i_try} times. Still failed. Skipped")
                    return False
                i_try += 1
                if CONDOR_API_VERSION == 1:
                    self.secman.invalidateAllSessions()
                time.sleep(3)
        # Sleep
        time.sleep(3)
        return True


# Condor job query
class CondorJobQuery(CondorClient, metaclass=SingletonWithID):
    # class lock
    classLock = threading.Lock()
    # Query commands
    orig_comStr_list = [
        "condor_q -xml",
        "condor_history -xml",
    ]
    # Bad text of redundant xml roots to eleminate from condor XML
    badtext = """
</classads>

<?xml version="1.0"?>
<!DOCTYPE classads SYSTEM "classads.dtd">
<classads>
"""

    def __init__(self, cacheEnable=False, cacheRefreshInterval=None, useCondorHistory=True, useCondorHistoryMaxAge=7200, *args, **kwargs):
        self.submissionHost = str(kwargs.get("id"))
        # Make logger
        tmpLog = core_utils.make_logger(
            baseLogger, f"submissionHost={self.submissionHost} thrid={get_ident()} oid={id(self)}", method_name="CondorJobQuery.__init__"
        )
        # Initialize
        with self.classLock:
            tmpLog.debug("Start")
            CondorClient.__init__(self, self.submissionHost, *args, **kwargs)
            # For condor_q cache
            self.cacheEnable = cacheEnable
            if self.cacheEnable:
                self.cacheRefreshInterval = cacheRefreshInterval
                self.dbProxy = DBProxy()
            self.useCondorHistory = useCondorHistory
            self.useCondorHistoryMaxAge = useCondorHistoryMaxAge
            tmpLog.debug("Initialize done")

    def get_all(self, batchIDs_dict=None, allJobs=False, to_update_cache=False):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobQuery.get_all")
        # Get all
        tmpLog.debug("Start")
        job_ads_all_dict = {}
        if self.condor_api_type == "python":
            try:
                job_ads_all_dict = self.query_with_python(batchIDs_dict, allJobs, to_update_cache)
            except Exception as e:
                tb_str = traceback.format_exc()
                tmpLog.error(f"Exception {e.__class__.__name__}: {e} ; {tb_str}")
                raise
        else:
            job_ads_all_dict = self.query_with_command(batchIDs_dict)
        return job_ads_all_dict

    def query_with_command(self, batchIDs_dict=None):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobQuery.query_with_command")
        # Start query
        tmpLog.debug("Start query")
        if batchIDs_dict is None:
            batchIDs_dict = {}
        job_ads_all_dict = {}
        batchIDs_set = set(batchIDs_dict.keys())
        for orig_comStr in self.orig_comStr_list:
            # String of batchIDs
            batchIDs_str = " ".join(list(batchIDs_set))
            # Command
            if "condor_q" in orig_comStr or ("condor_history" in orig_comStr and batchIDs_set):
                name_opt = f"-name {self.condor_schedd}" if self.condor_schedd else ""
                pool_opt = f"-pool {self.condor_pool}" if self.condor_pool else ""
                ids = batchIDs_str
                comStr = f"{orig_comStr} {name_opt} {pool_opt} {ids}"
            else:
                # tmpLog.debug('No batch job left to query in this cycle by this thread')
                continue
            tmpLog.debug(f"check with {comStr}")
            (retCode, stdOut, stdErr) = _runShell(comStr)
            if retCode == 0:
                # Command succeeded
                job_ads_xml_str = "\n".join(str(stdOut).split(self.badtext))
                if "<c>" in job_ads_xml_str:
                    # Found at least one job
                    # XML parsing
                    xml_root = ET.fromstring(job_ads_xml_str)

                    def _getAttribute_tuple(attribute_xml_element):
                        # Attribute name
                        _n = str(attribute_xml_element.get("n"))
                        # Attribute value text
                        _t = " ".join(attribute_xml_element.itertext())
                        return (_n, _t)

                    # Every batch job
                    for _c in xml_root.findall("c"):
                        job_ads_dict = dict()
                        # Every attribute
                        attribute_iter = map(_getAttribute_tuple, _c.findall("a"))
                        job_ads_dict.update(attribute_iter)
                        batchid = get_batchid_from_job(job_ads_dict)
                        condor_job_id = f"{self.submissionHost}#{batchid}"
                        job_ads_all_dict[condor_job_id] = job_ads_dict
                        # Remove batch jobs already gotten from the list
                        if batchid in batchIDs_set:
                            batchIDs_set.discard(batchid)
                else:
                    # Job not found
                    tmpLog.debug(f"job not found with {comStr}")
                    continue
            else:
                # Command failed
                errStr = f'command "{comStr}" failed, retCode={retCode}, error: {stdOut} {stdErr}'
                tmpLog.error(errStr)
                raise RuntimeError(errStr)
        if len(batchIDs_set) > 0:
            # Job unfound via both condor_q or condor_history, marked as unknown worker in harvester
            for batchid in batchIDs_set:
                condor_job_id = f"{self.submissionHost}#{batchid}"
                job_ads_all_dict[condor_job_id] = dict()
            tmpLog.info(f"Unfound batch jobs of submissionHost={self.submissionHost}: {' '.join(list(batchIDs_set))}")
        # Return
        return job_ads_all_dict

    @CondorClient.renew_session_and_retry
    def query_with_python(self, batchIDs_dict=None, allJobs=False, to_update_cache=False):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobQuery.query_with_python")
        # Start query
        tmpLog.debug("Start query")
        job_ads_all_dict = {}
        # make id sets
        if batchIDs_dict is None:
            batchIDs_dict = {}
        batchIDs_set = set(batchIDs_dict.keys())
        clusterids_set = set([get_job_id_tuple_from_batchid(batchid)[0] for batchid in batchIDs_dict])
        # query from the cache shared in DB

        def cache_query(constraint=None, projection=CONDOR_JOB_ADS_LIST):
            return self.query_from_cache(batchIDs_set, allJobs=allJobs, to_update_cache=to_update_cache)

        # query method options
        query_method_list = [self.schedd.query]
        if self.cacheEnable:
            query_method_list.insert(0, cache_query)
        if self.useCondorHistory:
            query_method_list.append(self.schedd.history)
        # Go
        for query_method in query_method_list:
            # exclude already checked and outdated jobs before querying with condor history
            if query_method.__name__ == self.schedd.history.__name__:
                now_time = core_utils.naive_utcnow()
                update_time_threshold = now_time - datetime.timedelta(seconds=self.useCondorHistoryMaxAge)
                clusterids_set = set()
                for batchid, workspec in batchIDs_dict.items():
                    if batchid in batchIDs_set and (
                        (workspec.submitTime and workspec.submitTime >= update_time_threshold)
                        or (workspec.startTime and workspec.startTime >= update_time_threshold)
                    ):
                        clusterid = get_job_id_tuple_from_batchid(batchid)[0]
                        clusterids_set.add(clusterid)
                # skip if no clusterid to check
                if not clusterids_set:
                    break
            # Make constraint
            clusterids_str = ",".join(list(clusterids_set))
            if query_method is cache_query or allJobs:
                constraint = f'harvesterID =?= "{harvesterID}"'
            else:
                constraint = f"member(ClusterID, {{{clusterids_str}}})"
            if allJobs:
                tmpLog.debug(f"Query method: {query_method.__name__} ; allJobs")
            else:
                tmpLog.debug(f'Query method: {query_method.__name__} ; clusterids: "{clusterids_str}"')
            # Query
            jobs_iter = query_method(constraint=constraint, projection=CONDOR_JOB_ADS_LIST)
            for job in jobs_iter:
                try:
                    job_ads_dict = dict(job)
                except Exception as e:
                    tmpLog.error(f"In doing schedd query or history; got exception {e.__class__.__name__}: {e} ; {repr(job)}")
                batchid = get_batchid_from_job(job_ads_dict)
                condor_job_id = f"{self.submissionHost}#{batchid}"
                job_ads_all_dict[condor_job_id] = job_ads_dict
                # Remove batch jobs already gotten from the list
                if not allJobs:
                    batchIDs_set.discard(batchid)
            if len(batchIDs_set) == 0 or allJobs:
                break
        # Remaining
        if not allJobs and len(batchIDs_set) > 0:
            # Job unfound via both condor_q or condor_history, marked as unknown worker in harvester
            for batchid in batchIDs_set:
                condor_job_id = f"{self.submissionHost}#{batchid}"
                job_ads_all_dict[condor_job_id] = dict()
            tmpLog.info(f"Unfound batch jobs of submissionHost={self.submissionHost}: {' '.join(list(batchIDs_set))}")
        # Return
        return job_ads_all_dict

    # get condor jobs from the cache shared in DB, refreshing the cache from the schedd when due
    def query_from_cache(self, batchIDs_set, allJobs=False, to_update_cache=False):
        """
        Return a list of job ads dicts of all jobs if allJobs, else of jobs in batchIDs_set.
        Jobs missing in the cache are not included. Empty if the cache is not usable
        """
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobQuery.query_from_cache")
        # refresh if due; one at a time across threads and processes
        lock_name = f"condor_job_cache:{self.submissionHost}"
        locked_by = f"{socket.gethostname()}_{os.getpid()}-{get_ident():x}"
        if to_update_cache:
            lock_interval = min(_cache_force_refresh_min_interval, self.cacheRefreshInterval)
        else:
            lock_interval = self.cacheRefreshInterval
        if self.dbProxy.get_process_lock(lock_name, locked_by, lock_interval):
            jobs = self.refresh_cache(tmpLog)
            if jobs is not None:
                if allJobs:
                    return jobs
                return [job for job in jobs if get_batchid_from_job(job) in batchIDs_set]
        # read from the cache
        refresh_time, target_map = self.dbProxy.get_worker_target_cache(self.submissionHost, None if allJobs else list(batchIDs_set))
        max_age = 2 * self.cacheRefreshInterval
        if refresh_time is None or refresh_time < core_utils.naive_utcnow() - datetime.timedelta(seconds=max_age):
            tmpLog.debug(f"cache not refreshed in last {max_age} sec; not used")
            return []
        tmpLog.debug(f"got {len(target_map)} jobs from cache refreshed at {refresh_time}")
        return list(target_map.values())

    # query all jobs of this harvester from the schedd and write them to the cache. Return a list of job ads dicts, or None if failed
    def refresh_cache(self, tmpLog):
        refresh_time = core_utils.naive_utcnow()
        try:
            jobs_iter = self.schedd.query(constraint=f'harvesterID =?= "{harvesterID}"', projection=CONDOR_JOB_ADS_LIST)
        except Exception as e:
            tmpLog.error(f"failed to query schedd to refresh cache; {e.__class__.__name__}: {e}")
            return None
        jobs = []
        target_list = []
        for job in jobs_iter:
            try:
                job_ads_dict = dict(job)
                batchid = get_batchid_from_job(job_ads_dict)
                worker_id = job_ads_dict.get("harvesterWorkerID")
                status_time = job_ads_dict.get("EnteredCurrentStatus")
                target_list.append(
                    {
                        "targetID": batchid,
                        "workerID": int(worker_id) if worker_id is not None else None,
                        "statusTime": (
                            datetime.datetime.fromtimestamp(int(status_time), datetime.timezone.utc).replace(tzinfo=None) if status_time is not None else None
                        ),
                        # values of non-JSON types, e.g. expressions, as strings
                        "attributes": {k: v if isinstance(v, (str, int, float, bool, type(None))) else str(v) for k, v in job_ads_dict.items()},
                    }
                )
            except Exception as e:
                tmpLog.error(f"In refreshing cache; got exception {e.__class__.__name__}: {e} ; {repr(job)}")
                continue
            jobs.append(job_ads_dict)
        retVal = self.dbProxy.refresh_worker_target_cache(self.submissionHost, target_list, refresh_time)
        if retVal is None:
            tmpLog.error("failed to write cache to DB")
        else:
            tmpLog.debug(f"refreshed cache with {retVal}")
        return jobs


# Condor job submit
class CondorJobSubmit(CondorClient, metaclass=SingletonWithID):
    # class lock
    classLock = threading.Lock()

    def __init__(self, *args, **kwargs):
        self.submissionHost = str(kwargs.get("id"))
        # Make logger
        tmpLog = core_utils.make_logger(
            baseLogger, f"submissionHost={self.submissionHost} thrid={get_ident()} oid={id(self)}", method_name="CondorJobSubmit.__init__"
        )
        # Initialize
        tmpLog.debug("Start")
        self.lock = threading.Lock()
        CondorClient.__init__(self, self.submissionHost, *args, **kwargs)
        tmpLog.debug("Initialize done")

    def submit(self, jdl_list, use_spool=False):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobSubmit.submit")
        # Get all
        tmpLog.debug("Start")
        job_ads_all_dict = {}
        if self.condor_api_type == "python":
            try:
                # TODO: submit_with_python will meet segfault or c++ error after many times of submission; need help from condor team
                # TODO: submit_with_python_process has no such error but spawns some processes that will not terminate after harvester stops
                # TODO: Fall back to submit_with_command for now
                # retVal = self.submit_with_python(jdl_list, use_spool)
                # retVal = self.submit_with_python_process(jdl_list, use_spool)
                retVal = self.submit_with_command(jdl_list, use_spool)
            except Exception as e:
                tmpLog.error(f"Exception {e.__class__.__name__}: {e}")
                raise
        else:
            retVal = self.submit_with_command(jdl_list, use_spool)
        return retVal

    def submit_with_command(self, jdl_list, use_spool=False, tmp_str="", keep_temp_sdf=False):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobSubmit.submit_with_command")
        # Initialize
        errStr = ""
        batchIDs_list = []
        # make sdf temp file from jdls
        tmpFile = tempfile.NamedTemporaryFile(mode="w", delete=(not keep_temp_sdf), suffix=f"_{tmp_str}_cluster_submit.sdf")
        sdf_file = tmpFile.name
        tmpFile.write("\n\n".join(jdl_list))
        tmpFile.flush()
        # make condor remote options
        name_opt = f"-name {self.condor_schedd}" if self.condor_schedd else ""
        pool_opt = f"-pool {self.condor_pool}" if self.condor_pool else ""
        spool_opt = "-remote -spool" if use_spool and self.condor_schedd else ""
        # command
        comStr = f"condor_submit -single-cluster {spool_opt} {name_opt} {pool_opt} {sdf_file}"
        # submit
        tmpLog.debug(f"submit with command: {comStr}")
        try:
            p = subprocess.Popen(comStr.split(), shell=False, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
        except Exception as e:
            stdOut = ""
            stdErr = core_utils.dump_error_message(tmpLog, no_message=True)
            retCode = 1
            errStr = f"{e.__class__.__name__}: {e}"
        finally:
            tmpFile.close()
        tmpLog.debug(f"retCode={retCode}")
        if retCode == 0:
            # extract clusterid and n_jobs
            job_id_match = None
            for tmp_line_str in stdOut.split("\n"):
                job_id_match = re.search(r"^(\d+) job[(]s[)] submitted to cluster (\d+)\.$", tmp_line_str)
                if job_id_match:
                    break
            if job_id_match is not None:
                n_jobs = int(job_id_match.group(1))
                clusterid = job_id_match.group(2)
                batchIDs_list = [f"{clusterid}.{procid}" for procid in range(n_jobs)]
                tmpLog.debug(f"submitted {n_jobs} jobs: {' '.join(batchIDs_list)}")
            else:
                errStr = f"no job submitted: {errStr}"
                tmpLog.error(errStr)
        else:
            errStr = f"{stdErr} ; {errStr}"
            tmpLog.error(f"submission failed: {errStr}")
        # Return
        return (batchIDs_list, errStr)

    @CondorClient.renew_session_and_retry
    def submit_with_python(self, jdl_list, use_spool=False):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobSubmit.submit_with_python")
        # Start
        tmpLog.debug("Start")
        # Initialize
        errStr = ""
        batchIDs_list = []
        # Make list of jdl map with dummy submit objects
        jdl_map_list = [dict(htcondor.Submit(jdl).items()) for jdl in jdl_list]
        # Go
        submit_obj = htcondor.Submit()
        try:
            with self.schedd.transaction() as txn:
                # TODO: Currently spool is not supported in htcondor.Submit ...
                submit_result = submit_obj.queue_with_itemdata(txn, 1, iter(jdl_map_list))
                clusterid = submit_result.cluster()
                first_proc = submit_result.first_proc()
                num_proc = submit_result.num_procs()
                batchIDs_list.extend([f"{clusterid}.{procid}" for procid in range(first_proc, first_proc + num_proc)])
        except RuntimeError as e:
            errStr = f"{e.__class__.__name__}: {e}"
            tmpLog.error(f"submission failed: {errStr}")
            raise
        if batchIDs_list:
            n_jobs = len(batchIDs_list)
            tmpLog.debug(f"submitted {n_jobs} jobs: {' '.join(batchIDs_list)}")
        elif not errStr:
            tmpLog.error("submitted nothing")
        tmpLog.debug("Done")
        # Return
        return (batchIDs_list, errStr)

    def submit_with_python_process(self, jdl_list, use_spool=False):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobSubmit.submit_with_python_process")
        # Start
        tmpLog.debug("Start")
        # Make list of jdl map with dummy submit objects
        jdl_map_list = [dict(htcondor.Submit(jdl).items()) for jdl in jdl_list]
        # Go
        mp_queue = multiprocessing.Queue()
        mp_process = multiprocessing.Process(target=condor_submit_process, args=(mp_queue, self.submissionHost, jdl_map_list, tmpLog))
        mp_process.daemon = True
        mp_process.start()
        (batchIDs_list, errStr) = mp_queue.get()
        mp_queue.close()
        mp_process.terminate()
        mp_process.join()
        if batchIDs_list:
            n_jobs = len(batchIDs_list)
            tmpLog.debug(f"submitted {n_jobs} jobs: {' '.join(batchIDs_list)}")
        elif not errStr:
            tmpLog.error("submitted nothing")
        tmpLog.debug("Done")
        # Return
        return (batchIDs_list, errStr)


# Condor job remove
class CondorJobManage(CondorClient, metaclass=SingletonWithID):
    # class lock
    classLock = threading.Lock()

    def __init__(self, *args, **kwargs):
        self.submissionHost = str(kwargs.get("id"))
        # Make logger
        tmpLog = core_utils.make_logger(
            baseLogger, f"submissionHost={self.submissionHost} thrid={get_ident()} oid={id(self)}", method_name="CondorJobManage.__init__"
        )
        # Initialize
        tmpLog.debug("Start")
        self.lock = threading.Lock()
        CondorClient.__init__(self, self.submissionHost, *args, **kwargs)
        tmpLog.debug("Initialize done")

    def remove(self, batchIDs_list=[]):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobManage.remove")
        # Get all
        tmpLog.debug("Start")
        job_ads_all_dict = {}
        if self.condor_api_type == "python":
            try:
                retVal = self.remove_with_python(batchIDs_list)
            except Exception as e:
                tmpLog.error(f"Exception {e.__class__.__name__}: {e}")
                raise
        else:
            retVal = self.remove_with_command(batchIDs_list)
        return retVal

    def remove_with_command(self, batchIDs_list=[]):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobManage.remove_with_command")
        # if workspec.batchID is None:
        #     tmpLog.info('Found workerID={0} has submissionHost={1} batchID={2} . Cannot kill. Skipped '.format(
        #                     workspec.workerID, workspec.submissionHost, workspec.batchID))
        #     ret_list.append((True, ''))
        #
        # ## Parse condor remote options
        # name_opt, pool_opt = '', ''
        # if workspec.submissionHost is None or workspec.submissionHost == 'LOCAL':
        #     pass
        # else:
        #     try:
        #         condor_schedd, condor_pool = workspec.submissionHost.split(',')[0:2]
        #     except ValueError:
        #         errStr = 'Invalid submissionHost: {0} . Skipped'.format(workspec.submissionHost)
        #         tmpLog.error(errStr)
        #         ret_list.append((False, errStr))
        #     name_opt = '-name {0}'.format(condor_schedd) if condor_schedd else ''
        #     pool_opt = '-pool {0}'.format(condor_pool) if condor_pool else ''
        #
        # ## Kill command
        # comStr = 'condor_rm {name_opt} {pool_opt} {batchID}'.format(name_opt=name_opt,
        #                                                             pool_opt=pool_opt,
        #                                                             batchID=workspec.batchID)
        # (retCode, stdOut, stdErr) = _runShell(comStr)
        # if retCode != 0:
        #     comStr = 'condor_q -l {name_opt} {pool_opt} {batchID}'.format(name_opt=name_opt,
        #                                                                 pool_opt=pool_opt,
        #                                                                 batchID=workspec.batchID)
        #     (retCode, stdOut, stdErr) = _runShell(comStr)
        #     if ('ClusterId = {0}'.format(workspec.batchID) in str(stdOut) \
        #         and 'JobStatus = 3' not in str(stdOut)) or retCode != 0:
        #         ## Force to cancel if batch job not terminated first time
        #         comStr = 'condor_rm -forcex {name_opt} {pool_opt} {batchID}'.format(name_opt=name_opt,
        #                                                                     pool_opt=pool_opt,
        #                                                                     batchID=workspec.batchID)
        #         (retCode, stdOut, stdErr) = _runShell(comStr)
        #         if retCode != 0:
        #             ## Command failed to kill
        #             errStr = 'command "{0}" failed, retCode={1}, error: {2} {3}'.format(comStr, retCode, stdOut, stdErr)
        #             tmpLog.error(errStr)
        #             ret_list.append((False, errStr))
        #     ## Found already killed
        #     tmpLog.info('Found workerID={0} submissionHost={1} batchID={2} already killed'.format(
        #                     workspec.workerID, workspec.submissionHost, workspec.batchID))
        # else:
        #     tmpLog.info('Succeeded to kill workerID={0} submissionHost={1} batchID={2}'.format(
        #                     workspec.workerID, workspec.submissionHost, workspec.batchID))
        raise NotImplementedError

    @CondorClient.renew_session_and_retry
    def remove_with_python(self, batchIDs_list=[]):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobManage.remove_with_python")
        # Start
        tmpLog.debug("Start")
        # Acquire class lock
        with self.classLock:
            tmpLog.debug("Got class lock")
            # Initialize
            ret_list = []
            retMap = {}
            # Go
            n_jobs = len(batchIDs_list)
            act_ret = self.schedd.act(htcondor.JobAction.Remove, batchIDs_list)
            # Check if all jobs clear (off from schedd queue)
            is_all_clear = n_jobs == act_ret["TotalAlreadyDone"] + act_ret["TotalNotFound"] + act_ret["TotalSuccess"]
            if act_ret and is_all_clear:
                tmpLog.debug(f"removed {n_jobs} jobs: {','.join(batchIDs_list)}")
                for batchid in batchIDs_list:
                    condor_job_id = f"{self.submissionHost}#{batchid}"
                    retMap[condor_job_id] = (True, "")
            else:
                tmpLog.error(f"job removal failed; batchIDs_list={batchIDs_list}, got: {act_ret}")
                # need to query queue for unterminated jobs not removed yet
                clusterids_set = set([get_job_id_tuple_from_batchid(batchid)[0] for batchid in batchIDs_list])
                clusterids_str = ",".join(list(clusterids_set))
                constraint = f"member(ClusterID, {{{clusterids_str}}}) && JobStatus =!= 3 && JobStatus =!= 4"
                jobs_iter = self.schedd.query(constraint=constraint, projection=CONDOR_JOB_ADS_LIST)
                all_batchid_map = {}
                ok_batchid_list = []
                ng_batchid_list = []
                for job in jobs_iter:
                    job_ads_dict = dict(job)
                    batchid = get_batchid_from_job(job_ads_dict)
                    all_batchid_map[batchid] = job_ads_dict
                for batchid in batchIDs_list:
                    condor_job_id = f"{self.submissionHost}#{batchid}"
                    if batchid in all_batchid_map:
                        ng_batchid_list.append(batchid)
                        retMap[condor_job_id] = (False, f"batchID={batchid} still unterminated in condor queue")
                    else:
                        ok_batchid_list.append(batchid)
                        retMap[condor_job_id] = (True, "")
                tmpLog.debug(
                    "removed {0} jobs: {1} ; failed to remove {2} jobs: {3}".format(
                        len(ok_batchid_list), ",".join(ok_batchid_list), len(ng_batchid_list), ",".join(ng_batchid_list)
                    )
                )
        tmpLog.debug("Done")
        # Return
        return retMap


# ===============================================================
