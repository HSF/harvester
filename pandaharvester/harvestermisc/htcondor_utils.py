# === Imports ===================================================

import datetime
import functools
import os
import re
import socket
import subprocess
import tempfile
import threading
import time
import traceback
from threading import get_ident

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.core_utils import SingletonWithID
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy

# === Condor ====================================================

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

# errors of condor API calls upon which to renew the session and retry; htcondor2 raises HTCondorException instead of RuntimeError
CONDOR_SESSION_ERRORS = (RuntimeError,) + ((htcondor.HTCondorException,) if hasattr(htcondor, "HTCondorException") else ())

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


def _normalize_batchid(batchid):
    """
    Get batchID string with ProcId; backward compatibility if batchID does not contain ProcId
    """
    batchid_str = str(batchid)
    if "." not in batchid_str:
        batchid_str += ".0"
    return batchid_str


def condor_job_id_from_workspec(workspec):
    """
    Generate condor job id with schedd host from workspec
    """
    return f"{workspec.submissionHost}#{_normalize_batchid(workspec.batchID)}"


def get_host_batchid_map(workspec_list):
    """
    Get a dictionary of submissionHost: list of batchIDs from workspec_list
    return {submissionHost_1: {batchID_1_1, ...}, submissionHost_2: {...}, ...}
    """
    host_batchid_map = {}
    for workspec in workspec_list:
        if workspec.batchID is None:
            continue
        host_batchid_map.setdefault(workspec.submissionHost, {})[_normalize_batchid(workspec.batchID)] = workspec
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


# ===============================================================

# === Classes ===================================================


# Condor client
class CondorClient(object):
    @classmethod
    def renew_session_and_retry(cls, func):
        """
        If got an error of condor API, call renew_session and retry once
        """

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Make logger
            tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorClient.renew_session_if_error")
            # no session if failed to create one in initialization
            if getattr(self, "schedd", None) is None:
                if not self.lock.acquire(False):
                    raise Exception(f"no session with {self.submissionHost} while another thread is renewing it")
                try:
                    is_renewed = self.renew_session()
                finally:
                    self.lock.release()
                if not is_renewed:
                    errStr = f"failed to communicate with {self.submissionHost}"
                    tmpLog.error(errStr)
                    raise Exception(errStr)
            try:
                return func(self, *args, **kwargs)
            except CONDOR_SESSION_ERRORS as e:
                tmpLog.debug(f"got {e.__class__.__name__}: {e}")
                if not self.lock.acquire(False):
                    tmpLog.debug("another thread is renewing condor session; skipped...")
                    raise
                try:
                    is_renewed = self.renew_session()
                finally:
                    self.lock.release()
                if not is_renewed:
                    tmpLog.error("failed to renew condor session")
                    raise
                tmpLog.debug(f"condor session renewed. Retrying {func.__name__}")
                return func(self, *args, **kwargs)

        return wrapper

    def __init__(self, submissionHost, *args, **kwargs):
        self.submissionHost = submissionHost
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorClient.__init__")
        # Initialize
        tmpLog.debug("Initializing client")
        self.lock = threading.Lock()
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
        # Python API
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
        # Sleep after renewal; not needed for a new session
        if not init:
            time.sleep(3)
        return True


# Condor job query
class CondorJobQuery(CondorClient, metaclass=SingletonWithID):
    """
    One instance per submissionHost, shared by all queues using it; query settings are thus given in each call rather than in the constructor
    """

    def __init__(self, *args, **kwargs):
        self.submissionHost = str(kwargs.get("id"))
        # Make logger
        tmpLog = core_utils.make_logger(
            baseLogger, f"submissionHost={self.submissionHost} thrid={get_ident()} oid={id(self)}", method_name="CondorJobQuery.__init__"
        )
        # Initialize
        tmpLog.debug("Start")
        CondorClient.__init__(self, self.submissionHost, *args, **kwargs)
        # for the condor job cache shared in DB
        self.dbProxy = DBProxy()
        tmpLog.debug("Initialize done")

    def get_all(
        self,
        batchIDs_dict=None,
        allJobs=False,
        to_update_cache=False,
        cacheEnable=False,
        cacheRefreshInterval=240,
        useCondorHistory=True,
        useCondorHistoryMaxAge=7200,
    ):
        """
        Return {condor_job_id: job ads dict} of all jobs of this harvester if allJobs, else of jobs in batchIDs_dict ({batchID: workspec}).
        Jobs in batchIDs_dict not found get an empty dict
        """
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobQuery.get_all")
        # Get all
        tmpLog.debug("Start")
        try:
            return self.query_with_python(batchIDs_dict, allJobs, to_update_cache, cacheEnable, cacheRefreshInterval, useCondorHistory, useCondorHistoryMaxAge)
        except Exception as e:
            tmpLog.error(f"Exception {e.__class__.__name__}: {e} ; {traceback.format_exc()}")
            raise

    @CondorClient.renew_session_and_retry
    def query_with_python(self, batchIDs_dict, allJobs, to_update_cache, cacheEnable, cacheRefreshInterval, useCondorHistory, useCondorHistoryMaxAge):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobQuery.query_with_python")
        # Start query
        tmpLog.debug("Start query")
        job_ads_all_dict = {}
        if batchIDs_dict is None:
            batchIDs_dict = {}
        # jobs not found yet
        batchIDs_set = set(batchIDs_dict.keys())

        def add_jobs(jobs_iter, source):
            for job in jobs_iter:
                try:
                    job_ads_dict = dict(job)
                    batchid = get_batchid_from_job(job_ads_dict)
                except Exception as e:
                    tmpLog.error(f"In {source}; got exception {e.__class__.__name__}: {e} ; {repr(job)}")
                    continue
                job_ads_all_dict[f"{self.submissionHost}#{batchid}"] = job_ads_dict
                batchIDs_set.discard(batchid)

        # query from the cache shared in DB
        if cacheEnable:
            cached_jobs = self.query_from_cache(batchIDs_set, cacheRefreshInterval, allJobs=allJobs, to_update_cache=to_update_cache)
            if cached_jobs is not None:
                add_jobs(cached_jobs, "cache")
                if allJobs:
                    return job_ads_all_dict
        # all jobs of this harvester in the queue
        if allJobs:
            tmpLog.debug("Query method: query ; allJobs")
            add_jobs(self.schedd.query(constraint=f'harvesterID =?= "{harvesterID}"', projection=CONDOR_JOB_ADS_LIST), "schedd query")
            return job_ads_all_dict
        # jobs not found yet, in the queue
        if batchIDs_set:
            clusterids_str = ",".join(sorted({get_job_id_tuple_from_batchid(batchid)[0] for batchid in batchIDs_set}))
            tmpLog.debug(f'Query method: query ; clusterids: "{clusterids_str}"')
            add_jobs(self.schedd.query(constraint=f"member(ClusterID, {{{clusterids_str}}})", projection=CONDOR_JOB_ADS_LIST), "schedd query")
        # jobs not found yet, in condor history; excluding outdated ones
        if batchIDs_set and useCondorHistory:
            update_time_threshold = core_utils.naive_utcnow() - datetime.timedelta(seconds=useCondorHistoryMaxAge)
            clusterids_set = set()
            for batchid in batchIDs_set:
                workspec = batchIDs_dict[batchid]
                if (workspec.submitTime and workspec.submitTime >= update_time_threshold) or (
                    workspec.startTime and workspec.startTime >= update_time_threshold
                ):
                    clusterids_set.add(get_job_id_tuple_from_batchid(batchid)[0])
            if clusterids_set:
                clusterids_str = ",".join(sorted(clusterids_set))
                tmpLog.debug(f'Query method: history ; clusterids: "{clusterids_str}"')
                add_jobs(self.schedd.history(constraint=f"member(ClusterID, {{{clusterids_str}}})", projection=CONDOR_JOB_ADS_LIST), "schedd history")
        # Remaining
        if batchIDs_set:
            # Job unfound via both condor_q or condor_history, marked as unknown worker in harvester
            for batchid in batchIDs_set:
                job_ads_all_dict[f"{self.submissionHost}#{batchid}"] = dict()
            tmpLog.info(f"Unfound batch jobs of submissionHost={self.submissionHost}: {' '.join(sorted(batchIDs_set))}")
        # Return
        return job_ads_all_dict

    # get condor jobs from the cache shared in DB, refreshing the cache from the schedd when due
    def query_from_cache(self, batchIDs_set, cacheRefreshInterval, allJobs=False, to_update_cache=False):
        """
        Return a list of job ads dicts of all jobs if allJobs, else of jobs in batchIDs_set.
        Jobs missing in the cache are not included. None if the cache is not usable
        """
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobQuery.query_from_cache")
        # refresh if due; one at a time across threads and processes
        lock_name = f"condor_job_cache:{self.submissionHost}"
        locked_by = f"{socket.gethostname()}_{os.getpid()}-{get_ident():x}"
        if to_update_cache:
            lock_interval = min(_cache_force_refresh_min_interval, cacheRefreshInterval)
        else:
            lock_interval = cacheRefreshInterval
        if self.dbProxy.get_process_lock(lock_name, locked_by, lock_interval):
            jobs = self.refresh_cache(tmpLog)
            if jobs is not None:
                if allJobs:
                    return jobs
                return [job for job in jobs if get_batchid_from_job(job) in batchIDs_set]
        # read from the cache
        refresh_time, target_map = self.dbProxy.get_worker_target_cache(self.submissionHost, None if allJobs else list(batchIDs_set))
        max_age = 2 * cacheRefreshInterval
        if refresh_time is None or refresh_time < core_utils.naive_utcnow() - datetime.timedelta(seconds=max_age):
            tmpLog.debug(f"cache not refreshed in last {max_age} sec; not used")
            return None
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
                        "statusTime": core_utils.naive_utcfromtimestamp(int(status_time)) if status_time is not None else None,
                        # only queried attributes, without ones the schedd adds such as ServerTime which changes in every query;
                        # values of non-JSON types, e.g. expressions, as strings
                        "attributes": {
                            k: v if isinstance(v, (str, int, float, bool, type(None))) else str(v) for k, v in job_ads_dict.items() if k in CONDOR_JOB_ADS_LIST
                        },
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
    def __init__(self, *args, **kwargs):
        self.submissionHost = str(kwargs.get("id"))
        # Make logger
        tmpLog = core_utils.make_logger(
            baseLogger, f"submissionHost={self.submissionHost} thrid={get_ident()} oid={id(self)}", method_name="CondorJobSubmit.__init__"
        )
        # Initialize
        tmpLog.debug("Start")
        CondorClient.__init__(self, self.submissionHost, *args, **kwargs)
        tmpLog.debug("Initialize done")

    def submit(self, jdl_list, use_spool=False):
        """
        Submit jdls as one cluster with condor_submit command; the Python API (Schedd.submit) got segfaults or C++ errors after many submissions.
        Return (list of batchIDs, error string)
        """
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobSubmit.submit")
        # Initialize
        errStr = ""
        batchIDs_list = []
        # make sdf temp file from jdls
        tmpFile = tempfile.NamedTemporaryFile(mode="w", suffix="_cluster_submit.sdf")
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


# Condor job remove
class CondorJobManage(CondorClient, metaclass=SingletonWithID):
    def __init__(self, *args, **kwargs):
        self.submissionHost = str(kwargs.get("id"))
        # Make logger
        tmpLog = core_utils.make_logger(
            baseLogger, f"submissionHost={self.submissionHost} thrid={get_ident()} oid={id(self)}", method_name="CondorJobManage.__init__"
        )
        # Initialize
        tmpLog.debug("Start")
        CondorClient.__init__(self, self.submissionHost, *args, **kwargs)
        # one removal at a time on this submissionHost
        self.removeLock = threading.Lock()
        tmpLog.debug("Initialize done")

    @CondorClient.renew_session_and_retry
    def remove(self, batchIDs_list):
        """
        Remove condor jobs. Return {condor_job_id: (is_ok, error string)}
        """
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, f"submissionHost={self.submissionHost}", method_name="CondorJobManage.remove")
        # Start
        tmpLog.debug("Start")
        with self.removeLock:
            tmpLog.debug("Got remove lock")
            # Initialize
            retMap = {}
            # Go
            n_jobs = len(batchIDs_list)
            act_ret = self.schedd.act(htcondor.JobAction.Remove, batchIDs_list)
            # Check if all jobs clear (off from schedd queue)
            is_all_clear = bool(act_ret) and n_jobs == act_ret["TotalAlreadyDone"] + act_ret["TotalNotFound"] + act_ret["TotalSuccess"]
            if is_all_clear:
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
