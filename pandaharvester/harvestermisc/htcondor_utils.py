# === Imports ===================================================

import re
import time
import threading
import random
import multiprocessing
import tempfile
import functools
import traceback
import xml.etree.ElementTree as ET

try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

try:
    from threading import get_ident
except ImportError:
    from thread import get_ident

import six

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.core_utils import SingletonWithID
from pandaharvester.harvestercore.fifos import SpecialFIFOBase

# condor python or command api
try:
    import htcondor
except ImportError:
    CONDOR_API = "command"
else:
    CONDOR_API = "python"

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
    return "{0}#{1}".format(workspec.submissionHost, batchid_str)


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
        try:
            host_batchid_map[host].append(batchid_str)
        except KeyError:
            host_batchid_map[host] = [batchid_str]
    return host_batchid_map


def get_batchid_from_job(job_ads_dict):
    """
    Get batchID string from condor job dict
    """
    batchid = "{0}.{1}".format(job_ads_dict["ClusterId"], job_ads_dict["ProcId"])
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


def condor_submit_process(mp_queue, host, jdl_map_list):
    """
    Function for new process to submit condor
    """
    # initialization
    errStr = ""
    batchIDs_list = []
    # parse schedd and pool name
    condor_schedd, condor_pool = None, None
    if host in ("LOCAL", "None"):
        tmpLog.debug("submissionHost is {0}, treated as local schedd. Skipped".format(host))
    else:
        try:
            condor_schedd, condor_pool = host.split(",")[0:2]
        except ValueError:
            tmpLog.error("Invalid submissionHost: {0} . Skipped".format(host))
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
        errStr = "create condor collector and schedd failed; {0}: {1}".format(e.__class__.__name__, e)
    else:
        submit_obj = htcondor.Submit()
        try:
            with schedd.transaction() as txn:
                # TODO: Currently spool is not supported in htcondor.Submit ...
                submit_result = submit_obj.queue_with_itemdata(txn, 1, iter(jdl_map_list))
                clusterid = submit_result.cluster()
                first_proc = submit_result.first_proc()
                num_proc = submit_result.num_procs()
                batchIDs_list.extend(["{0}.{1}".format(clusterid, procid) for procid in range(first_proc, first_proc + num_proc)])
        except RuntimeError as e:
            errStr = "submission failed; {0}: {1}".format(e.__class__.__name__, e)
    mp_queue.put((batchIDs_list, errStr))


# ===============================================================

# === Classes ===================================================

# Condor queue cache fifo


class CondorQCacheFifo(six.with_metaclass(SingletonWithID, SpecialFIFOBase)):
    global_lock_id = -1

    def __init__(self, target, *args, **kwargs):
        name_suffix = target.split(".")[0]
        name_suffix = re.sub("-", "_", name_suffix)
        self.titleName = "CondorQCache_{0}".format(name_suffix)
        SpecialFIFOBase.__init__(self)

    def lock(self, score=None):
        lock_key = format(int(random.random() * 2**32), "x")
        if score is None:
            score = time.time()
        retVal = self.putbyid(self.global_lock_id, lock_key, score)
        if retVal:
            return lock_key
        return None

    def unlock(self, key=None, force=False):
        peeked_tuple = self.peekbyid(id=self.global_lock_id)
        if peeked_tuple.score is None or peeked_tuple.item is None:
            return True
        elif force or self.decode(peeked_tuple.item) == key:
            self.delete([self.global_lock_id])
            return True
        else:
            return False


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
            tmpLog = core_utils.make_logger(baseLogger, "submissionHost={0}".format(self.submissionHost), method_name="CondorClient.renew_session_if_error")
            func_name = func.__name__
            try:
                self.schedd
            except AttributeError as e:
                if self.lock.acquire(False):
                    is_renewed = self.renew_session()
                    self.lock.release()
                    if not is_renewed:
                        errStr = "failed to communicate with {0}".format(self.submissionHost)
                        tmpLog.error(errStr)
                        tmpLog.debug("got RuntimeError: {0}".format(e))
                        raise Exception(errStr)
            try:
                ret = func(self, *args, **kwargs)
            except RuntimeError as e:
                tmpLog.debug("got RuntimeError: {0}".format(e))
                if self.lock.acquire(False):
                    is_renewed = self.renew_session()
                    self.lock.release()
                    if is_renewed:
                        if to_retry:
                            tmpLog.debug("condor session renewed. Retrying {0}".format(func_name))
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
        tmpLog = core_utils.make_logger(baseLogger, "submissionHost={0}".format(self.submissionHost), method_name="CondorClient.__init__")
        # Initialize
        tmpLog.debug("Initializing client")
        self.lock = threading.Lock()
        self.condor_api = CONDOR_API
        self.condor_schedd = None
        self.condor_pool = None
        # Parse condor command remote options from workspec
        if self.submissionHost in ("LOCAL", "None"):
            tmpLog.debug("submissionHost is {0}, treated as local schedd. Skipped".format(self.submissionHost))
        else:
            try:
                self.condor_schedd, self.condor_pool = self.submissionHost.split(",")[0:2]
                if self.condor_schedd in ["None"]:
                    self.condor_schedd = None
                if self.condor_pool in ["None"]:
                    self.condor_pool = None
            except ValueError:
                tmpLog.error("Invalid submissionHost: {0} . Skipped".format(self.submissionHost))
        # Use Python API or fall back to command
        if self.condor_api == "python":
            try:
                self.secman = htcondor.SecMan()
                self.renew_session(init=True)
            except Exception as e:
                tmpLog.error("Error when using htcondor Python API. Exception {0}: {1}".format(e.__class__.__name__, e))
                raise
        tmpLog.debug("Initialized client")

    @synchronize
    def renew_session(self, retry=3, init=False):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, "submissionHost={0}".format(self.submissionHost), method_name="CondorClient.renew_session")
        # Clear security session if not initialization
        if not init:
            tmpLog.info("Renew condor session")
            self.secman.invalidateAllSessions()
        # Recreate collector and schedd object
        i_try = 1
        while i_try <= retry:
            try:
                tmpLog.info("Try {0}".format(i_try))
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
                tmpLog.warning("Recreate condor collector and schedd failed: {0}".format(e))
                if i_try < retry:
                    tmpLog.warning("Failed. Retry...")
                else:
                    tmpLog.warning("Retry {0} times. Still failed. Skipped".format(i_try))
                    return False
                i_try += 1
                self.secman.invalidateAllSessions()
                time.sleep(3)
        # Sleep
        time.sleep(3)
        return True


# Condor job query
class CondorJobQuery(six.with_metaclass(SingletonWithID, CondorClient)):
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

    def __init__(self, cacheEnable=False, cacheRefreshInterval=None, useCondorHistory=True, *args, **kwargs):
        self.submissionHost = str(kwargs.get("id"))
        # Make logger
        tmpLog = core_utils.make_logger(
            baseLogger, "submissionHost={0} thrid={1} oid={2}".format(self.submissionHost, get_ident(), id(self)), method_name="CondorJobQuery.__init__"
        )
        # Initialize
        with self.classLock:
            tmpLog.debug("Start")
            CondorClient.__init__(self, self.submissionHost, *args, **kwargs)
            # For condor_q cache
            self.cacheEnable = cacheEnable
            if self.cacheEnable:
                self.cache = ([], 0)
                self.cacheRefreshInterval = cacheRefreshInterval
            self.useCondorHistory = useCondorHistory
            tmpLog.debug("Initialize done")

    def get_all(self, batchIDs_list=[], allJobs=False):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, "submissionHost={0}".format(self.submissionHost), method_name="CondorJobQuery.get_all")
        # Get all
        tmpLog.debug("Start")
        job_ads_all_dict = {}
        if self.condor_api == "python":
            try:
                job_ads_all_dict = self.query_with_python(batchIDs_list, allJobs)
            except Exception as e:
                tmpLog.error("Exception {0}: {1}".format(e.__class__.__name__, e))
                raise
        else:
            job_ads_all_dict = self.query_with_command(batchIDs_list)
        return job_ads_all_dict

    def query_with_command(self, batchIDs_list=[]):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, "submissionHost={0}".format(self.submissionHost), method_name="CondorJobQuery.query_with_command")
        # Start query
        tmpLog.debug("Start query")
        job_ads_all_dict = {}
        batchIDs_set = set(batchIDs_list)
        for orig_comStr in self.orig_comStr_list:
            # String of batchIDs
            batchIDs_str = " ".join(list(batchIDs_set))
            # Command
            if "condor_q" in orig_comStr or ("condor_history" in orig_comStr and batchIDs_set):
                name_opt = "-name {0}".format(self.condor_schedd) if self.condor_schedd else ""
                pool_opt = "-pool {0}".format(self.condor_pool) if self.condor_pool else ""
                ids = batchIDs_str
                comStr = "{cmd} {name_opt} {pool_opt} {ids}".format(cmd=orig_comStr, name_opt=name_opt, pool_opt=pool_opt, ids=ids)
            else:
                # tmpLog.debug('No batch job left to query in this cycle by this thread')
                continue
            tmpLog.debug("check with {0}".format(comStr))
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
                        condor_job_id = "{0}#{1}".format(self.submissionHost, batchid)
                        job_ads_all_dict[condor_job_id] = job_ads_dict
                        # Remove batch jobs already gotten from the list
                        if batchid in batchIDs_set:
                            batchIDs_set.discard(batchid)
                else:
                    # Job not found
                    tmpLog.debug("job not found with {0}".format(comStr))
                    continue
            else:
                # Command failed
                errStr = 'command "{0}" failed, retCode={1}, error: {2} {3}'.format(comStr, retCode, stdOut, stdErr)
                tmpLog.error(errStr)
        if len(batchIDs_set) > 0:
            # Job unfound via both condor_q or condor_history, marked as unknown worker in harvester
            for batchid in batchIDs_set:
                condor_job_id = "{0}#{1}".format(self.submissionHost, batchid)
                job_ads_all_dict[condor_job_id] = dict()
            tmpLog.info("Unfound batch jobs of submissionHost={0}: {1}".format(self.submissionHost, " ".join(list(batchIDs_set))))
        # Return
        return job_ads_all_dict

    @CondorClient.renew_session_and_retry
    def query_with_python(self, batchIDs_list=[], allJobs=False):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, "submissionHost={0}".format(self.submissionHost), method_name="CondorJobQuery.query_with_python")
        # Start query
        tmpLog.debug("Start query")
        cache_fifo = None
        job_ads_all_dict = {}
        # make id sets
        batchIDs_set = set(batchIDs_list)
        clusterids_set = set([get_job_id_tuple_from_batchid(batchid)[0] for batchid in batchIDs_list])
        # query from cache

        def cache_query(constraint=None, projection=CONDOR_JOB_ADS_LIST, timeout=60):
            # query from condor xquery and update cache to fifo
            def update_cache(lockInterval=90):
                tmpLog.debug("update_cache")
                # acquire lock with score timestamp
                score = time.time() - self.cacheRefreshInterval + lockInterval
                lock_key = cache_fifo.lock(score=score)
                if lock_key is not None:
                    # acquired lock, update from condor schedd
                    tmpLog.debug("got lock, updating cache")
                    jobs_iter_orig = self.schedd.xquery(constraint=constraint, projection=projection)
                    jobs_iter = []
                    for job in jobs_iter_orig:
                        try:
                            jobs_iter.append(dict(job))
                        except Exception as e:
                            tmpLog.error("In updating cache schedd xquery; got exception {0}: {1} ; {2}".format(e.__class__.__name__, e, repr(job)))
                    timeNow = time.time()
                    cache_fifo.put(jobs_iter, timeNow)
                    self.cache = (jobs_iter, timeNow)
                    # release lock
                    retVal = cache_fifo.unlock(key=lock_key)
                    if retVal:
                        tmpLog.debug("done update cache and unlock")
                    else:
                        tmpLog.warning("cannot unlock... Maybe something wrong")
                    return jobs_iter
                else:
                    tmpLog.debug("cache fifo locked by other thread. Skipped")
                    return None

            # remove invalid or outdated caches from fifo

            def cleanup_cache(timeout=60):
                tmpLog.debug("cleanup_cache")
                id_list = list()
                attempt_timestamp = time.time()
                n_cleanup = 0
                while True:
                    if time.time() > attempt_timestamp + timeout:
                        tmpLog.debug("time is up when cleanup cache. Skipped")
                        break
                    peeked_tuple = cache_fifo.peek(skip_item=True)
                    if peeked_tuple is None:
                        tmpLog.debug("empty cache fifo")
                        break
                    elif peeked_tuple.score is not None and time.time() <= peeked_tuple.score + self.cacheRefreshInterval:
                        tmpLog.debug("nothing expired")
                        break
                    elif peeked_tuple.id is not None:
                        retVal = cache_fifo.delete([peeked_tuple.id])
                        if isinstance(retVal, int):
                            n_cleanup += retVal
                    else:
                        # problematic
                        tmpLog.warning("got nothing when cleanup cache, maybe problematic. Skipped")
                        break
                tmpLog.debug("cleaned up {0} objects in cache fifo".format(n_cleanup))

            # start
            jobs_iter = tuple()
            try:
                attempt_timestamp = time.time()
                while True:
                    if time.time() > attempt_timestamp + timeout:
                        # skip cache_query if too long
                        tmpLog.debug("cache_query got timeout ({0} seconds). Skipped ".format(timeout))
                        break
                    # get latest cache
                    peeked_tuple = cache_fifo.peeklast(skip_item=True)
                    if peeked_tuple is not None and peeked_tuple.score is not None:
                        # got something
                        if peeked_tuple.id == cache_fifo.global_lock_id:
                            if time.time() <= peeked_tuple.score + self.cacheRefreshInterval:
                                # lock
                                tmpLog.debug("got fifo locked. Wait and retry...")
                                time.sleep(random.uniform(1, 5))
                                continue
                            else:
                                # expired lock
                                tmpLog.debug("got lock expired. Clean up and retry...")
                                cleanup_cache()
                                continue
                        elif time.time() <= peeked_tuple.score + self.cacheRefreshInterval:
                            # got valid cache
                            _obj, _last_update = self.cache
                            if _last_update >= peeked_tuple.score:
                                # valid local cache
                                tmpLog.debug("valid local cache")
                                jobs_iter = _obj
                            else:
                                # valid fifo cache
                                tmpLog.debug("update local cache from fifo")
                                peeked_tuple_with_item = cache_fifo.peeklast()
                                if (
                                    peeked_tuple_with_item is not None
                                    and peeked_tuple.id != cache_fifo.global_lock_id
                                    and peeked_tuple_with_item.item is not None
                                ):
                                    jobs_iter = cache_fifo.decode(peeked_tuple_with_item.item)
                                    self.cache = (jobs_iter, peeked_tuple_with_item.score)
                                else:
                                    tmpLog.debug("peeked invalid cache fifo object. Wait and retry...")
                                    time.sleep(random.uniform(1, 5))
                                    continue
                        else:
                            # cache expired
                            tmpLog.debug("update cache in fifo")
                            retVal = update_cache()
                            if retVal is not None:
                                jobs_iter = retVal
                            cleanup_cache()
                        break
                    else:
                        # no cache in fifo, check with size again
                        if cache_fifo.size() == 0:
                            if time.time() > attempt_timestamp + random.uniform(10, 30):
                                # have waited for long enough, update cache
                                tmpLog.debug("waited enough, update cache in fifo")
                                retVal = update_cache()
                                if retVal is not None:
                                    jobs_iter = retVal
                                break
                            else:
                                # still nothing, wait
                                time.sleep(2)
                        continue
            except Exception as _e:
                tb_str = traceback.format_exc()
                tmpLog.error("Error querying from cache fifo; {0} ; {1}".format(_e, tb_str))
            return jobs_iter

        # query method options
        query_method_list = [self.schedd.xquery]
        if self.cacheEnable:
            cache_fifo = CondorQCacheFifo(target=self.submissionHost, id="{0},{1}".format(self.submissionHost, get_ident()))
            query_method_list.insert(0, cache_query)
        if self.useCondorHistory:
            query_method_list.append(self.schedd.history)
        # Go
        for query_method in query_method_list:
            # Make constraint
            clusterids_str = ",".join(list(clusterids_set))
            if query_method is cache_query or allJobs:
                constraint = 'harvesterID =?= "{0}"'.format(harvesterID)
            else:
                constraint = "member(ClusterID, {{{0}}})".format(clusterids_str)
            if allJobs:
                tmpLog.debug("Query method: {0} ; allJobs".format(query_method.__name__))
            else:
                tmpLog.debug('Query method: {0} ; clusterids: "{1}"'.format(query_method.__name__, clusterids_str))
            # Query
            jobs_iter = query_method(constraint=constraint, projection=CONDOR_JOB_ADS_LIST)
            for job in jobs_iter:
                try:
                    job_ads_dict = dict(job)
                except Exception as e:
                    tmpLog.error("In doing schedd xquery or history; got exception {0}: {1} ; {2}".format(e.__class__.__name__, e, repr(job)))
                batchid = get_batchid_from_job(job_ads_dict)
                condor_job_id = "{0}#{1}".format(self.submissionHost, batchid)
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
                condor_job_id = "{0}#{1}".format(self.submissionHost, batchid)
                job_ads_all_dict[condor_job_id] = dict()
            tmpLog.info("Unfound batch jobs of submissionHost={0}: {1}".format(self.submissionHost, " ".join(list(batchIDs_set))))
        # Return
        return job_ads_all_dict


# Condor job submit
class CondorJobSubmit(six.with_metaclass(SingletonWithID, CondorClient)):
    # class lock
    classLock = threading.Lock()

    def __init__(self, *args, **kwargs):
        self.submissionHost = str(kwargs.get("id"))
        # Make logger
        tmpLog = core_utils.make_logger(
            baseLogger, "submissionHost={0} thrid={1} oid={2}".format(self.submissionHost, get_ident(), id(self)), method_name="CondorJobSubmit.__init__"
        )
        # Initialize
        tmpLog.debug("Start")
        self.lock = threading.Lock()
        CondorClient.__init__(self, self.submissionHost, *args, **kwargs)
        tmpLog.debug("Initialize done")

    def submit(self, jdl_list, use_spool=False):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, "submissionHost={0}".format(self.submissionHost), method_name="CondorJobSubmit.submit")
        # Get all
        tmpLog.debug("Start")
        job_ads_all_dict = {}
        if self.condor_api == "python":
            try:
                # TODO: submit_with_python will meet segfault or c++ error after many times of submission; need help from condor team
                # TODO: submit_with_python_proces has no such error but spawns some processes that will not terminate after harvester stops
                # TODO: Fall back to submit_with_command for now
                # retVal = self.submit_with_python(jdl_list, use_spool)
                # retVal = self.submit_with_python_proces(jdl_list, use_spool)
                retVal = self.submit_with_command(jdl_list, use_spool)
            except Exception as e:
                tmpLog.error("Exception {0}: {1}".format(e.__class__.__name__, e))
                raise
        else:
            retVal = self.submit_with_command(jdl_list, use_spool)
        return retVal

    def submit_with_command(self, jdl_list, use_spool=False, tmp_str="", keep_temp_sdf=False):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, "submissionHost={0}".format(self.submissionHost), method_name="CondorJobSubmit.submit_with_command")
        # Initialize
        errStr = ""
        batchIDs_list = []
        # make sdf temp file from jdls
        tmpFile = tempfile.NamedTemporaryFile(mode="w", delete=(not keep_temp_sdf), suffix="_{0}_cluster_submit.sdf".format(tmp_str))
        sdf_file = tmpFile.name
        tmpFile.write("\n\n".join(jdl_list))
        tmpFile.flush()
        # make condor remote options
        name_opt = "-name {0}".format(self.condor_schedd) if self.condor_schedd else ""
        pool_opt = "-pool {0}".format(self.condor_pool) if self.condor_pool else ""
        spool_opt = "-remote -spool" if use_spool and self.condor_schedd else ""
        # command
        comStr = "condor_submit -single-cluster {spool_opt} {name_opt} {pool_opt} {sdf_file}".format(
            sdf_file=sdf_file, name_opt=name_opt, pool_opt=pool_opt, spool_opt=spool_opt
        )
        # submit
        tmpLog.debug("submit with command: {0}".format(comStr))
        try:
            p = subprocess.Popen(comStr.split(), shell=False, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
        except Exception as e:
            stdOut = ""
            stdErr = core_utils.dump_error_message(tmpLog, no_message=True)
            retCode = 1
            errStr = "{0}: {1}".format(e.__class__.__name__, e)
        finally:
            tmpFile.close()
        tmpLog.debug("retCode={0}".format(retCode))
        if retCode == 0:
            # extract clusterid and n_jobs
            job_id_match = None
            for tmp_line_str in stdOut.split("\n"):
                job_id_match = re.search("^(\d+) job[(]s[)] submitted to cluster (\d+)\.$", tmp_line_str)
                if job_id_match:
                    break
            if job_id_match is not None:
                n_jobs = int(job_id_match.group(1))
                clusterid = job_id_match.group(2)
                batchIDs_list = ["{0}.{1}".format(clusterid, procid) for procid in range(n_jobs)]
                tmpLog.debug("submitted {0} jobs: {1}".format(n_jobs, " ".join(batchIDs_list)))
            else:
                errStr = "no job submitted: {0}".format(errStr)
                tmpLog.error(errStr)
        else:
            errStr = "{0} ; {1}".format(stdErr, errStr)
            tmpLog.error("submission failed: {0}".format(errStr))
        # Return
        return (batchIDs_list, errStr)

    @CondorClient.renew_session_and_retry
    def submit_with_python(self, jdl_list, use_spool=False):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, "submissionHost={0}".format(self.submissionHost), method_name="CondorJobSubmit.submit_with_python")
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
                batchIDs_list.extend(["{0}.{1}".format(clusterid, procid) for procid in range(first_proc, first_proc + num_proc)])
        except RuntimeError as e:
            errStr = "{0}: {1}".format(e.__class__.__name__, e)
            tmpLog.error("submission failed: {0}".format(errStr))
            raise
        if batchIDs_list:
            n_jobs = len(batchIDs_list)
            tmpLog.debug("submitted {0} jobs: {1}".format(n_jobs, " ".join(batchIDs_list)))
        elif not errStr:
            tmpLog.error("submitted nothing")
        tmpLog.debug("Done")
        # Return
        return (batchIDs_list, errStr)

    def submit_with_python_process(self, jdl_list, use_spool=False):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, "submissionHost={0}".format(self.submissionHost), method_name="CondorJobSubmit.submit_with_python_process")
        # Start
        tmpLog.debug("Start")
        # Make list of jdl map with dummy submit objects
        jdl_map_list = [dict(htcondor.Submit(jdl).items()) for jdl in jdl_list]
        # Go
        mp_queue = multiprocessing.Queue()
        mp_process = multiprocessing.Process(target=condor_submit_process, args=(mp_queue, self.submissionHost, jdl_map_list))
        mp_process.daemon = True
        mp_process.start()
        (batchIDs_list, errStr) = mp_queue.get()
        mp_queue.close()
        mp_process.terminate()
        mp_process.join()
        if batchIDs_list:
            n_jobs = len(batchIDs_list)
            tmpLog.debug("submitted {0} jobs: {1}".format(n_jobs, " ".join(batchIDs_list)))
        elif not errStr:
            tmpLog.error("submitted nothing")
        tmpLog.debug("Done")
        # Return
        return (batchIDs_list, errStr)


# Condor job remove
class CondorJobManage(six.with_metaclass(SingletonWithID, CondorClient)):
    # class lock
    classLock = threading.Lock()

    def __init__(self, *args, **kwargs):
        self.submissionHost = str(kwargs.get("id"))
        # Make logger
        tmpLog = core_utils.make_logger(
            baseLogger, "submissionHost={0} thrid={1} oid={2}".format(self.submissionHost, get_ident(), id(self)), method_name="CondorJobManage.__init__"
        )
        # Initialize
        tmpLog.debug("Start")
        self.lock = threading.Lock()
        CondorClient.__init__(self, self.submissionHost, *args, **kwargs)
        tmpLog.debug("Initialize done")

    def remove(self, batchIDs_list=[]):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, "submissionHost={0}".format(self.submissionHost), method_name="CondorJobManage.remove")
        # Get all
        tmpLog.debug("Start")
        job_ads_all_dict = {}
        if self.condor_api == "python":
            try:
                retVal = self.remove_with_python(batchIDs_list)
            except Exception as e:
                tmpLog.error("Exception {0}: {1}".format(e.__class__.__name__, e))
                raise
        else:
            retVal = self.remove_with_command(batchIDs_list)
        return retVal

    def remove_with_command(self, batchIDs_list=[]):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, "submissionHost={0}".format(self.submissionHost), method_name="CondorJobManage.remove_with_command")
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
        tmpLog = core_utils.make_logger(baseLogger, "submissionHost={0}".format(self.submissionHost), method_name="CondorJobManage.remove_with_python")
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
                tmpLog.debug("removed {0} jobs: {1}".format(n_jobs, ",".join(batchIDs_list)))
                for batchid in batchIDs_list:
                    condor_job_id = "{0}#{1}".format(self.submissionHost, batchid)
                    retMap[condor_job_id] = (True, "")
            else:
                tmpLog.error("job removal failed; batchIDs_list={0}, got: {1}".format(batchIDs_list, act_ret))
                # need to query queue for unterminated jobs not removed yet
                clusterids_set = set([get_job_id_tuple_from_batchid(batchid)[0] for batchid in batchIDs_list])
                clusterids_str = ",".join(list(clusterids_set))
                constraint = "member(ClusterID, {{{0}}}) && JobStatus =!= 3 && JobStatus =!= 4".format(clusterids_str)
                jobs_iter = self.schedd.xquery(constraint=constraint, projection=CONDOR_JOB_ADS_LIST)
                all_batchid_map = {}
                ok_batchid_list = []
                ng_batchid_list = []
                for job in jobs_iter:
                    job_ads_dict = dict(job)
                    batchid = get_batchid_from_job(job_ads_dict)
                    all_batchid_map[batchid] = job_ads_dict
                for batchid in batchIDs_list:
                    condor_job_id = "{0}#{1}".format(self.submissionHost, batchid)
                    if batchid in all_batchid_map:
                        ng_batchid_list.append(batchid)
                        retMap[condor_job_id] = (False, "batchID={0} still unterminated in condor queue".format(batchid))
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
