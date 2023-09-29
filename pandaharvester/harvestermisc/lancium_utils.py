"""
Lancium python API wrapper functions
"""

from lancium.api.Data import Data
from lancium.api.Job import Job
import os
import time
import datetime
import re
import random
import threading
import traceback

from threading import get_ident

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.core_utils import SingletonWithID
from pandaharvester.harvestercore.fifos import SpecialFIFOBase


try:
    api_key = harvester_config.lancium.api_key
except AttributeError:
    raise RuntimeError("The configuration is missing the [lancium] section and/or the api_key entry")

# The key needs to be set before importing the lancium API
os.environ["LANCIUM_API_KEY"] = api_key


# logger
base_logger = core_utils.setup_logger("lancium_utils")

SECRETS_PATH = "/voms/"
SCRIPTS_PATH = "/scripts/"

LANCIUM_JOB_ATTRS_LIST = [
    "id",
    "name",
    "status",
    "created_at",
    "updated_at",
    "submitted_at",
    "completed_at",
    "exit_code",
]


def fake_callback(total_chunks, current_chunk):
    pass


def get_job_name_from_workspec(workspec):
    job_name = "{0}:{1}".format(harvester_config.master.harvester_id, workspec.workerID)
    return job_name


def get_workerid_from_job_name(job_name):
    tmp_str_list = job_name.split(":")
    harvester_id = None
    worker_id = None
    try:
        harvester_id = str(tmp_str_list[0])
        worker_id = int(tmp_str_list[-1])
    except Exception:
        pass
    return (harvester_id, worker_id)


def get_full_batch_id(submission_host, batch_id):
    full_batch_id = "{0}#{1}".format(submission_host, batch_id)
    return full_batch_id


def get_full_batch_id_from_workspec(workspec):
    full_batch_id = "{0}#{1}".format(workspec.submissionHost, workspec.batchID)
    return full_batch_id


def get_host_batch_id_map(workspec_list):
    """
    Get a dictionary of submissionHost: list of batchIDs from workspec_list
    return {submissionHost_1: {batchID_1_1, ...}, submissionHost_2: {...}, ...}
    """
    host_batch_id_map = {}
    for workspec in workspec_list:
        host = workspec.submissionHost
        batch_id = workspec.batchID
        if batch_id is None:
            continue
        try:
            host_batch_id_map[host].append(batch_id)
        except KeyError:
            host_batch_id_map[host] = [batch_id]
    return host_batch_id_map


def timestamp_to_datetime(timestamp_str):
    return datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")


class LanciumClient(object):
    def __init__(self, submission_host, queue_name=None):
        self.submission_host = submission_host
        self.queue_name = queue_name

    def upload_file(self, local_path, lancium_path, force=True):
        tmp_log = core_utils.make_logger(base_logger, method_name="upload_file")
        try:
            tmp_log.debug("Uploading file {0}".format(local_path))
            tmp_log = core_utils.make_logger(base_logger, "queue_name={0}".format(self.queue_name), method_name="upload_file")

            data = Data().create(lancium_path, "file", source=os.path.abspath(local_path), force=force)
            data.upload(os.path.abspath(local_path), fake_callback)
            ex = data.show(lancium_path)[0]
            tmp_log.debug("Done: {0}".format(ex.__dict__))

            return True, ""
        except Exception as _e:
            error_message = "Failed to upload file with {0}".format(_e)
            tmp_log.error("Failed to upload the file with {0}".format(traceback.format_exc()))
            return False, error_message

    def submit_job(self, **jobparams):
        # create and submit a job to lancium
        tmp_log = core_utils.make_logger(base_logger, "queue_name={0}".format(self.queue_name), method_name="submit_job")

        try:
            tmp_log.debug("Creating and submitting a job")

            job = Job().create(**jobparams)
            tmp_log.debug("Job created. name: {0}, id: {1}, status: {2}".format(job.name, job.id, job.status))

            job.submit()
            tmp_log.debug("Job submitted. name: {0}, id: {1}, status: {2}".format(job.name, job.id, job.status))
            batch_id = str(job.id)
            return True, batch_id
        except Exception as _e:
            error_message = "Failed to create or submit a job with {0}".format(_e)
            tmp_log.error("Failed to create or submit a job with {0}".format(traceback.format_exc()))
            return False, error_message

    def delete_job(self, job_id):
        # delete job by job ID
        tmp_log = core_utils.make_logger(base_logger, "queue_name={0} job_id={1}".format(self.queue_name, job_id), method_name="delete_job")
        tmp_log.debug("Going to delete job {0}".format(job_id))
        Job.delete(job_id)
        tmp_log.debug("Deleted job {0}".format(job_id))


class LanciumJobsCacheFifo(SpecialFIFOBase, metaclass=SingletonWithID):
    """
    Cache FIFO for Lancium jobs
    """

    global_lock_id = -1

    def __init__(self, target, *args, **kwargs):
        name_suffix = target.split(".")[0]
        name_suffix = re.sub("-", "_", name_suffix)
        self.titleName = "LanciumJobsCache_{0}".format(name_suffix)
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


class LanciumJobQuery(object, metaclass=SingletonWithID):
    # class lock
    classLock = threading.Lock()

    def __init__(self, cacheEnable=False, cacheRefreshInterval=None, *args, **kwargs):
        self.submission_host = str(kwargs.get("id"))
        # Make logger
        tmpLog = core_utils.make_logger(
            base_logger, "submissionHost={0} thrid={1} oid={2}".format(self.submission_host, get_ident(), id(self)), method_name="LanciumJobQuery.__init__"
        )
        # Initialize
        with self.classLock:
            tmpLog.debug("Start")
            # For cache
            self.cacheEnable = cacheEnable
            if self.cacheEnable:
                self.cache = ([], 0)
                self.cacheRefreshInterval = cacheRefreshInterval
            tmpLog.debug("Initialize done")

    def query_jobs(self, batchIDs_list=[], all_jobs=False):
        # Make logger
        tmpLog = core_utils.make_logger(base_logger, "submissionHost={0}".format(self.submission_host), method_name="LanciumJobQuery.query_jobs")
        # Start query
        tmpLog.debug("Start query")
        cache_fifo = None
        job_attr_all_dict = {}
        # make id sets
        batchIDs_set = set(batchIDs_list)
        # query from cache

        def cache_query(batch_id_set, timeout=60):
            # query from lancium job and update cache to fifo
            def update_cache(lockInterval=90):
                tmpLog.debug("update_cache")
                # acquire lock with score timestamp
                score = time.time() - self.cacheRefreshInterval + lockInterval
                lock_key = cache_fifo.lock(score=score)
                if lock_key is not None:
                    # acquired lock, update
                    tmpLog.debug("got lock, updating cache")
                    all_jobs_light_list = Job().all()
                    jobs_iter = []
                    for job in all_jobs_light_list:
                        try:
                            lancium_job_id = job.id
                            one_job_attr = Job().get(lancium_job_id)
                            one_job_dict = dict()
                            for attr in LANCIUM_JOB_ATTRS_LIST:
                                one_job_dict[attr] = getattr(one_job_attr, attr, None)
                            jobs_iter.append(one_job_dict)
                        except Exception as e:
                            tmpLog.error("In update_cache all job; got exception {0}: {1} ; {2}".format(e.__class__.__name__, e, repr(job)))
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

        def direct_query(batch_id_set, **kwargs):
            jobs_iter = []
            batch_ids = batch_id_set
            if all_jobs:
                try:
                    all_jobs_light_list = Job().all()
                    batch_ids = set()
                    for job in all_jobs_light_list:
                        lancium_job_id = job.id
                        batch_ids.add(lancium_job_id)
                except Exception as e:
                    tmpLog.error("In doing Job().all(); got exception {0}: {1} ".format(e.__class__.__name__, e))
            for batch_id in batch_id_set:
                try:
                    lancium_job_id = batch_id
                    one_job_attr = Job().get(lancium_job_id)
                    one_job_dict = dict()
                    for attr in LANCIUM_JOB_ATTRS_LIST:
                        one_job_dict[attr] = getattr(one_job_attr, attr, None)
                    jobs_iter.append(one_job_dict)
                except Exception as e:
                    tmpLog.error("In doing Job().get({0}); got exception {1}: {2} ".format(batch_id, e.__class__.__name__, e))
            return jobs_iter

        # query method options
        query_method_list = [direct_query]
        if self.cacheEnable:
            cache_fifo = LanciumJobsCacheFifo(target=self.submission_host, idlist="{0},{1}".format(self.submission_host, get_ident()))
            query_method_list.insert(0, cache_query)
        # Go
        for query_method in query_method_list:
            # Query
            jobs_iter = query_method(batch_id_set=batchIDs_set)
            for job in jobs_iter:
                try:
                    job_attr_dict = dict(job)
                    batch_id = job_attr_dict["id"]
                except Exception as e:
                    tmpLog.error("in querying; got exception {0}: {1} ; {2}".format(e.__class__.__name__, e, repr(job)))
                else:
                    full_batch_id = get_full_batch_id(self.submission_host, batch_id)
                    job_attr_all_dict[full_batch_id] = job_attr_dict
                # Remove batch jobs already gotten from the list
                if not all_jobs:
                    batchIDs_set.discard(batch_id)
            if len(batchIDs_set) == 0 or all_jobs:
                break
        # Remaining
        if not all_jobs and len(batchIDs_set) > 0:
            # Job unfound, marked as unknown worker in harvester
            for batch_id in batchIDs_set:
                full_batch_id = get_full_batch_id(self.submission_host, batch_id)
                job_attr_all_dict[full_batch_id] = dict()
            tmpLog.info("Unfound batch jobs of submissionHost={0}: {1}".format(self.submission_host, " ".join(list(batchIDs_set))))
        # Return
        return job_attr_all_dict
