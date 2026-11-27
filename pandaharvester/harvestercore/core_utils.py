"""
utilities

"""

import base64
import codecs
import fcntl
import functools
import json
import math
import os
import pickle
import random
import socket
import sys
import threading
import time
import traceback
import uuid
import zlib
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from threading import get_ident

import Cryptodome.Cipher.AES
import Cryptodome.Hash.HMAC
import Cryptodome.Random
from pandalogger.LogWrapper import LogWrapper
from pandalogger.PandaLogger import PandaLogger

from pandaharvester.harvesterconfig import harvester_config

from .event_spec import EventSpec
from .file_spec import FileSpec
from .work_spec import WorkSpec

with_memory_profile = False


# lock for synchronization
sync_lock = threading.Lock()

##############
# Decorators #
##############


# synchronize decorator
def synchronize(func):
    """synchronize decorator"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with sync_lock:
            return func(*args, **kwargs)

    return wrapper


###########
# Classes #
###########


# stopwatch class
class StopWatch(object):
    # constructor
    def __init__(self):
        self.start_time = time.monotonic()

    # get string message about elapsed time
    def get_elapsed_time(self):
        time_diff = self.get_elapsed_time_in_sec()
        return f" : took {time_diff:.3f} sec"

    # get elapsed time in seconds
    def get_elapsed_time_in_sec(self):
        return time.monotonic() - self.start_time

    # reset
    def reset(self):
        self.start_time = time.monotonic()


# map with lock
class MapWithLock(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.dataMap = dict()

    def __getitem__(self, item):
        return self.dataMap[item]

    def __setitem__(self, item, value):
        self.dataMap[item] = value

    def __contains__(self, item):
        return item in self.dataMap

    def acquire(self):
        self.lock.acquire()

    def release(self):
        self.lock.release()


# global dict for all threads
global_dict = MapWithLock()


# singleton distinguishable with id
class SingletonWithID(type):
    def __init__(cls, *args, **kwargs):
        cls.__instance = {}
        super(SingletonWithID, cls).__init__(*args, **kwargs)

    @synchronize
    def __call__(cls, *args, **kwargs):
        obj_id = str(kwargs.get("id", ""))
        if obj_id not in cls.__instance:
            cls.__instance[obj_id] = super(SingletonWithID, cls).__call__(*args, **kwargs)
        return cls.__instance.get(obj_id)


# tuple of a DB row which can also be converted to a dict of column names and values
class DictTupleHybrid(tuple):
    def set_attributes(self, attributes):
        self.attributes = attributes

    def _asdict(self):
        return dict(zip(self.attributes, self))


# safe dictionary to return original strings for missing keys
class SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"


#############
# Functions #
#############


# enable memory profiling
def enable_memory_profiling():
    global with_memory_profile
    with_memory_profile = True


# setup logger; the name is the module name of the caller by default
def setup_logger(name=None):
    if name is None:
        name = sys._getframe(1).f_globals["__name__"].split(".")[-1]
    try:
        log_level = getattr(harvester_config.log_level, name)
        return PandaLogger().getLogger(name, log_level=log_level)
    except Exception:
        pass
    return PandaLogger().getLogger(name)


# make logger; the method name is the name of the caller by default
def make_logger(tmp_log, token=None, method_name=None, hook=None):
    if method_name is None:
        method_name = sys._getframe(1).f_code.co_name
    if token is not None:
        prefix = f"{method_name} <{token}>"
    else:
        prefix = f"{method_name} :"
    return LogWrapper(tmp_log, prefix, seeMem=with_memory_profile, hook=hook)


# dump error message
def dump_error_message(tmp_log, err_str=None, no_message=False):
    if not isinstance(tmp_log, LogWrapper):
        method_name = f"{sys._getframe(1).f_code.co_name} : "
    else:
        method_name = ""
    # error
    if err_str is None:
        err_type, err_value = sys.exc_info()[:2]
        err_str = f"{method_name} {err_type.__name__} {err_value} {traceback.format_exc()}"
    if not no_message:
        tmp_log.error(err_str)
    return err_str


# sleep for random duration and return True if no more sleep is needed
def sleep(interval, stop_event, randomize=True):
    if randomize and interval > 0:
        random_interval = random.randint(int(interval * 0.4), int(interval * 1.4))
    else:
        random_interval = interval
    if stop_event is None:
        time.sleep(random_interval)
    else:
        i = 0
        while True:
            if stop_event.is_set():
                return True
            if i >= random_interval:
                break
            stop_event.wait(1)
            i += 1
    return False


# make PFC
def make_pool_file_catalog(jobspec_list):
    xml_str = """<?xml version="1.0" ?>
<!DOCTYPE POOLFILECATALOG  SYSTEM "InMemory">
<POOLFILECATALOG>
    """

    done_lfns = set()
    for jobSpec in jobspec_list:
        input_files = jobSpec.get_input_file_attributes()
        for input_lfn, input_file in input_files.items():
            if input_lfn in done_lfns:
                continue
            done_lfns.add(input_lfn)
            xml_str += f"""  <File ID="{input_file['guid']}">
    <physical>
      <pfn filetype="ROOT_All" name="{input_lfn}"/>
    </physical>
    <logical/>
  </File>
  """

    xml_str += "</POOLFILECATALOG>"
    return xml_str


# calculate adler32
def calc_adler32(file_name):
    val = 1
    block_size = 32 * 1024 * 1024
    with open(file_name, "rb") as fp:
        while True:
            data = fp.read(block_size)
            if not data:
                break
            val = zlib.adler32(data, val)
    return f"{val:08x}"


# get output file report
def get_output_file_report(jobspec):
    if jobspec.outputFilesToReport is not None:
        return jobspec.outputFilesToReport
    report = {}
    # body
    for fileSpec in jobspec.outFiles:
        # only successful files
        if fileSpec.status != "finished":
            continue
        # extract guid
        if "guid" in fileSpec.fileAttributes:
            guid = fileSpec.fileAttributes["guid"]
        elif fileSpec.fileType == "log":
            guid = jobspec.get_logfile_info()["guid"]
        else:
            guid = str(uuid.uuid4())
        # checksum
        if fileSpec.chksum is not None and ":" in fileSpec.chksum:
            chksum = fileSpec.chksum.split(":")[-1]
        else:
            chksum = fileSpec.chksum
        report[fileSpec.lfn] = {"guid": guid, "fsize": fileSpec.fsize, "adler32": chksum}
    return json.dumps(report)


def create_shards(input_list, size):
    """
    Creates shards of size n from the input list.
    """
    shard, i = [], 0
    for element in input_list:
        shard.append(element)
        i += 1
        if i == size:
            yield shard
            shard, i = [], 0

    if i > 0:
        yield shard


# add output files of a job reported by workers, a dict of {workerID: {PandaID: {lfn: [file attributes, ...]}}}
def _add_output_files_to_job(jobspec, files_to_stage_out_list):
    out_file_attrs = jobspec.get_output_file_attributes()
    for worker_id, files_to_stage_out in files_to_stage_out_list.items():
        for lfn, file_attrs_list in files_to_stage_out.get(jobspec.PandaID, {}).items():
            for file_attrs in file_attrs_list:
                file_spec = FileSpec()
                file_spec.lfn = lfn
                file_spec.PandaID = jobspec.PandaID
                file_spec.taskID = jobspec.taskID
                file_spec.path = file_attrs["path"]
                file_spec.fsize = file_attrs["fsize"]
                file_spec.fileType = file_attrs["type"]
                file_spec.fileAttributes = file_attrs
                file_spec.workerID = worker_id
                if "isZip" in file_attrs:
                    file_spec.isZip = file_attrs["isZip"]
                if "chksum" in file_attrs:
                    file_spec.chksum = file_attrs["chksum"]
                if "eventRangeID" in file_attrs:
                    file_spec.eventRangeID = file_attrs["eventRangeID"]
                    # use input fileID as provenanceID
                    try:
                        file_spec.provenanceID = file_spec.eventRangeID.split("-")[2]
                    except Exception:
                        file_spec.provenanceID = None
                if lfn in out_file_attrs:
                    file_spec.scope = out_file_attrs[lfn]["scope"]
                jobspec.add_out_file(file_spec)


# add events of a job reported by workers, a list of {PandaID: [event data, ...]}
def _add_events_to_job(jobspec, events_to_update_list):
    for events_to_update in events_to_update_list:
        for data in events_to_update.get(jobspec.PandaID, []):
            event_spec = EventSpec()
            event_spec.from_data(data, jobspec.PandaID)
            jobspec.add_event(event_spec, None)


# update jobs with one worker, for OneToOne and ManyToOne (MultiJobs)
def _update_jobs_with_one_worker(jobspec_list, workspec, files_to_stage_out_list, events_to_update_list):
    for jobSpec in jobspec_list:
        jobSpec.set_attributes(workspec.workAttributes)
        # delete job metadata from worker attributes
        try:
            del workspec.workAttributes[jobSpec.PandaID]["metaData"]
        except Exception:
            pass
        # set start and end times
        if workspec.status == WorkSpec.ST_running:
            jobSpec.set_start_time()
        elif workspec.pilot_closed:
            jobSpec.reset_start_end_time()
        elif workspec.is_final_status():
            jobSpec.set_end_time()
        # core count, shared by the jobs
        if workspec.nCore is not None and jobSpec.nCore is None:
            try:
                jobSpec.nCore = max(int(workspec.nCore / len(jobspec_list)), 1)
            except Exception:
                pass
        # batch ID
        if not jobSpec.has_attribute("batchID") and workspec.batchID is not None:
            jobSpec.set_one_attribute("batchID", workspec.batchID)
        _add_output_files_to_job(jobSpec, files_to_stage_out_list)
        _add_events_to_job(jobSpec, events_to_update_list)
        # status
        statusInJobAttr = jobSpec.get_job_status_from_attributes()
        jobSpec.status, jobSpec.subStatus = workspec.convert_to_job_status(statusInJobAttr)
        if workspec.pilot_closed:
            jobSpec.set_pilot_closed()
        if workspec.new_status:
            jobSpec.trigger_propagation()


# update a job with many workers, for OneToMany (MultiWorkers)
def _update_job_with_many_workers(jobSpec, workspec_list, files_to_stage_out_list, events_to_update_list):
    # scan all workers
    allDone = True
    isRunning = False
    oneFinished = False
    oneFailed = False
    nCore = 0
    nCoreTime = 0
    for workSpec in workspec_list:
        if workSpec.new_status:
            jobSpec.trigger_propagation()
        # the worker is running
        if workSpec.status == WorkSpec.ST_running:
            isRunning = True
            # set start time
            jobSpec.set_start_time()
            nCore += workSpec.nCore
        # the worker is done
        if workSpec.is_final_status():
            if workSpec.startTime is not None and workSpec.endTime is not None:
                nCoreTime += workSpec.nCore * (workSpec.endTime - workSpec.startTime).total_seconds()
            if workSpec.status == WorkSpec.ST_finished:
                oneFinished = True
            elif workSpec.status == WorkSpec.ST_failed:
                oneFailed = True
        else:
            # the worker is still active
            allDone = False
    # set final values
    if allDone:
        # set end time
        jobSpec.set_end_time()
        # time-averaged core count
        if jobSpec.startTime is not None:
            total_time = (jobSpec.endTime - jobSpec.startTime).total_seconds()
            if total_time > 0:
                jobSpec.nCore = int(math.ceil(nCoreTime / total_time))
        # disable to get more workers
        jobSpec.moreWorkers = 0
    else:
        # live core count
        jobSpec.nCore = nCore
    # worker attributes are not set to the job, since attributes from many workers would need to be merged without conflicts
    _add_output_files_to_job(jobSpec, files_to_stage_out_list)
    _add_events_to_job(jobSpec, events_to_update_list)
    # set job status
    if allDone:
        if oneFinished:
            job_status = WorkSpec.ST_finished
        elif oneFailed:
            job_status = WorkSpec.ST_failed
        else:
            job_status = WorkSpec.ST_cancelled
    elif isRunning or jobSpec.status == "running":
        job_status = WorkSpec.ST_running
    else:
        job_status = WorkSpec.ST_submitted
    jobSpec.status, jobSpec.subStatus = workspec_list[0].convert_to_job_status(job_status)


# update job attributes with workers
def update_job_attributes_with_workers(map_type, jobspec_list, workspec_list, files_to_stage_out_list, events_to_update_list):
    if map_type in [WorkSpec.MT_OneToOne, WorkSpec.MT_MultiJobs]:
        _update_jobs_with_one_worker(jobspec_list, workspec_list[0], files_to_stage_out_list, events_to_update_list)
    elif map_type == WorkSpec.MT_MultiWorkers:
        _update_job_with_many_workers(jobspec_list[0], workspec_list, files_to_stage_out_list, events_to_update_list)
    return True


# rollover for log files
def do_log_rollover():
    PandaLogger.doRollOver()


# get stopwatch
def get_stopwatch():
    return StopWatch()


# get global dict
def get_global_dict():
    return global_dict


# get file lock
@contextmanager
def get_file_lock(file_name, lock_interval):
    if os.path.exists(file_name):
        opt = "r+"
    else:
        opt = "w+"
    with open(file_name, opt) as f:
        locked = False
        try:
            # lock file
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
            # read timestamp
            timeNow = naive_utcnow()
            toSkip = False
            try:
                s = f.read()
                pTime = datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
                if timeNow - pTime < timedelta(seconds=lock_interval):
                    toSkip = True
            except Exception:
                pass
            # skip if still in locked interval
            if toSkip:
                raise IOError("skipped since still in locked interval")
            # write timestamp
            f.seek(0)
            f.write(timeNow.strftime("%Y-%m-%d %H:%M:%S.%f"))
            f.truncate()
            # execute with block
            yield
        finally:
            # unlock
            if locked:
                fcntl.flock(f, fcntl.LOCK_UN)


# convert a key phrase to a cipher key
def convert_phrase_to_key(key_phrase):
    h = Cryptodome.Hash.HMAC.new(key_phrase)
    return h.hexdigest()


# encrypt a string
def encrypt_string(key_phrase, plain_text):
    k = convert_phrase_to_key(key_phrase)
    v = Cryptodome.Random.new().read(Cryptodome.Cipher.AES.block_size)
    c = Cryptodome.Cipher.AES.new(k, Cryptodome.Cipher.AES.MODE_CFB, v)
    return base64.b64encode(v + c.encrypt(plain_text))


# decrypt a string
def decrypt_string(key_phrase, cipher_text):
    cipher_text = base64.b64decode(cipher_text)
    k = convert_phrase_to_key(key_phrase)
    v = cipher_text[: Cryptodome.Cipher.AES.block_size]
    c = Cryptodome.Cipher.AES.new(k, Cryptodome.Cipher.AES.MODE_CFB, v)
    cipher_text = cipher_text[Cryptodome.Cipher.AES.block_size :]
    return c.decrypt(cipher_text)


# set permission
def set_file_permission(path):
    if not os.path.exists(path):
        return
    targets = []
    if os.path.isfile(path):
        targets += [path]
    else:
        for root, dirs, files in os.walk(path):
            targets += [os.path.join(root, f) for f in files]
    umask = os.umask(0)
    uid = os.getuid()
    gid = os.getgid()
    for f in targets:
        try:
            os.chmod(f, 0o666 - umask)
            os.chown(f, uid, gid)
        except Exception:
            pass
    os.umask(umask)


# get unique queue name
def get_unique_queue_name(queue_name, resource_type, job_type):
    return f"{queue_name}:{resource_type}:{job_type}"


# capability to dynamically change plugins
def dynamic_plugin_change():
    return getattr(harvester_config.master, "dynamic_plugin_change", True)


# Make a list of choice candidates according to permille weight
def make_choice_list(pdpm=None, default=None):
    if pdpm is None:
        pdpm = {}
    weight_sum = sum(pdpm.values())
    weight_default = 1000
    ret_list = []
    for candidate, weight in pdpm.items():
        if weight_sum > 1000:
            real_weight = int(weight * 1000 / weight_sum)
        else:
            real_weight = int(weight)
        ret_list.extend([candidate] * real_weight)
        weight_default -= real_weight
    ret_list.extend([default] * weight_default)
    return ret_list


# pickle to text
def pickle_to_text(data):
    return codecs.encode(pickle.dumps(data), "base64").decode()


# unpickle from text
def unpickle_from_text(text):
    return pickle.loads(codecs.decode(text.encode(), "base64"))


# increasing retry period after timeout or failure
def retry_period_sec(nth_retry, increment=1, max_retries=None, max_seconds=None, min_seconds=1):
    nth = max(nth_retry, 1)
    ret_period = max(min_seconds, 1)
    if max_retries and nth_retry > max_retries:
        return False
    else:
        ret_period += (nth - 1) * increment
        if max_seconds:
            ret_period = min(ret_period, max_seconds)
        return ret_period


# get process identifier on the fly
def get_pid(with_hostname=True, with_thread_id=True):
    ret = f"{os.getpid()}"
    if with_hostname:
        ret = f"{socket.gethostname()}_{ret}"
    if with_thread_id:
        ret = f"{ret}-{get_ident():x}"
    return ret


def aware_utcnow() -> datetime:
    """
    Return the current UTC date and time, with tzinfo timezone.utc

    Returns:
        datetime: current UTC date and time, with tzinfo timezone.utc
    """
    return datetime.now(timezone.utc)


def aware_utcfromtimestamp(timestamp: float) -> datetime:
    """
    Return the local date and time, with tzinfo timezone.utc, corresponding to the POSIX timestamp

    Args:
        timestamp (float): POSIX timestamp

    Returns:
        datetime: current UTC date and time, with tzinfo timezone.utc
    """
    return datetime.fromtimestamp(timestamp, timezone.utc)


def naive_utcnow() -> datetime:
    """
    Return the current UTC date and time, without tzinfo

    Returns:
        datetime: current UTC date and time, without tzinfo
    """
    return aware_utcnow().replace(tzinfo=None)


def naive_utcfromtimestamp(timestamp: float) -> datetime:
    """
    Return the local date and time, without tzinfo, corresponding to the POSIX timestamp

    Args:
        timestamp (float): POSIX timestamp

    Returns:
        datetime: current UTC date and time, without tzinfo
    """
    return aware_utcfromtimestamp(timestamp).replace(tzinfo=None)


def naive_utc_to_timestamp(naive_utc_time: datetime) -> float:
    """
    Return the POSIX timestamp of a naive datetime in UTC; the inverse of naive_utcfromtimestamp
    Note that naive_utc_time.timestamp() would take it as local time

    Args:
        naive_utc_time (datetime): UTC date and time without tzinfo

    Returns:
        float: POSIX timestamp
    """
    return naive_utc_time.replace(tzinfo=timezone.utc).timestamp()


def special_pilot_type_to_prod_source_label(pilot_type: str) -> str:
    """
    Convert special pilotType of worker to prodSourceLabel of PanDA job.

    Args:
        pilot_type (str): pilotType of worker, e.g. "RC", "ALRB", "PT" (except "PR" which is production)

    Returns:
        str: prodSourceLabel of PanDA job, e.g. "rc_test2", "rc_alrb", "ptest"; "ANY" if no mapping is defined for the given pilot_type
    """
    pilot_type_to_prod_source_label_map = {
        "RC": "rc_test2",
        "ALRB": "rc_alrb",
        "PT": "ptest",
    }
    return pilot_type_to_prod_source_label_map.get(pilot_type, "ANY")


def prod_source_label_to_pilot_type(prod_source_label: str) -> str:
    """
    Convert prodSourceLabel of PanDA job to pilotType of worker.

    Args:
        prod_source_label (str): prodSourceLabel of PanDA job, e.g. "rc_test2", "rc_alrb", "ptest"

    Returns:
        str: pilotType of worker, e.g. "RC", "ALRB", "PT"; default to "PR" (production) if no mapping is defined for the given prod_source_label
    """
    prod_source_label_to_pilot_type_map = {
        "rc_test2": "RC",
        "rc_alrb": "ALRB",
        "ptest": "PT",
    }
    return prod_source_label_to_pilot_type_map.get(prod_source_label, "PR")
