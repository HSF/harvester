"""
utilities

"""

import os
import sys
import time
import zlib
import uuid
import math
import fcntl
import codecs
import base64
import random
import inspect
import datetime
import threading
import traceback
import functools
import Cryptodome.Random
import Cryptodome.Hash.HMAC
import Cryptodome.Cipher.AES
from future.utils import iteritems
from contextlib import contextmanager

try:
    from threading import get_ident
except ImportError:
    from thread import get_ident

try:
    import cPickle as pickle
except ImportError:
    import pickle

from .work_spec import WorkSpec
from .file_spec import FileSpec
from .event_spec import EventSpec
from pandalogger.PandaLogger import PandaLogger
from pandalogger.LogWrapper import LogWrapper
from pandaharvester.harvesterconfig import harvester_config


with_memory_profile = False


# lock for synchronization
sync_lock = threading.Lock()


# synchronize decorator
def synchronize(func):
    """ synchronize decorator """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with sync_lock:
            return func(*args, **kwargs)
    return wrapper


# stopwatch class
class StopWatch(object):
    # constructor
    def __init__(self):
        self.startTime = datetime.datetime.utcnow()

    # get elapsed time
    def get_elapsed_time(self):
        diff = datetime.datetime.utcnow() - self.startTime
        return " : took {0}.{1:03} sec".format(diff.seconds + diff.days * 24 * 3600,
                                               diff.microseconds // 1000)

    # get elapsed time in seconds
    def get_elapsed_time_in_sec(self, precise=False):
        diff = datetime.datetime.utcnow() - self.startTime
        if precise:
            return diff.seconds + diff.days * 24 * 3600 + diff.microseconds * 1e-6
        else:
            return diff.seconds + diff.days * 24 * 3600

    # reset
    def reset(self):
        self.startTime = datetime.datetime.utcnow()


# map with lock
class MapWithLock(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.dataMap = dict()

    def __getitem__(self, item):
        ret = self.dataMap.__getitem__(item)
        return ret

    def __setitem__(self, item, value):
        self.dataMap.__setitem__(item, value)

    def __contains__(self, item):
        ret = self.dataMap.__contains__(item)
        return ret

    def acquire(self):
        self.lock.acquire()

    def release(self):
        self.lock.release()

    def iteritems(self):
        return iteritems(self.dataMap)


# singleton distinguishable with id
class SingletonWithID(type):
    def __init__(cls, *args,**kwargs):
        cls.__instance = {}
        super(SingletonWithID, cls).__init__(*args, **kwargs)

    @synchronize
    def __call__(cls, *args, **kwargs):
        obj_id = str(kwargs.get('id', ''))
        if obj_id not in cls.__instance:
            cls.__instance[obj_id] = super(SingletonWithID, cls).__call__(*args, **kwargs)
        return cls.__instance.get(obj_id)


# singleton distinguishable with each thread and id
class SingletonWithThreadAndID(type):
    def __init__(cls, *args,**kwargs):
        cls.__instance = {}
        super(SingletonWithThreadAndID, cls).__init__(*args, **kwargs)

    @synchronize
    def __call__(cls, *args, **kwargs):
        thread_id = get_ident()
        obj_id = (thread_id, str(kwargs.get('id', '')))
        if obj_id not in cls.__instance:
            cls.__instance[obj_id] = super(SingletonWithThreadAndID, cls).__call__(*args, **kwargs)
        return cls.__instance.get(obj_id)


# enable memory profiling
def enable_memory_profiling():
    global with_memory_profile
    with_memory_profile = True


# setup logger
def setup_logger(name=None):
    if name is None:
        frm = inspect.stack()[1][0]
        mod = inspect.getmodule(frm)
        name = mod.__name__.split('.')[-1]
    try:
        log_level = getattr(harvester_config.log_level, name)
        return PandaLogger().getLogger(name, log_level=log_level)
    except Exception:
        pass
    return PandaLogger().getLogger(name)


# make logger
def make_logger(tmp_log, token=None, method_name=None, hook=None):
    # get method name of caller
    if method_name is None:
        tmpStr = inspect.stack()[1][3]
    else:
        tmpStr = method_name
    if token is not None:
        tmpStr += ' <{0}>'.format(token)
    else:
        tmpStr += ' :'.format(token)
    newLog = LogWrapper(tmp_log, tmpStr, seeMem=with_memory_profile, hook=hook)
    return newLog


# dump error message
def dump_error_message(tmp_log, err_str=None, no_message=False):
    if not isinstance(tmp_log, LogWrapper):
        methodName = '{0} : '.format(inspect.stack()[1][3])
    else:
        methodName = ''
    # error
    if err_str is None:
        errtype, errvalue = sys.exc_info()[:2]
        err_str = "{0} {1} {2} ".format(methodName, errtype.__name__, errvalue)
        err_str += traceback.format_exc()
    if not no_message:
        tmp_log.error(err_str)
    return err_str


# sleep for random duration and return True if no more sleep is needed
def sleep(interval, stop_event, randomize=True):
    if randomize and interval > 0:
        randInterval = random.randint(int(interval * 0.4), int(interval * 1.4))
    else:
        randInterval = interval
    if stop_event is None:
        time.sleep(randInterval)
    else:
        i = 0
        while True:
            if stop_event.is_set():
                return True
            if i >= randInterval:
                break
            stop_event.wait(1)
            i += 1
    return False


# make PFC
def make_pool_file_catalog(jobspec_list):
    xmlStr = """<?xml version="1.0" ?>
<!DOCTYPE POOLFILECATALOG  SYSTEM "InMemory">
<POOLFILECATALOG>
    """
    doneLFNs = set()
    for jobSpec in jobspec_list:
        inFiles = jobSpec.get_input_file_attributes()
        for inLFN, inFile in iteritems(inFiles):
            if inLFN in doneLFNs:
                continue
            doneLFNs.add(inLFN)
            xmlStr += """  <File ID="{guid}">
    <physical>
      <pfn filetype="ROOT_All" name="{lfn}"/>
    </physical>
    <logical/>
  </File>
  """.format(guid=inFile['guid'], lfn=inLFN)
    xmlStr += "</POOLFILECATALOG>"
    return xmlStr


# calculate adler32
def calc_adler32(file_name):
    val = 1
    blockSize = 32 * 1024 * 1024
    with open(file_name, 'rb') as fp:
        while True:
            data = fp.read(blockSize)
            if not data:
                break
            val = zlib.adler32(data, val)
    if val < 0:
        val += 2 ** 32
    return hex(val)[2:10].zfill(8).lower()


# get output file report
def get_output_file_report(jobspec):
    if jobspec.outputFilesToReport is not None:
        return jobspec.outputFilesToReport
    # header
    xml = """<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
    <!-- ATLAS file meta-data catalog -->
    <!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
    <POOLFILECATALOG>
    """
    # body
    for fileSpec in jobspec.outFiles:
        # only successful files
        if fileSpec.status != 'finished':
            continue
        # extract guid
        if 'guid' in fileSpec.fileAttributes:
            guid = fileSpec.fileAttributes['guid']
        elif fileSpec.fileType == 'log':
            guid = jobspec.get_logfile_info()['guid']
        else:
            guid = str(uuid.uuid4())
        # checksum
        if fileSpec.chksum is not None and ':' in fileSpec.chksum:
            chksum = fileSpec.chksum.split(':')[-1]
        else:
            chksum = fileSpec.chksum
        xml += """<File ID="{guid}">
        <logical>
        <lfn name="{lfn}"/>
        </logical>
        <metadata att_name="fsize" att_value = "{fsize}"/>
        <metadata att_name="adler32" att_value="{chksum}"/>
        </File>
        """.format(guid=guid, lfn=fileSpec.lfn, fsize=fileSpec.fsize, chksum=chksum)
    # skipped files
    skippedLFNs = jobspec.get_one_attribute('skippedInputs')
    if skippedLFNs:
        for tmpLFN in skippedLFNs:
            xml += """<File ID="">
            <logical>
            <lfn name="{lfn}"/>
            </logical>
            <metadata att_name="fsize" att_value = "0"/>
            <metadata att_name="adler32" att_value=""/>
            </File>
            """.format(lfn=tmpLFN)
    # tailor
    xml += """
    </POOLFILECATALOG>
    """
    return xml


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


# update job attributes with workers
def update_job_attributes_with_workers(map_type, jobspec_list, workspec_list, files_to_stage_out_list,
                                       events_to_update_list):
    if map_type in [WorkSpec.MT_OneToOne, WorkSpec.MT_MultiJobs]:
        workSpec = workspec_list[0]
        for jobSpec in jobspec_list:
            jobSpec.set_attributes(workSpec.workAttributes)
            # delete job metadata from worker attributes
            try:
                del workSpec.workAttributes[jobSpec.PandaID]['metaData']
            except Exception:
                pass
            # set start and end times
            if workSpec.status in [WorkSpec.ST_running]:
                jobSpec.set_start_time()
            elif workSpec.pilot_closed:
                jobSpec.reset_start_end_time()
            elif workSpec.is_final_status():
                jobSpec.set_end_time()
            # core count
            if workSpec.nCore is not None and jobSpec.nCore is None:
                try:
                    jobSpec.nCore = int(workSpec.nCore / len(jobspec_list))
                    if jobSpec.nCore == 0:
                        jobSpec.nCore = 1
                except Exception:
                    pass
            # batch ID
            if not jobSpec.has_attribute('batchID'):
                if workSpec.batchID is not None:
                    jobSpec.set_one_attribute('batchID', workSpec.batchID)
            # add files
            outFileAttrs = jobSpec.get_output_file_attributes()
            for tmpWorkerID, files_to_stage_out in iteritems(files_to_stage_out_list):
                if jobSpec.PandaID in files_to_stage_out:
                    for lfn, fileAttersList in iteritems(files_to_stage_out[jobSpec.PandaID]):
                        for fileAtters in fileAttersList:
                            fileSpec = FileSpec()
                            fileSpec.lfn = lfn
                            fileSpec.PandaID = jobSpec.PandaID
                            fileSpec.taskID = jobSpec.taskID
                            fileSpec.path = fileAtters['path']
                            fileSpec.fsize = fileAtters['fsize']
                            fileSpec.fileType = fileAtters['type']
                            fileSpec.fileAttributes = fileAtters
                            fileSpec.workerID = tmpWorkerID
                            if 'isZip' in fileAtters:
                                fileSpec.isZip = fileAtters['isZip']
                            if 'chksum' in fileAtters:
                                fileSpec.chksum = fileAtters['chksum']
                            if 'eventRangeID' in fileAtters:
                                fileSpec.eventRangeID = fileAtters['eventRangeID']
                                # use input fileID as provenanceID
                                try:
                                    provenanceID = fileSpec.eventRangeID.split('-')[2]
                                except Exception:
                                    provenanceID = None
                                fileSpec.provenanceID = provenanceID
                            if lfn in outFileAttrs:
                                fileSpec.scope = outFileAttrs[lfn]['scope']
                            jobSpec.add_out_file(fileSpec)
            # add events
            for events_to_update in events_to_update_list:
                if jobSpec.PandaID in events_to_update:
                    for data in events_to_update[jobSpec.PandaID]:
                        eventSpec = EventSpec()
                        eventSpec.from_data(data, jobSpec.PandaID)
                        jobSpec.add_event(eventSpec, None)
            statusInJobAttr = jobSpec.get_job_status_from_attributes()
            jobSpec.status, jobSpec.subStatus = workSpec.convert_to_job_status(statusInJobAttr)
            if workSpec.pilot_closed:
                jobSpec.set_pilot_closed()
            if workSpec.new_status:
                jobSpec.trigger_propagation()
    elif map_type == WorkSpec.MT_MultiWorkers:
        jobSpec = jobspec_list[0]
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
            # the the worker is running
            if workSpec.status in [WorkSpec.ST_running]:
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
                    jobSpec.nCore = float(nCoreTime) / float(total_time)
                    jobSpec.nCore = int(math.ceil(jobSpec.nCore))
            # disable to get more workers
            jobSpec.moreWorkers = 0
        else:
            # live core count
            jobSpec.nCore = nCore
        # combine worker attributes and set it to job
        # FIXME
        # jobSpec.set_attributes(workAttributes)
        # add files
        outFileAttrs = jobSpec.get_output_file_attributes()
        for tmpWorkerID, files_to_stage_out in iteritems(files_to_stage_out_list):
            if jobSpec.PandaID in files_to_stage_out:
                for lfn, fileAttersList in iteritems(files_to_stage_out[jobSpec.PandaID]):
                    for fileAtters in fileAttersList:
                        fileSpec = FileSpec()
                        fileSpec.lfn = lfn
                        fileSpec.PandaID = jobSpec.PandaID
                        fileSpec.taskID = jobSpec.taskID
                        fileSpec.path = fileAtters['path']
                        fileSpec.fsize = fileAtters['fsize']
                        fileSpec.fileType = fileAtters['type']
                        fileSpec.fileAttributes = fileAtters
                        fileSpec.workerID = tmpWorkerID
                        if 'isZip' in fileAtters:
                            fileSpec.isZip = fileAtters['isZip']
                        if 'chksum' in fileAtters:
                            fileSpec.chksum = fileAtters['chksum']
                        if 'eventRangeID' in fileAtters:
                            fileSpec.eventRangeID = fileAtters['eventRangeID']
                            # use input fileID as provenanceID
                            try:
                                provenanceID = fileSpec.eventRangeID.split('-')[2]
                            except Exception:
                                provenanceID = None
                            fileSpec.provenanceID = provenanceID
                        if lfn in outFileAttrs:
                            fileSpec.scope = outFileAttrs[lfn]['scope']
                        jobSpec.add_out_file(fileSpec)
        # add events
        for events_to_update in events_to_update_list:
            if jobSpec.PandaID in events_to_update:
                for data in events_to_update[jobSpec.PandaID]:
                    eventSpec = EventSpec()
                    eventSpec.from_data(data, jobSpec.PandaID)
                    jobSpec.add_event(eventSpec, None)
        # set job status
        workSpec = workspec_list[0]
        if allDone:
            if oneFinished:
                jobSpec.status, jobSpec.subStatus = workSpec.convert_to_job_status(WorkSpec.ST_finished)
            elif oneFailed:
                jobSpec.status, jobSpec.subStatus = workSpec.convert_to_job_status(WorkSpec.ST_failed)
            else:
                jobSpec.status, jobSpec.subStatus = workSpec.convert_to_job_status(WorkSpec.ST_cancelled)
        else:
            if isRunning or jobSpec.status == 'running':
                jobSpec.status, jobSpec.subStatus = workSpec.convert_to_job_status(WorkSpec.ST_running)
            else:
                jobSpec.status, jobSpec.subStatus = workSpec.convert_to_job_status(WorkSpec.ST_submitted)
    return True


# rollover for log files
def do_log_rollover():
    PandaLogger.doRollOver()


# get stopwatch
def get_stopwatch():
    return StopWatch()


# global dict for all threads
global_dict = MapWithLock()


# get global dict
def get_global_dict():
    return global_dict


# get file lock
@contextmanager
def get_file_lock(file_name, lock_interval):
    if os.path.exists(file_name):
        opt = 'r+'
    else:
        opt = 'w+'
    with open(file_name, opt) as f:
        locked = False
        try:
            # lock file
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
            # read timestamp
            timeNow = datetime.datetime.utcnow()
            toSkip = False
            try:
                s = f.read()
                pTime = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
                if timeNow - pTime < datetime.timedelta(seconds=lock_interval):
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
    v = cipher_text[:Cryptodome.Cipher.AES.block_size]
    c = Cryptodome.Cipher.AES.new(k, Cryptodome.Cipher.AES.MODE_CFB, v)
    cipher_text = cipher_text[Cryptodome.Cipher.AES.block_size:]
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


# get URL of queues config file
def get_queues_config_url():
    try:
        return os.environ['HARVESTER_QUEUE_CONFIG_URL']
    except Exception:
        return None


# get unique queue name
def get_unique_queue_name(queue_name, resource_type, job_type):
    return '{0}:{1}:{2}'.format(queue_name, resource_type, job_type)


# capability to dynamically change plugins
def dynamic_plugin_change():
    try:
        return harvester_config.master.dynamic_plugin_change
    except Exception:
        return True


# replacement for slow namedtuple in python 2
class DictTupleHybrid(tuple):
    def set_attributes(self, attributes):
        self.attributes = attributes

    def _asdict(self):
        return dict(zip(self.attributes, self))


# Make a list of choice candidates according to permille weight
def make_choice_list(pdpm={}, default=None):
    weight_sum = sum(pdpm.values())
    weight_default = 1000
    ret_list = []
    for candidate, weight in iteritems(pdpm):
        if weight_sum > 1000:
            real_weight = int(weight * 1000 / weight_sum)
        else:
            real_weight = int(weight)
        ret_list.extend([candidate]*real_weight)
        weight_default -= real_weight
    ret_list.extend([default]*weight_default)
    return ret_list


# pickle to text
def pickle_to_text(data):
    return codecs.encode(pickle.dumps(data), 'base64').decode()


# unpickle from text
def unpickle_from_text(text):
    return pickle.loads(codecs.decode(text.encode(), 'base64'))


# increasing retry period after timeout or failure
def retry_period_sec(nth_retry, increment=1, max_retries=None, max_seconds=None, min_seconds=1):
    nth = max(nth_retry, 1)
    ret_period = max(min_seconds, 1)
    if max_retries and nth_retry > max_retries:
        return False
    else:
        ret_period += (nth - 1)*increment
        if max_seconds:
            ret_period = min(ret_period, max_seconds)
        return ret_period


# safe dictionary to retrun original strings for missing keys
class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'
