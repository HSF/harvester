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
import base64
import random
import inspect
import datetime
import threading
import traceback
import Crypto.Random
import Crypto.Hash.HMAC
import Crypto.Cipher.AES
from future.utils import iteritems
from contextlib import contextmanager

from .work_spec import WorkSpec
from .file_spec import FileSpec
from .event_spec import EventSpec
from pandalogger.PandaLogger import PandaLogger
from pandalogger.LogWrapper import LogWrapper


with_memory_profile = False


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
    return PandaLogger().getLogger(name)


# make logger
def make_logger(tmp_log, token=None, method_name=None):
    # get method name of caller
    if method_name is None:
        tmpStr = inspect.stack()[1][3]
    else:
        tmpStr = method_name
    if token is not None:
        tmpStr += ' <{0}>'.format(token)
    else:
        tmpStr += ' :'.format(token)
    newLog = LogWrapper(tmp_log, tmpStr, seeMem=with_memory_profile)
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
    if randomize:
        randInterval = random.randint(int(interval * 0.8), int(interval * 1.2))
    else:
        randInterval = interval
    if stop_event is None:
        time.sleep(randInterval)
    else:
        for i in range(randInterval):
            stop_event.wait(1)
            if stop_event.is_set():
                return True
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
        xml += """"<File ID="{guid}">
        <logical>
        <lfn name="{lfn}"/>
        </logical>
        <metadata att_name="fsize" att_value = "{fsize}"/>
        <metadata att_name="adler32" att_value="{chksum}"/>
        </File> """.format(guid=guid, lfn=fileSpec.lfn, fsize=fileSpec.fsize, chksum=chksum)
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
            # set start and end times
            if workSpec.status in [WorkSpec.ST_running]:
                jobSpec.set_start_time()
            elif workSpec.is_final_status():
                jobSpec.set_end_time()
            # core count
            if workSpec.nCore is not None and jobSpec.nCore is None:
                try:
                    jobSpec.nCore = int(workSpec.nCore / len(jobspec_list))
                    if jobSpec.nCore == 0:
                        jobSpec.nCore = 1
                except:
                    pass
            # batch ID
            if not jobSpec.has_attribute('batchID'):
                if workSpec.batchID is not None:
                    jobSpec.set_one_attribute('batchID', workSpec.batchID)
            # add files
            outFileAttrs = jobSpec.get_output_file_attributes()
            for files_to_stage_out in files_to_stage_out_list:
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
                            if 'isZip' in fileAtters:
                                fileSpec.isZip = fileAtters['isZip']
                            if 'chksum' in fileAtters:
                                fileSpec.chksum = fileAtters['chksum']
                            if 'eventRangeID' in fileAtters:
                                fileSpec.eventRangeID = fileAtters['eventRangeID']
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
        else:
            # live core count
            jobSpec.nCore = nCore
        # combine worker attributes and set it to job
        # FIXME
        # jobSpec.set_attributes(workAttributes)
        # add files
        outFileAttrs = jobSpec.get_output_file_attributes()
        for files_to_stage_out in files_to_stage_out_list:
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
                        if 'isZip' in fileAtters:
                            fileSpec.isZip = fileAtters['isZip']
                        if 'chksum' in fileAtters:
                            fileSpec.chksum = fileAtters['chksum']
                        if 'eventRangeID' in fileAtters:
                            fileSpec.eventRangeID = fileAtters['eventRangeID']
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


# stopwatch class
class StopWatch(object):
    # constructor
    def __init__(self):
        self.startTime = datetime.datetime.utcnow()

    # get elapsed time
    def get_elapsed_time(self):
        diff = datetime.datetime.utcnow() - self.startTime
        return " : took {0}.{0} sec".format(diff.seconds + diff.days * 24 * 3600,
                                            diff.microseconds/1000)

    # reset
    def reset(self):
        self.startTime = datetime.datetime.utcnow()


# get stopwatch
def get_stopwatch():
    return StopWatch()


# map with lock
class MapWithLock:
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
            except:
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
    h = Crypto.Hash.HMAC.new(key_phrase)
    return h.hexdigest()


# encrypt a string
def encrypt_string(key_phrase, plain_text):
    k = convert_phrase_to_key(key_phrase)
    v = Crypto.Random.new().read(Crypto.Cipher.AES.block_size)
    c = Crypto.Cipher.AES.new(k, Crypto.Cipher.AES.MODE_CFB, v)
    return base64.b64encode(v + c.encrypt(plain_text))


# decrypt a string
def decrypt_string(key_phrase, cipher_text):
    cipher_text = base64.b64decode(cipher_text)
    k = convert_phrase_to_key(key_phrase)
    v = cipher_text[:Crypto.Cipher.AES.block_size]
    c = Crypto.Cipher.AES.new(k, Crypto.Cipher.AES.MODE_CFB, v)
    cipher_text = cipher_text[Crypto.Cipher.AES.block_size:]
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
        os.chmod(f, 0o666 - umask)
        os.chown(f, uid, gid)
    os.umask(umask)
