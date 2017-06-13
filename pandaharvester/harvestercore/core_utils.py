"""
utilities

"""

import sys
import time
import zlib
import uuid
import math
import random
import inspect
import traceback

from work_spec import WorkSpec
from file_spec import FileSpec
from event_spec import EventSpec
from pandalogger.PandaLogger import PandaLogger
from pandalogger.LogWrapper import LogWrapper


# setup logger
def setup_logger(name=None):
    if name is None:
        frm = inspect.stack()[1][0]
        mod = inspect.getmodule(frm)
        name = mod.__name__.split('.')[-1]
    return PandaLogger().getLogger(name)


# make logger
def make_logger(tmp_log, token=None):
    # get method name of caller
    tmpStr = inspect.stack()[1][3]
    if token is not None:
        tmpStr += ' <{0}>'.format(token)
    else:
        tmpStr += ' :'.format(token)
    newLog = LogWrapper(tmp_log, tmpStr)
    return newLog


# dump error message
def dump_error_message(tmp_log, err_str=None):
    if not isinstance(tmp_log, LogWrapper):
        methodName = '{0} : '.format(inspect.stack()[1][3])
    else:
        methodName = ''
    # error
    if err_str is None:
        errtype, errvalue = sys.exc_info()[:2]
        err_str = "{0} {1} {2} ".format(methodName, errtype.__name__, errvalue)
        err_str += traceback.format_exc()
    tmp_log.error(err_str)
    return err_str


# sleep for random duration and return True if no more sleep is needed
def sleep(interval, stop_event):
    randInterval = random.randint(int(interval * 0.8), int(interval * 1.2))
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
        for inLFN, inFile in inFiles.iteritems():
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
    with open(file_name) as fp:
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
        xml += """"<File ID="{guid}">
        <logical>
        <lfn name="{lfn}"/>
        </logical>
        <metadata att_name="fsize" att_value = "{fsize}"/>
        <metadata att_name="adler32" att_value="{chksum}"/>
        </File> """.format(guid=guid, lfn=fileSpec.lfn, fsize=fileSpec.fsize, chksum=fileSpec.chksum)
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
    if map_type == WorkSpec.MT_OneToOne:
        jobSpec = jobspec_list[0]
        workSpec = workspec_list[0]
        jobSpec.set_attributes(workSpec.workAttributes)
        # set start and end times
        if workSpec.status in [WorkSpec.ST_running]:
            jobSpec.set_start_time()
        elif workSpec.status in [WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled]:
            jobSpec.set_end_time()
        # core count
        if workSpec.nCore is not None:
            jobSpec.nCore = workSpec.nCore
        # add files
        for files_to_stage_out in files_to_stage_out_list:
            if jobSpec.PandaID in files_to_stage_out:
                for lfn, fileAtters in files_to_stage_out[jobSpec.PandaID].iteritems():
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
                    jobSpec.add_out_file(fileSpec)
        # add events
        for events_to_update in events_to_update_list:
            if jobSpec.PandaID in events_to_update:
                for data in events_to_update[jobSpec.PandaID]:
                    eventSpec = EventSpec()
                    eventSpec.from_data(data)
                    jobSpec.add_event(eventSpec, None)
        jobSpec.status, jobSpec.subStatus = workSpec.convert_to_job_status()
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
        for files_to_stage_out in files_to_stage_out_list:
            if jobSpec.PandaID in files_to_stage_out:
                for lfn, fileAtters in files_to_stage_out[jobSpec.PandaID].iteritems():
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
                    jobSpec.add_out_file(fileSpec)
        # add events
        for events_to_update in events_to_update_list:
            if jobSpec.PandaID in events_to_update:
                for data in events_to_update[jobSpec.PandaID]:
                    eventSpec = EventSpec()
                    eventSpec.from_data(data)
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
    elif map_type == WorkSpec.MT_MultiJobs:
        # TOBEFIXED
        pass
    return True
