"""
utilities

"""

import sys
import time
import zlib
import uuid
import random
import inspect
import traceback

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
        if fileSpec.fileType == 'log':
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
