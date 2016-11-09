"""
utilities

"""

import sys
import time
import random
import inspect
import traceback

from pandalogger.PandaLogger import PandaLogger
from pandalogger.LogWrapper import LogWrapper


# setup logger
def setupLogger():
    frm = inspect.stack()[1][0]
    mod = inspect.getmodule(frm)
    return PandaLogger().getLogger(mod.__name__.split('.')[-1])


# make logger
def makeLogger(tmpLog, token=None):
    # get method name of caller
    tmpStr = inspect.stack()[1][3]
    if token != None:
        tmpStr += ' <{0}>'.format(token)
    else:
        tmpStr += ' :'.format(token)
    newLog = LogWrapper(tmpLog, tmpStr)
    return newLog


# dump error message
def dumpErrorMessage(tmpLog, errStr=None):
    if not isinstance(tmpLog, LogWrapper):
        methodName = '{0} : '.format(inspect.stack()[1][3])
    else:
        methodName = ''
    # error
    if errStr == None:
        errtype, errvalue = sys.exc_info()[:2]
        errStr = "{0} {1} {2} ".format(methodName, errtype.__name__, errvalue)
        errStr += traceback.format_exc()
    tmpLog.error(errStr)
    return errStr


# sleep for random duration
def sleep(interval):
    time.sleep(random.randint(int(interval * 0.8), int(interval * 1.2)))


# make PFC
def make_pool_file_catalog(jobspec_list):
    xmlStr = """<?xml version="1.0" ?>
<!DOCTYPE POOLFILECATALOG  SYSTEM "InMemory">
<POOLFILECATALOG>
    """
    doneLFNs = set()
    for jobSpec in jobspec_list:
        inFiles = jobSpec.getInputFileAttributes()
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
