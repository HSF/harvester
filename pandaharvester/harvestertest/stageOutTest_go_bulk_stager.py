import sys
import os
import os.path
import hashlib
import datetime
import uuid
import random
import string
import time
import threading
import logging
from future.utils import iteritems
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvesterbody.cacher import Cacher
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
from pandaharvester.harvestercore import core_utils

# initial variables
fileTableName = "file_table"
queueName = "ALCF_Theta"
begin_job_id = 1111
end_job_id = 1113

# connection lock
conLock = threading.Lock()


def dump(obj):
    for attr in dir(obj):
        if hasattr(obj, attr):
            print("obj.%s = %s" % (attr, getattr(obj, attr)))


if len(sys.argv) > 1:
    queueName = sys.argv[1]
if len(sys.argv) > 2:
    begin_job_id = int(sys.argv[2])
if len(sys.argv) > 3:
    end_job_id = int(sys.argv[3])

queueConfigMapper = QueueConfigMapper()
queueConfig = queueConfigMapper.get_queue(queueName)
initial_queueConfig_stager = queueConfig.stager
queueConfig.stager["module"] = "pandaharvester.harvesterstager.go_bulk_stager"
queueConfig.stager["name"] = "GlobusBulkStager"
modified_queueConfig_stager = queueConfig.stager

pluginFactory = PluginFactory()
# get stage-out plugin
stagerCore = pluginFactory.get_plugin(queueConfig.stager)

# logger
_logger = core_utils.setup_logger("stageOutTest_go_bulk_stager")
tmpLog = core_utils.make_logger(_logger, method_name="stageOutTest_go_bulk_stager")
tmpLog.debug("start")

for loggerName, loggerObj in logging.Logger.manager.loggerDict.iteritems():
    # print "loggerName - {}".format(loggerName)
    if loggerName.startswith("panda.log"):
        if len(loggerObj.handlers) == 0:
            continue
        if loggerName.split(".")[-1] in ["db_proxy"]:
            continue
        stdoutHandler = logging.StreamHandler(sys.stdout)
        stdoutHandler.setFormatter(loggerObj.handlers[0].formatter)
        loggerObj.addHandler(stdoutHandler)

msgStr = "plugin={0}".format(stagerCore.__class__.__name__)
tmpLog.debug(msgStr)
msgStr = "Initial queueConfig.stager = {}".format(initial_queueConfig_stager)
tmpLog.debug(msgStr)
msgStr = "Modified queueConfig.stager = {}".format(modified_queueConfig_stager)
tmpLog.debug(msgStr)

scope = "panda"

proxy = DBProxy()
communicator = CommunicatorPool()
cacher = Cacher(communicator, single_mode=True)
cacher.run()


# check if db lock exits
locked = stagerCore.dbInterface.get_object_lock("dummy_id_for_out_0", lock_interval=120)
if not locked:
    tmpLog.debug("DB Already locked by another thread")
# now unlock db
unlocked = stagerCore.dbInterface.release_object_lock("dummy_id_for_out_0")
if unlocked:
    tmpLog.debug("unlocked db")
else:
    tmpLog.debug(" Could not unlock db")

# loop over the job id's creating various JobSpecs
jobSpec_list = []
for job_id in range(begin_job_id, end_job_id + 1):
    jobSpec = JobSpec()
    jobSpec.jobParams = {
        "scopeLog": "panda",
        "logFile": "log",
    }
    jobSpec.computingSite = queueName
    jobSpec.PandaID = job_id
    jobSpec.modificationTime = datetime.datetime.now()
    realDataset = "panda.sgotest." + uuid.uuid4().hex
    ddmEndPointOut = "BNL-OSG2_DATADISK"
    outFiles_scope_str = ""
    outFiles_str = ""
    realDatasets_str = ""
    ddmEndPointOut_str = ""
    # create up 5 files for output
    for index in range(random.randint(1, 5)):
        fileSpec = FileSpec()
        assFileSpec = FileSpec()
        fileSpec.fileType = "es_output"
        assFileSpec.lfn = "panda.sgotest." + uuid.uuid4().hex
        fileSpec.lfn = assFileSpec.lfn + ".gz"
        fileSpec.scope = "panda"
        outFiles_scope_str += "panda,"
        outFiles_str += fileSpec.lfn + ","
        realDatasets_str += realDataset + ","
        ddmEndPointOut_str += ddmEndPointOut + ","
        assFileSpec.fileType = "es_output"
        assFileSpec.fsize = random.randint(10, 100)
        # create source file
        hash = hashlib.md5()
        hash.update("%s:%s" % (scope, fileSpec.lfn))
        hash_hex = hash.hexdigest()
        correctedscope = "/".join(scope.split("."))
        assFileSpec.path = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(
            endPoint=queueConfig.stager["Globus_srcPath"], scope=correctedscope, hash1=hash_hex[0:2], hash2=hash_hex[2:4], lfn=assFileSpec.lfn
        )
        if not os.path.exists(os.path.dirname(assFileSpec.path)):
            tmpLog.debug("os.makedirs({})".format(os.path.dirname(assFileSpec.path)))
            os.makedirs(os.path.dirname(assFileSpec.path))
        oFile = open(assFileSpec.path, "w")
        oFile.write("".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(assFileSpec.fsize)))
        oFile.close()
        fileSpec.path = assFileSpec.path + ".gz"
        fileSpec.add_associated_file(assFileSpec)
        # print "dump(fileSpec)"
        # dump(fileSpec)
        # add output file to jobSpec
        jobSpec.add_out_file(fileSpec)
        #
        tmpLog.debug("file to transfer - {}".format(fileSpec.path))
        # print "dump(jobSpec)"
        # dump(jobSpec)
    # add log file info
    outFiles_str += "log"
    realDatasets_str += "log." + uuid.uuid4().hex
    ddmEndPointOut_str += "MWT2-UC_DATADISK"
    # remove final ","
    outFiles_scope_str = outFiles_scope_str[:-1]
    jobSpec.jobParams["scopeOut"] = outFiles_scope_str
    jobSpec.jobParams["outFiles"] = outFiles_str
    jobSpec.jobParams["realDatasets"] = realDatasets_str
    jobSpec.jobParams["ddmEndPointOut"] = ddmEndPointOut_str
    msgStr = "jobSpec.jobParams ={}".format(jobSpec.jobParams)
    tmpLog.debug(msgStr)
    msgStr = "len(jobSpec.get_output_file_attributes()) = {0} type - {1}".format(
        len(jobSpec.get_output_file_attributes()), type(jobSpec.get_output_file_attributes())
    )
    tmpLog.debug(msgStr)
    for key, value in jobSpec.get_output_file_attributes().iteritems():
        msgStr = "output file attributes - pre DB {0} {1}".format(key, value)
        tmpLog.debug(msgStr)
    jobSpec_list.append(jobSpec)


# now load into DB JobSpec's and output FileSpec's from jobSpec_list
tmpStat = proxy.insert_jobs(jobSpec_list)
if tmpStat:
    msgStr = "OK Loaded jobs into DB"
    tmpLog.debug(msgStr)
else:
    msgStr = "NG Could not load jobs into DB"
    tmpLog.debug(msgStr)
tmpStat = proxy.insert_files(jobSpec_list)
if tmpStat:
    msgStr = "OK Loaded files into DB"
    tmpLog.debug(msgStr)
else:
    msgStr = "NG Could not load files into DB"
    tmpLog.debug(msgStr)

# Now loop over the jobSpec's

for jobSpec in jobSpec_list:
    # print out jobSpec PandID
    msgStr = "jobSpec PandaID - {}".format(jobSpec.PandaID)
    msgStr = "testing zip"
    tmpStat, tmpOut = stagerCore.zip_output(jobSpec)
    if tmpStat:
        msgStr = " OK"
        tmpLog.debug(msgStr)
    else:
        msgStr = " NG {0}".format(tmpOut)
        tmpLog.debug(msgStr)
    msgStr = "testing trigger_stage_out"
    tmpLog.debug(msgStr)
    tmpStat, tmpOut = stagerCore.trigger_stage_out(jobSpec)
    if tmpStat:
        msgStr = " OK "
        tmpLog.debug(msgStr)
    elif tmpStat is None:
        msgStr = " Temporary failure NG {0}".format(tmpOut)
        tmpLog.debug(msgStr)
    elif not tmpStat:
        msgStr = " NG {0}".format(tmpOut)
        tmpLog.debug(msgStr)
        sys.exit(1)
    print
    # get the files with the group_id and print out
    msgStr = "dummy_transfer_id = {}".format(stagerCore.get_dummy_transfer_id())
    files = proxy.get_files_with_group_id(stagerCore.get_dummy_transfer_id())
    files = stagerCore.dbInterface.get_files_with_group_id(stagerCore.get_dummy_transfer_id())
    msgStr = "checking status for transfer and perhaps ultimately triggering the transfer"
    tmpLog.debug(msgStr)
    tmpStat, tmpOut = stagerCore.check_stage_out_status(jobSpec)
    if tmpStat:
        msgStr = " OK"
        tmpLog.debug(msgStr)
    elif tmpStat is None:
        msgStr = " Temporary failure NG {0}".format(tmpOut)
        tmpLog.debug(msgStr)
    elif not tmpStat:
        msgStr = " NG {0}".format(tmpOut)
        tmpLog.debug(msgStr)

# sleep for 10 minutes 1 second

msgStr = "Sleep for 601 seconds"
# msgStr = "Sleep for 181 seconds"
tmpLog.debug(msgStr)
# time.sleep(181)
time.sleep(601)
msgStr = "now check the jobs"
tmpLog.debug(msgStr)

for jobSpec in jobSpec_list:
    # print out jobSpec PandID
    msgStr = "jobSpec PandaID - {}".format(jobSpec.PandaID)
    tmpLog.debug(msgStr)
    msgStr = "checking status for transfer and perhaps ultimately triggering the transfer"
    tmpStat, tmpOut = stagerCore.check_stage_out_status(jobSpec)
    if tmpStat:
        msgStr = " OK"
        tmpLog.debug(msgStr)
    elif tmpStat is None:
        msgStr = " Temporary failure NG {0}".format(tmpOut)
        tmpLog.debug(msgStr)
    elif not tmpStat:
        msgStr = " NG {0}".format(tmpOut)
        tmpLog.debug(msgStr)

# sleep for 3 minutes

msgStr = "Sleep for 180 seconds"
tmpLog.debug(msgStr)
time.sleep(180)
msgStr = "now check the jobs"
tmpLog.debug(msgStr)

for jobSpec in jobSpec_list:
    # print out jobSpec PandID
    msgStr = "jobSpec PandaID - {}".format(jobSpec.PandaID)
    tmpLog.debug(msgStr)
    msgStr = "checking status for transfer and perhaps ultimately triggering the transfer"
    tmpStat, tmpOut = stagerCore.check_stage_out_status(jobSpec)
    if tmpStat:
        msgStr = " OK"
        tmpLog.debug(msgStr)
    elif tmpStat is None:
        msgStr = " Temporary failure NG {0}".format(tmpOut)
        tmpLog.debug(msgStr)
    elif not tmpStat:
        msgStr = " NG {0}".format(tmpOut)
        tmpLog.debug(msgStr)
