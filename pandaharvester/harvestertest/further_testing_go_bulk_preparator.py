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
from pandaharvester.harvestermisc import globus_utils

from globus_sdk import TransferClient
from globus_sdk import TransferData
from globus_sdk import NativeAppAuthClient
from globus_sdk import RefreshTokenAuthorizer


# initial variables
fileTableName = "file_table"
queueName = "ALCF_Theta"
begin_job_id = 1111
end_job_id = 1113
globus_sleep_time = 15

# connection lock
conLock = threading.Lock()


def dump(obj):
    for attr in dir(obj):
        if hasattr(obj, attr):
            print("obj.%s = %s" % (attr, getattr(obj, attr)))


if len(sys.argv) > 1:
    queueName = sys.argv[1]
# if len(sys.argv) > 2:
#   begin_job_id = int(sys.argv[2])
# if len(sys.argv) > 3:
#   end_job_id = int(sys.argv[3])
# if len(sys.argv) > 4:
#   globus_sleep_time = int(sys.argv[4])

queueConfigMapper = QueueConfigMapper()
queueConfig = queueConfigMapper.get_queue(queueName)
initial_queueConfig_preparator = queueConfig.preparator
queueConfig.preparator["module"] = "pandaharvester.harvesterpreparator.go_bulk_preparator"
queueConfig.preparator["name"] = "GlobusBulkPreparator"
modified_queueConfig_preparator = queueConfig.preparator

pluginFactory = PluginFactory()
# get stage-out plugin
preparatorCore = pluginFactory.get_plugin(queueConfig.preparator)

# logger
_logger = core_utils.setup_logger("further_testing_go_bulk_preparator")
tmpLog = core_utils.make_logger(_logger, method_name="further_testing_go_bulk_preparator")
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

msgStr = "plugin={0}".format(preparatorCore.__class__.__name__)
tmpLog.debug(msgStr)
msgStr = "Initial queueConfig.preparator = {}".format(initial_queueConfig_preparator)
tmpLog.debug(msgStr)
msgStr = "Modified queueConfig.preparator = {}".format(modified_queueConfig_preparator)
tmpLog.debug(msgStr)

scope = "panda"

proxy = DBProxy()
communicator = CommunicatorPool()
cacher = Cacher(communicator, single_mode=True)
cacher.run()

tmpLog.debug("plugin={0}".format(preparatorCore.__class__.__name__))
tmpLog.debug("BasePath from preparator configuration: %s " % preparatorCore.basePath)

# get all jobs in table in a preparing substate
tmpLog.debug("try to get all jobs in a preparing substate")
jobSpec_list = proxy.get_jobs_in_sub_status("preparing", 2000, None, None, None, None, None, None)
tmpLog.debug("got {0} jobs".format(len(jobSpec_list)))
# loop over all found jobs
if len(jobSpec_list) > 0:
    for jobSpec in jobSpec_list:
        tmpLog.debug(" PandaID = %d status = %s subStatus = %s lockedBy = %s" % (jobSpec.PandaID, jobSpec.status, jobSpec.subStatus, jobSpec.lockedBy))
        # get the transfer groups
        groups = jobSpec.get_groups_of_input_files(skip_ready=True)
        tmpLog.debug("jobspec.get_groups_of_input_files() = : {0}".format(groups))
        # loop over groups keys to see if db is locked
        for key in groups:
            locked = preparatorCore.dbInterface.get_object_lock(key, lock_interval=120)
            if not locked:
                tmpLog.debug("DB Already locked by another thread")
            # now unlock db
            unlocked = preparatorCore.dbInterface.release_object_lock(key)
            if unlocked:
                tmpLog.debug("unlocked db")
            else:
                tmpLog.debug(" Could not unlock db")
        # print out jobSpec PandID
        msgStr = "jobSpec PandaID - {}".format(jobSpec.PandaID)
        tmpLog.debug(msgStr)
        # msgStr = "testing trigger_preparation"
        # tmpLog.debug(msgStr)
        # tmpStat, tmpOut = preparatorCore.trigger_preparation(jobSpec)
        # if tmpStat:
        #   msgStr = " OK "
        #   tmpLog.debug(msgStr)
        # elif tmpStat == None:
        #   msgStr = " Temporary failure NG {0}".format(tmpOut)
        #   tmpLog.debug(msgStr)
        # elif not tmpStat:
        #   msgStr = " No Good {0}".format(tmpOut)
        #   tmpLog.debug(msgStr)
        #   sys.exit(1)

        # check status to actually trigger transfer
        # get the files with the group_id and print out
        msgStr = "Original dummy_transfer_id = {}".format(preparatorCore.get_dummy_transfer_id())
        tmpLog.debug(msgStr)
        # modify dummy_transfer_id from groups of input files
        for key in groups:
            preparatorCore.set_dummy_transfer_id_testing(key)
            msgStr = "Revised dummy_transfer_id = {}".format(preparatorCore.get_dummy_transfer_id())
            tmpLog.debug(msgStr)
            files = proxy.get_files_with_group_id(preparatorCore.get_dummy_transfer_id())
            tmpLog.debug("proxy.get_files_with_group_id(preparatorCore.get_dummy_transfer_id()) = {0}".format(files))
            files = preparatorCore.dbInterface.get_files_with_group_id(preparatorCore.get_dummy_transfer_id())
            tmpLog.debug("preparatorCore.dbInterface.get_files_with_group_id(preparatorCore.get_dummy_transfer_id()) = {0}".format(files))
            msgStr = "checking status for transfer and perhaps ultimately triggering the transfer"
            tmpLog.debug(msgStr)
            tmpStat, tmpOut = preparatorCore.check_stage_in_status(jobSpec)
            if tmpStat:
                msgStr = " OK"
                tmpLog.debug(msgStr)
            elif tmpStat is None:
                msgStr = " Temporary failure No Good {0}".format(tmpOut)
                tmpLog.debug(msgStr)
            elif not tmpStat:
                msgStr = " No Good {0}".format(tmpOut)
                tmpLog.debug(msgStr)
