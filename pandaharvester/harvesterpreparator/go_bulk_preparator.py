from pandaharvester.harvestermisc import globus_utils
import time
import datetime
import uuid
import os
import sys
import os.path
import threading
import zipfile
import hashlib
import string
from future.utils import iteritems

from globus_sdk import TransferClient
from globus_sdk import TransferData
from globus_sdk import NativeAppAuthClient
from globus_sdk import RefreshTokenAuthorizer

# TO BE REMOVED for python2.7
import requests.packages.urllib3

try:
    requests.packages.urllib3.disable_warnings()
except BaseException:
    pass
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermover import mover_utils
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper


# Define dummy transfer identifier
dummy_transfer_id_base = "dummy_id_for_in"
# lock to get a unique ID
uLock = threading.Lock()

# number to get a unique ID
uID = 0

# logger
_logger = core_utils.setup_logger("go_bulk_preparator")


def validate_transferid(transferid):
    tmptransferid = transferid.replace("-", "")
    return all(c in string.hexdigits for c in tmptransferid)


def dump(obj):
    for attr in dir(obj):
        if hasattr(obj, attr):
            print("obj.%s = %s" % (attr, getattr(obj, attr)))


# Globus plugin for stager with bulk transfers. For JobSpec and DBInterface methods, see
# https://github.com/PanDAWMS/panda-harvester/wiki/Utilities#file-grouping-for-file-transfers
class GlobusBulkPreparator(PluginBase):
    next_id = 0
    # constructor

    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        # make logger
        tmpLog = self.make_logger(_logger, "ThreadID={0}".format(threading.current_thread().ident), method_name="GlobusBulkPreparator __init__ {} ")
        tmpLog.debug("__init__ start")
        self.thread_id = threading.current_thread().ident
        self.id = GlobusBulkPreparator.next_id
        GlobusBulkPreparator.next_id += 1
        with uLock:
            global uID
            self.dummy_transfer_id = "{0}_{1}".format(dummy_transfer_id_base, "XXXX")
            uID += 1
            uID %= harvester_config.preparator.nThreads
        # create Globus Transfer Client
        try:
            self.tc = None
            # need to get client_id and refresh_token from PanDA server via harvester cache mechanism
            tmpLog.debug("about to call dbInterface.get_cache(globus_secret)")
            c_data = self.dbInterface.get_cache("globus_secret")
            if (c_data is not None) and c_data.data["StatusCode"] == 0:
                tmpLog.debug("Got the globus_secrets from PanDA")
                self.client_id = c_data.data["publicKey"]  # client_id
                self.refresh_token = c_data.data["privateKey"]  # refresh_token
                tmpStat, self.tc = globus_utils.create_globus_transfer_client(tmpLog, self.client_id, self.refresh_token)
                if not tmpStat:
                    self.tc = None
                    errStr = "failed to create Globus Transfer Client"
                    tmpLog.error(errStr)
            else:
                self.client_id = None
                self.refresh_token = None
                self.tc = None
                errStr = "failed to get Globus Client ID and Refresh Token"
                tmpLog.error(errStr)
        except BaseException:
            core_utils.dump_error_message(tmpLog)
        # tmp debugging
        tmpLog.debug("self.id = {0}".format(self.id))
        tmpLog.debug("self.dummy_transfer_id = {0}".format(self.dummy_transfer_id))
        # tmp debugging
        tmpLog.debug("__init__ finish")

    # get dummy_transfer_id

    def get_dummy_transfer_id(self):
        return self.dummy_transfer_id

    # set dummy_transfer_id for testing
    def set_dummy_transfer_id_testing(self, dummy_transfer_id):
        self.dummy_transfer_id = dummy_transfer_id

    # set FileSpec.status
    def set_FileSpec_status(self, jobspec, status):
        # loop over all input files
        for fileSpec in jobspec.inFiles:
            fileSpec.status = status

    # check status
    def check_stage_in_status(self, jobspec):
        # make logger
        tmpLog = self.make_logger(
            _logger, "PandaID={0} ThreadID={1}".format(jobspec.PandaID, threading.current_thread().ident), method_name="check_stage_in_status"
        )
        tmpLog.debug("start")
        # check that jobspec.computingSite is defined
        if jobspec.computingSite is None:
            # not found
            tmpLog.error("jobspec.computingSite is not defined")
            return False, "jobspec.computingSite is not defined"
        else:
            tmpLog.debug("jobspec.computingSite : {0}".format(jobspec.computingSite))
        # show the dummy transfer id and set to a value with the jobspec.computingSite if needed.
        tmpLog.debug("self.dummy_transfer_id = {}".format(self.dummy_transfer_id))
        if self.dummy_transfer_id == "{0}_{1}".format(dummy_transfer_id_base, "XXXX"):
            old_dummy_transfer_id = self.dummy_transfer_id
            self.dummy_transfer_id = "{0}_{1}".format(dummy_transfer_id_base, jobspec.computingSite)
            tmpLog.debug("Change self.dummy_transfer_id  from {0} to {1}".format(old_dummy_transfer_id, self.dummy_transfer_id))

        # default return
        tmpRetVal = (True, "")
        # set flag if have db lock
        have_db_lock = False
        queueConfigMapper = QueueConfigMapper()
        queueConfig = queueConfigMapper.get_queue(jobspec.computingSite)
        # test we have a Globus Transfer Client
        if not self.tc:
            errStr = "failed to get Globus Transfer Client"
            tmpLog.error(errStr)
            return False, errStr
        # set transferID to None
        transferID = None
        # get transfer groups
        groups = jobspec.get_groups_of_input_files(skip_ready=True)
        tmpLog.debug("jobspec.get_groups_of_input_files() = : {0}".format(groups))
        # lock if the dummy transfer ID is used to avoid submitting duplicated transfer requests
        for dummy_transferID in groups:
            # skip if valid transfer ID not dummy one
            if validate_transferid(dummy_transferID):
                continue
            # lock for 120 sec
            tmpLog.debug(
                "attempt to set DB lock for self.id - {0} self.dummy_transfer_id - {1}, dummy_transferID - {2}".format(
                    self.id, self.dummy_transfer_id, dummy_transferID
                )
            )
            have_db_lock = self.dbInterface.get_object_lock(dummy_transferID, lock_interval=120)
            tmpLog.debug(" DB lock result - {0}".format(have_db_lock))
            if not have_db_lock:
                # escape since locked by another thread
                msgStr = "escape since locked by another thread"
                tmpLog.debug(msgStr)
                return None, msgStr
            # refresh group information since that could have been updated by another thread before getting the lock
            tmpLog.debug("self.dbInterface.refresh_file_group_info(jobspec)")
            self.dbInterface.refresh_file_group_info(jobspec)
            tmpLog.debug("after self.dbInterface.refresh_file_group_info(jobspec)")
            # get transfer groups again with refreshed info
            tmpLog.debug("groups = jobspec.get_groups_of_input_files(skip_ready=True)")
            groups = jobspec.get_groups_of_input_files(skip_ready=True)
            tmpLog.debug("after db lock and refresh - jobspec.get_groups_of_input_files(skip_ready=True) = : {0}".format(groups))
            # the dummy transfer ID is still there
            if dummy_transferID in groups:
                groupUpdateTime = groups[dummy_transferID]["groupUpdateTime"]
                # get files with the dummy transfer ID across jobs
                fileSpecs_allgroups = self.dbInterface.get_files_with_group_id(dummy_transferID)
                msgStr = "dummy_transferID = {0} self.dbInterface.get_files_with_group_id(dummy_transferID)  number of files = {1}".format(
                    dummy_transferID, len(fileSpecs_allgroups)
                )
                tmpLog.debug(msgStr)
                fileSpecs = jobspec.get_input_file_specs(dummy_transferID, skip_ready=True)
                msgStr = "dummy_transferID = {0} jobspec.get_input_file_specs(dummy_transferID,skip_ready=True)  number of files = {1}".format(
                    dummy_transferID, len(fileSpecs)
                )
                tmpLog.debug(msgStr)
                # submit transfer if there are more than 10 files or the group was made before more than 10 min
                if len(fileSpecs) >= 10 or groupUpdateTime < datetime.datetime.utcnow() - datetime.timedelta(minutes=10):
                    tmpLog.debug("prepare to transfer files")
                    # submit transfer and get a real transfer ID
                    # set the Globus destination Endpoint id and path will get them from Agis eventually
                    self.Globus_srcPath = queueConfig.preparator["Globus_srcPath"]
                    self.srcEndpoint = queueConfig.preparator["srcEndpoint"]
                    self.Globus_dstPath = self.basePath
                    # self.Globus_dstPath = queueConfig.preparator['Globus_dstPath']
                    self.dstEndpoint = queueConfig.preparator["dstEndpoint"]
                    # Test the endpoints and create the transfer data class
                    errMsg = None
                    try:
                        # Test endpoints for activation
                        tmpStatsrc, srcStr = globus_utils.check_endpoint_activation(tmpLog, self.tc, self.srcEndpoint)
                        tmpStatdst, dstStr = globus_utils.check_endpoint_activation(tmpLog, self.tc, self.dstEndpoint)
                        if tmpStatsrc and tmpStatdst:
                            errStr = "source Endpoint and destination Endpoint activated"
                            tmpLog.debug(errStr)
                        else:
                            errMsg = ""
                            if not tmpStatsrc:
                                errMsg += " source Endpoint not activated "
                            if not tmpStatdst:
                                errMsg += " destination Endpoint not activated "
                            # release process lock
                            tmpLog.debug(
                                "attempt to release DB lock for self.id - {0} self.dummy_transfer_id - {1}, dummy_transferID - {2}".format(
                                    self.id, self.dummy_transfer_id, dummy_transferID
                                )
                            )
                            have_db_lock = self.dbInterface.release_object_lock(dummy_transferID)
                            if not have_db_lock:
                                errMsg += " - Could not release DB lock for {}".format(dummy_transferID)
                            tmpLog.error(errMsg)
                            tmpRetVal = (None, errMsg)
                            return tmpRetVal
                        # both endpoints activated now prepare to transfer data
                        tdata = None
                        tdata = TransferData(self.tc, self.srcEndpoint, self.dstEndpoint, sync_level="exists")
                        #                                             sync_level="checksum")
                        tmpLog.debug("size of tdata[DATA] - {}".format(len(tdata["DATA"])))

                    except BaseException:
                        errStat, errMsg = globus_utils.handle_globus_exception(tmpLog)
                        # release process lock
                        tmpLog.debug(
                            "attempt to release DB lock for self.id - {0} self.dummy_transfer_id - {1}, dummy_transferID - {2}".format(
                                self.id, self.dummy_transfer_id, dummy_transferID
                            )
                        )
                        release_db_lock = self.dbInterface.release_object_lock(dummy_transferID)
                        if not release_db_lock:
                            errMsg += " - Could not release DB lock for {}".format(self.dummy_transferID)
                        tmpLog.error(errMsg)
                        tmpRetVal = (errStat, errMsg)
                        return tmpRetVal
                    # loop over all files
                    ifile = 0
                    for fileSpec in fileSpecs:
                        # only print to log file first 25 files
                        if ifile < 25:
                            msgStr = "fileSpec.lfn - {0} fileSpec.scope - {1}".format(fileSpec.lfn, fileSpec.scope)
                            tmpLog.debug(msgStr)
                        if ifile == 25:
                            msgStr = "printed first 25 files skipping the rest".format(fileSpec.lfn, fileSpec.scope)
                            tmpLog.debug(msgStr)
                        # end debug log file test
                        scope = "panda"
                        if fileSpec.scope is not None:
                            scope = fileSpec.scope
                        hash = hashlib.md5()
                        if sys.version_info.major == 2:
                            hash.update("%s:%s" % (scope, fileSpec.lfn))
                        if sys.version_info.major == 3:
                            hash_string = "{0}:{1}".format(scope, fileSpec.lfn)
                            hash.update(bytes(hash_string, "utf-8"))
                        hash_hex = hash.hexdigest()
                        correctedscope = "/".join(scope.split("."))
                        # srcURL = fileSpec.path
                        srcURL = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(
                            endPoint=self.Globus_srcPath, scope=correctedscope, hash1=hash_hex[0:2], hash2=hash_hex[2:4], lfn=fileSpec.lfn
                        )
                        dstURL = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(
                            endPoint=self.Globus_dstPath, scope=correctedscope, hash1=hash_hex[0:2], hash2=hash_hex[2:4], lfn=fileSpec.lfn
                        )
                        # add files to transfer object - tdata
                        if ifile < 25:
                            tmpLog.debug("tdata.add_item({},{})".format(srcURL, dstURL))
                        tdata.add_item(srcURL, dstURL)
                        ifile += 1
                    # submit transfer
                    tmpLog.debug("Number of files to transfer - {}".format(len(tdata["DATA"])))
                    try:
                        transfer_result = self.tc.submit_transfer(tdata)
                        # check status code and message
                        tmpLog.debug(str(transfer_result))
                        if transfer_result["code"] == "Accepted":
                            # succeeded
                            # set transfer ID which are used for later lookup
                            transferID = transfer_result["task_id"]
                            tmpLog.debug("successfully submitted id={0}".format(transferID))
                            # set status for files
                            self.dbInterface.set_file_group(fileSpecs, transferID, "running")
                            msgStr = "submitted transfer with ID={0}".format(transferID)
                            tmpLog.debug(msgStr)
                        else:
                            # release process lock
                            tmpLog.debug("attempt to release DB lock for self.id - {0} dummy_transferID - {1}".format(self.id, dummy_transferID))
                            release_db_lock = self.dbInterface.release_object_lock(dummy_transferID)
                            if release_db_lock:
                                tmpLog.debug("Released DB lock for self.id - {0} dummy_transferID - {1}".format(self.id, dummy_transferID))
                                have_db_lock = False
                            else:
                                errMsg = "Could not release DB lock for {}".format(dummy_transferID)
                                tmpLog.error(errMsg)
                            tmpRetVal = (None, transfer_result["message"])
                            return tmpRetVal
                    except Exception as e:
                        errStat, errMsg = globus_utils.handle_globus_exception(tmpLog)
                        # release process lock
                        tmpLog.debug("attempt to release DB lock for self.id - {0} dummy_transferID - {1}".format(self.id, dummy_transferID))
                        release_db_lock = self.dbInterface.release_object_lock(dummy_transferID)
                        if release_db_lock:
                            tmpLog.debug("Released DB lock for self.id - {0} dummy_transferID - {1}".format(self.id, dummy_transferID))
                            have_db_lock = False
                        else:
                            errMsg += " - Could not release DB lock for {}".format(dummy_transferID)
                        tmpLog.error(errMsg)
                        return errStat, errMsg
                else:
                    msgStr = "wait until enough files are pooled"
                    tmpLog.debug(msgStr)
                # release the lock
                tmpLog.debug("attempt to release DB lock for self.id - {0} dummy_transferID - {1}".format(self.id, dummy_transferID))
                release_db_lock = self.dbInterface.release_object_lock(dummy_transferID)
                if release_db_lock:
                    tmpLog.debug("released DB lock for self.id - {0} dummy_transferID - {1}".format(self.id, dummy_transferID))
                    have_db_lock = False
                else:
                    msgStr += " - Could not release DB lock for {}".format(dummy_transferID)
                    tmpLog.error(msgStr)
                # return None to retry later
                return None, msgStr
            # release the db lock if needed
            if have_db_lock:
                tmpLog.debug("attempt to release DB lock for self.id - {0} dummy_transferID - {1}".format(self.id, dummy_transferID))
                release_db_lock = self.dbInterface.release_object_lock(dummy_transferID)
                if release_db_lock:
                    tmpLog.debug("released DB lock for self.id - {0} dummy_transferID - {1}".format(self.id, dummy_transferID))
                    have_db_lock = False
                else:
                    msgStr += " - Could not release DB lock for {}".format(dummy_transferID)
                    tmpLog.error(msgStr)
                    return None, msgStr
        # check transfer with real transfer IDs
        # get transfer groups
        tmpLog.debug("groups = jobspec.get_groups_of_input_files(skip_ready=True)")
        groups = jobspec.get_groups_of_input_files(skip_ready=True)
        tmpLog.debug("Number of transfer groups (skip_ready)- {0}".format(len(groups)))
        tmpLog.debug("transfer groups any state (skip_ready)- {0}".format(groups))
        tmpLog.debug("groups = jobspec.get_groups_of_input_files()")
        groups = jobspec.get_groups_of_input_files()
        tmpLog.debug("Number of transfer groups - {0}".format(len(groups)))
        tmpLog.debug("transfer groups any state - {0}".format(groups))
        tmpLog.debug("groups = jobspec.get_groups_of_input_files(skip_ready=True)")
        groups = jobspec.get_groups_of_input_files(skip_ready=True)
        if len(groups) == 0:
            tmpLog.debug("jobspec.get_groups_of_input_files(skip_ready=True) returned no files ")
            tmpLog.debug("check_stage_in_status return status - True ")
            return True, ""
        for transferID in groups:
            # allow only valid UUID
            if validate_transferid(transferID):
                # get transfer task
                tmpStat, transferTasks = globus_utils.get_transfer_task_by_id(tmpLog, self.tc, transferID)
                # return a temporary error when failed to get task
                if not tmpStat:
                    errStr = "failed to get transfer task; tc = %s; transferID = %s" % (str(self.tc), str(transferID))
                    tmpLog.error(errStr)
                    return None, errStr
                # return a temporary error when task is missing
                if transferID not in transferTasks:
                    errStr = "transfer task ID - {} is missing".format(transferID)
                    tmpLog.error(errStr)
                    return None, errStr
                # succeeded in finding a transfer task by tranferID
                if transferTasks[transferID]["status"] == "SUCCEEDED":
                    tmpLog.debug("transfer task {} succeeded".format(transferID))
                    self.set_FileSpec_status(jobspec, "finished")
                    return True, ""
                # failed
                if transferTasks[transferID]["status"] == "FAILED":
                    errStr = "transfer task {} failed".format(transferID)
                    tmpLog.error(errStr)
                    self.set_FileSpec_status(jobspec, "failed")
                    return False, errStr
                # another status
                tmpStr = "transfer task {0} status: {1}".format(transferID, transferTasks[transferID]["status"])
                tmpLog.debug(tmpStr)
                return None, tmpStr
        # end of loop over transfer groups
        tmpLog.debug("End of loop over transfers groups - ending check_stage_in_status function")
        return None, "no valid transfer id found"

    # trigger preparation

    def trigger_preparation(self, jobspec):
        # make logger
        tmpLog = self.make_logger(
            _logger, "PandaID={0} ThreadID={1}".format(jobspec.PandaID, threading.current_thread().ident), method_name="trigger_preparation"
        )
        tmpLog.debug("start")
        # default return
        tmpRetVal = (True, "")
        # check that jobspec.computingSite is defined
        if jobspec.computingSite is None:
            # not found
            tmpLog.error("jobspec.computingSite is not defined")
            return False, "jobspec.computingSite is not defined"
        else:
            tmpLog.debug("jobspec.computingSite : {0}".format(jobspec.computingSite))
        # test we have a Globus Transfer Client
        if not self.tc:
            errStr = "failed to get Globus Transfer Client"
            tmpLog.error(errStr)
            return False, errStr
        # show the dummy transfer id and set to a value with the computingSite if needed.
        tmpLog.debug("self.dummy_transfer_id = {}".format(self.dummy_transfer_id))
        if self.dummy_transfer_id == "{0}_{1}".format(dummy_transfer_id_base, "XXXX"):
            old_dummy_transfer_id = self.dummy_transfer_id
            self.dummy_transfer_id = "{0}_{1}".format(dummy_transfer_id_base, jobspec.computingSite)
            tmpLog.debug("Change self.dummy_transfer_id  from {0} to {1}".format(old_dummy_transfer_id, self.dummy_transfer_id))
        # set the dummy transfer ID which will be replaced with a real ID in check_stage_in_status()
        inFiles = jobspec.get_input_file_attributes(skip_ready=True)
        lfns = list(inFiles.keys())
        # for inLFN in inFiles.keys():
        #    lfns.append(inLFN)
        tmpLog.debug("number of lfns - {0} type(lfns) - {1}".format(len(lfns), type(lfns)))
        jobspec.set_groups_to_files({self.dummy_transfer_id: {"lfns": lfns, "groupStatus": "pending"}})
        if len(lfns) < 10:
            msgStr = "jobspec.set_groups_to_files - self.dummy_tranfer_id - {0}, lfns - {1}, groupStatus - pending".format(self.dummy_transfer_id, lfns)
        else:
            tmp_lfns = lfns[:10]
            msgStr = "jobspec.set_groups_to_files - self.dummy_tranfer_id - {0}, lfns (first 25) - {1}, groupStatus - pending".format(
                self.dummy_transfer_id, tmp_lfns
            )
        tmpLog.debug(msgStr)
        fileSpec_list = jobspec.get_input_file_specs(self.dummy_transfer_id, skip_ready=True)
        tmpLog.debug("call jobspec.get_input_file_specs({0}, skip_ready=True) num files returned = {1}".format(self.dummy_transfer_id, len(fileSpec_list)))
        tmpLog.debug(
            "call self.dbInterface.set_file_group(jobspec.get_input_file_specs(self.dummy_transfer_id,skip_ready=True),self.dummy_transfer_id,pending)"
        )
        tmpStat = self.dbInterface.set_file_group(fileSpec_list, self.dummy_transfer_id, "pending")
        msgStr = "called self.dbInterface.set_file_group(jobspec.get_input_file_specs(self.dummy_transfer_id,skip_ready=True),self.dummy_transfer_id,pending) return Status {}".format(
            tmpStat
        )
        tmpLog.debug(msgStr)
        return True, ""

    # make label for transfer task

    def make_label(self, jobspec):
        return "IN-{computingSite}-{PandaID}".format(computingSite=jobspec.computingSite, PandaID=jobspec.PandaID)

    # resolve input file paths
    def resolve_input_paths(self, jobspec):
        # get input files
        inFiles = jobspec.get_input_file_attributes()
        # set path to each file
        for inLFN, inFile in iteritems(inFiles):
            inFile["path"] = mover_utils.construct_file_path(self.basePath, inFile["scope"], inLFN)
        # set
        jobspec.set_input_file_paths(inFiles)
        return True, ""

    # Globus specific commands
