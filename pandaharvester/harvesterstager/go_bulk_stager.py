import time
import datetime
import uuid
import os
import sys
import os.path
import threading
import tarfile
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
from pandaharvester.harvestermisc import globus_utils
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvesterstager.base_stager import BaseStager

# Define dummy transfer identifier
dummy_transfer_id_base = "dummy_id_for_out"
# lock to get a unique ID
uLock = threading.Lock()

# number to get a unique ID
uID = 0

# logger
_logger = core_utils.setup_logger("go_bulk_stager")


def dump(obj):
    for attr in dir(obj):
        if hasattr(obj, attr):
            print("obj.%s = %s" % (attr, getattr(obj, attr)))


def validate_transferid(transferid):
    tmptransferid = transferid.replace("-", "")
    return all(c in string.hexdigits for c in tmptransferid)


# Globus plugin for stager with bulk transfers. For JobSpec and DBInterface methods, see
# https://github.com/PanDAWMS/panda-harvester/wiki/Utilities#file-grouping-for-file-transfers
class GlobusBulkStager(BaseStager):
    next_id = 0
    # constructor

    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        # make logger
        tmpLog = self.make_logger(_logger, "ThreadID={0}".format(threading.current_thread().ident), method_name="GlobusBulkStager __init__ ")
        tmpLog.debug("start")
        self.EventServicejob = False
        self.pathConvention = None
        self.id = GlobusBulkStager.next_id
        self.changeFileStatusOnSuccess = True
        GlobusBulkStager.next_id += 1
        with uLock:
            global uID
            # self.dummy_transfer_id = '{0}_{1}_{2}'.format(dummy_transfer_id_base,self.id,int(round(time.time() * 1000)))
            self.dummy_transfer_id = "{0}_{1}".format(dummy_transfer_id_base, "XXXX")
            uID += 1
            uID %= harvester_config.stager.nThreads
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
        tmpLog.debug("__init__ finish")

    # get dummy_transfer_id

    def get_dummy_transfer_id(self):
        return self.dummy_transfer_id

    # set dummy_transfer_id for testing
    def set_dummy_transfer_id_testing(self, dummy_transfer_id):
        self.dummy_transfer_id = dummy_transfer_id

    # set FileSpec.objstoreID
    def set_FileSpec_objstoreID(self, jobspec, objstoreID, pathConvention):
        # loop over all output files
        for fileSpec in jobspec.outFiles:
            fileSpec.objstoreID = objstoreID
            fileSpec.pathConvention = pathConvention

    # set FileSpec.status
    def set_FileSpec_status(self, jobspec, status):
        # loop over all output files
        for fileSpec in jobspec.outFiles:
            fileSpec.status = status

    # check status
    def check_stage_out_status(self, jobspec):
        # make logger
        tmpLog = self.make_logger(
            _logger, "PandaID={0} ThreadID={1}".format(jobspec.PandaID, threading.current_thread().ident), method_name="check_stage_out_status"
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
        # show the dummy transfer id and set to a value with the PandaID if needed.
        tmpLog.debug("self.dummy_transfer_id = {}".format(self.dummy_transfer_id))
        if self.dummy_transfer_id == "{0}_{1}".format(dummy_transfer_id_base, "XXXX"):
            old_dummy_transfer_id = self.dummy_transfer_id
            self.dummy_transfer_id = "{0}_{1}".format(dummy_transfer_id_base, jobspec.computingSite)
            tmpLog.debug("Change self.dummy_transfer_id  from {0} to {1}".format(old_dummy_transfer_id, self.dummy_transfer_id))
        # set flag if have db lock
        have_db_lock = False
        # get the queueConfig and corresponding objStoreID_ES
        queueConfigMapper = QueueConfigMapper()
        queueConfig = queueConfigMapper.get_queue(jobspec.computingSite)
        # check queueConfig stager section to see if jobtype is set
        if "jobtype" in queueConfig.stager:
            if queueConfig.stager["jobtype"] == "EventService":
                self.EventServicejob = True
                tmpLog.debug("Setting job type to EventService")
            # guard against old parameter in queue config
            if queueConfig.stager["jobtype"] == "Yoda":
                self.EventServicejob = True
                tmpLog.debug("Setting job type to EventService")
        # set the location of the files in fileSpec.objstoreID
        # see file /cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints.json
        self.objstoreID = int(queueConfig.stager["objStoreID_ES"])
        if self.EventServicejob:
            self.pathConvention = int(queueConfig.stager["pathConvention"])
            tmpLog.debug("EventService Job - PandaID = {0} objstoreID = {1} pathConvention ={2}".format(jobspec.PandaID, self.objstoreID, self.pathConvention))
        else:
            self.pathConvention = None
            tmpLog.debug("PandaID = {0} objstoreID = {1}".format(jobspec.PandaID, self.objstoreID))
        # test we have a Globus Transfer Client
        if not self.tc:
            errStr = "failed to get Globus Transfer Client"
            tmpLog.error(errStr)
            return False, errStr
        # set transferID to None
        transferID = None
        # get the scope of the log files
        outfileattrib = jobspec.get_output_file_attributes()
        # get transfer groups
        groups = jobspec.get_groups_of_output_files()
        tmpLog.debug("jobspec.get_groups_of_output_files() = : {0}".format(groups))
        # lock if the dummy transfer ID is used to avoid submitting duplicated transfer requests
        for dummy_transferID in groups:
            if validate_transferid(dummy_transferID):
                continue
            # lock for 120 sec
            tmpLog.debug("attempt to set DB lock for self.id - {0} dummy_transferID - {1}".format(self.id, dummy_transferID))
            have_db_lock = self.dbInterface.get_object_lock(dummy_transferID, lock_interval=120)
            if not have_db_lock:
                # escape since locked by another thread
                msgStr = "escape since locked by another thread"
                tmpLog.debug(msgStr)
                return None, msgStr
            # refresh group information since that could have been updated by another thread before getting the lock
            tmpLog.debug("self.dbInterface.refresh_file_group_info(jobspec)")
            self.dbInterface.refresh_file_group_info(jobspec)
            # get transfer groups again with refreshed info
            tmpLog.debug("After db refresh call groups=jobspec.get_groups_of_output_files()")
            groups = jobspec.get_groups_of_output_files()
            tmpLog.debug("jobspec.get_groups_of_output_files() = : {0}".format(groups))
            # the dummy transfer ID is still there
            if dummy_transferID in groups:
                groupUpdateTime = groups[dummy_transferID]["groupUpdateTime"]
                # get files with the dummy transfer ID across jobs
                fileSpecs = self.dbInterface.get_files_with_group_id(dummy_transferID)
                # submit transfer if there are more than 10 files or the group was made before more than 10 min
                msgStr = "dummy_transferID = {0}  number of files = {1}".format(dummy_transferID, len(fileSpecs))
                tmpLog.debug(msgStr)
                if len(fileSpecs) >= 10 or groupUpdateTime < datetime.datetime.utcnow() - datetime.timedelta(minutes=10):
                    tmpLog.debug("prepare to transfer files")
                    # submit transfer and get a real transfer ID
                    # set the Globus destination Endpoint id and path will get them from Agis eventually
                    # self.Globus_srcPath = queueConfig.stager['Globus_srcPath']
                    self.srcEndpoint = queueConfig.stager["srcEndpoint"]
                    self.Globus_srcPath = self.basePath
                    self.Globus_dstPath = queueConfig.stager["Globus_dstPath"]
                    self.dstEndpoint = queueConfig.stager["dstEndpoint"]
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
                            tmpLog.debug("attempt to release DB lock for self.id - {0} dummy_transferID - {1}".format(self.id, dummy_transferID))
                            self.have_db_lock = self.dbInterface.release_object_lock(dummy_transferID)
                            if not self.have_db_lock:
                                errMsg += " - Could not release DB lock for {}".format(dummy_transferID)
                            tmpLog.error(errMsg)
                            tmpRetVal = (None, errMsg)
                            return tmpRetVal
                        # both endpoints activated now prepare to transfer data
                        tdata = None
                        tdata = TransferData(self.tc, self.srcEndpoint, self.dstEndpoint, sync_level="checksum")
                    except BaseException:
                        errStat, errMsg = globus_utils.handle_globus_exception(tmpLog)
                        # release process lock
                        tmpLog.debug("attempt to release DB lock for self.id - {0} dummy_transferID - {1}".format(self.id, dummy_transferID))
                        release_db_lock = self.dbInterface.release_object_lock(dummy_transferID)
                        if not release_db_lock:
                            errMsg += " - Could not release DB lock for {}".format(dummy_transferID)
                        tmpLog.error(errMsg)
                        tmpRetVal = (errStat, errMsg)
                        return tmpRetVal
                    # loop over all files
                    ifile = 0
                    for fileSpec in fileSpecs:
                        # protect against blank lfn's
                        if not fileSpec.lfn:
                            msgStr = "fileSpec.lfn is empty"
                            tmpLog.debug(msgStr)
                            continue
                        logfile = False
                        scope = "panda"
                        if fileSpec.scope is not None:
                            scope = fileSpec.scope
                        # for EventService job set the scope to transient for non log files
                        if self.EventServicejob:
                            scope = "transient"
                        # only print to log file first 25 files
                        if ifile < 25:
                            msgStr = "fileSpec.lfn - {0} fileSpec.scope - {1}".format(fileSpec.lfn, fileSpec.scope)
                            tmpLog.debug(msgStr)
                        if ifile == 25:
                            msgStr = "printed first 25 files skipping the rest".format(fileSpec.lfn, fileSpec.scope)
                            tmpLog.debug(msgStr)
                        hash = hashlib.md5()
                        if sys.version_info.major == 2:
                            hash.update("%s:%s" % (scope, fileSpec.lfn))
                        if sys.version_info.major == 3:
                            hash_string = "{0}:{1}".format(scope, fileSpec.lfn)
                            hash.update(bytes(hash_string, "utf-8"))
                        hash_hex = hash.hexdigest()
                        correctedscope = "/".join(scope.split("."))
                        srcURL = fileSpec.path
                        dstURL = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(
                            endPoint=self.Globus_dstPath, scope=correctedscope, hash1=hash_hex[0:2], hash2=hash_hex[2:4], lfn=fileSpec.lfn
                        )
                        if logfile:
                            tmpLog.debug("src={srcURL} dst={dstURL}".format(srcURL=srcURL, dstURL=dstURL))
                        if ifile < 25:
                            tmpLog.debug("src={srcURL} dst={dstURL}".format(srcURL=srcURL, dstURL=dstURL))
                        # add files to transfer object - tdata
                        if os.access(srcURL, os.R_OK):
                            if ifile < 25:
                                tmpLog.debug("tdata.add_item({},{})".format(srcURL, dstURL))
                            tdata.add_item(srcURL, dstURL)
                        else:
                            errMsg = "source file {} does not exist".format(srcURL)
                            # release process lock
                            tmpLog.debug("attempt to release DB lock for self.id - {0} dummy_transferID - {1}".format(self.id, dummy_transferID))
                            release_db_lock = self.dbInterface.release_object_lock(dummy_transferID)
                            if not release_db_lock:
                                errMsg += " - Could not release DB lock for {}".format(dummy_transferID)
                            tmpLog.error(errMsg)
                            tmpRetVal = (False, errMsg)
                            return tmpRetVal
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
                            if not release_db_lock:
                                errMsg = "Could not release DB lock for {}".format(dummy_transferID)
                                tmpLog.error(errMsg)
                            tmpRetVal = (None, transfer_result["message"])
                            return tmpRetVal
                    except Exception as e:
                        errStat, errMsg = globus_utils.handle_globus_exception(tmpLog)
                        # release process lock
                        tmpLog.debug("attempt to release DB lock for self.id - {0} dummy_transferID - {1}".format(self.id, dummy_transferID))
                        release_db_lock = self.dbInterface.release_object_lock(dummy_transferID)
                        if not release_db_lock:
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
        tmpLog.debug("groups = jobspec.get_groups_of_output_files()")
        groups = jobspec.get_groups_of_output_files()
        tmpLog.debug("Number of transfer groups - {0}".format(len(groups)))
        tmpLog.debug("transfer groups any state - {0}".format(groups))
        if len(groups) == 0:
            tmpLog.debug("jobspec.get_groups_of_output_files(skip_done=True) returned no files ")
            tmpLog.debug("check_stage_out_status return status - True ")
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
                    self.set_FileSpec_objstoreID(jobspec, self.objstoreID, self.pathConvention)
                    if self.changeFileStatusOnSuccess:
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
                return None, ""
        # end of loop over transfer groups
        tmpLog.debug("End of loop over transfers groups - ending check_stage_out_status function")
        return None, "no valid transfer id found"

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = self.make_logger(
            _logger, "PandaID={0}  ThreadID={1}".format(jobspec.PandaID, threading.current_thread().ident), method_name="trigger_stage_out"
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
        # show the dummy transfer id and set to a value with the PandaID if needed.
        tmpLog.debug("self.dummy_transfer_id = {}".format(self.dummy_transfer_id))
        if self.dummy_transfer_id == "{0}_{1}".format(dummy_transfer_id_base, "XXXX"):
            old_dummy_transfer_id = self.dummy_transfer_id
            self.dummy_transfer_id = "{0}_{1}".format(dummy_transfer_id_base, jobspec.computingSite)
            tmpLog.debug("Change self.dummy_transfer_id  from {0} to {1}".format(old_dummy_transfer_id, self.dummy_transfer_id))
        # set the dummy transfer ID which will be replaced with a real ID in check_stage_out_status()
        lfns = []
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            # test if fileSpec.lfn is not empty
            if not fileSpec.lfn:
                msgStr = "fileSpec.lfn is empty"
            else:
                msgStr = "fileSpec.lfn is {0}".format(fileSpec.lfn)
                lfns.append(fileSpec.lfn)
            tmpLog.debug(msgStr)
        jobspec.set_groups_to_files({self.dummy_transfer_id: {"lfns": lfns, "groupStatus": "pending"}})
        msgStr = "jobspec.set_groups_to_files - self.dummy_tranfer_id - {0}, lfns - {1}, groupStatus - pending".format(self.dummy_transfer_id, lfns)
        tmpLog.debug(msgStr)
        tmpLog.debug("call self.dbInterface.set_file_group(jobspec.get_output_file_specs(skip_done=True),self.dummy_transfer_id,pending)")
        tmpStat = self.dbInterface.set_file_group(jobspec.get_output_file_specs(skip_done=True), self.dummy_transfer_id, "pending")
        tmpLog.debug("called self.dbInterface.set_file_group(jobspec.get_output_file_specs(skip_done=True),self.dummy_transfer_id,pending)")
        return True, ""

    # use tar despite name for  output files
    def zip_output(self, jobspec):
        # make logger
        tmpLog = self.make_logger(_logger, "PandaID={0} ThreadID={1}".format(jobspec.PandaID, threading.current_thread().ident), method_name="zip_output")
        return self.simple_zip_output(jobspec, tmpLog)

    # make label for transfer task
    def make_label(self, jobspec):
        return "OUT-{computingSite}-{PandaID}".format(computingSite=jobspec.computingSite, PandaID=jobspec.PandaID)

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
