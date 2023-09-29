import time
import datetime
import uuid
import os
import sys
import stat
import os.path
import threading
import tarfile
import hashlib
import string
import shutil
import errno

from future.utils import iteritems

from rucio.client import Client as RucioClient
from rucio.common.exception import DataIdentifierNotFound, DuplicateRule, DataIdentifierAlreadyExists, FileAlreadyExists


from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermover import mover_utils
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvesterstager.base_stager import BaseStager

from .base_stager import BaseStager


def dump(tmpLog, obj):
    for attr in dir(obj):
        if hasattr(obj, attr):
            tmpLog.debug("obj.%s = %s" % (attr, getattr(obj, attr)))


# logger
baseLogger = core_utils.setup_logger("yoda_rucio_rse_direct_stager")


class Error(EnvironmentError):
    pass


class SpecialFileError(EnvironmentError):
    """Raised when trying to do a kind of operation (e.g. copying) which is
    not supported on a special file (e.g. a named pipe)"""


class ExecError(EnvironmentError):
    """Raised when a command could not be executed"""


# stager plugin with RSE + local site move behaviour for Yoda zip files
class YodaRucioRseDirectStager(BaseStager):
    """In the workflow for RseDirectStager, workers directly upload output files to RSE
    and thus there is no data motion in Harvester."""

    # constructor

    def __init__(self, **kwarg):
        BaseStager.__init__(self, **kwarg)
        tmpLog = self.make_logger(baseLogger, "ThreadID={0}".format(threading.current_thread().ident), method_name="YodaRucioRseDirectStager __init__ ")
        tmpLog.debug("start")
        self.Yodajob = False
        self.pathConvention = None
        self.objstoreID = None
        tmpLog.debug("stop")

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
        tmpStat = True
        tmpMsg = ""
        # make logger
        tmpLog = self.make_logger(
            baseLogger, "PandaID={0} ThreadID={1}".format(jobspec.PandaID, threading.current_thread().ident), method_name="check_stage_out_status"
        )
        tmpLog.debug("start")
        # check that jobspec.computingSite is defined
        if jobspec.computingSite is None:
            # not found
            tmpLog.error("jobspec.computingSite is not defined")
            return False, "jobspec.computingSite is not defined"
        else:
            tmpLog.debug("jobspec.computingSite : {0}".format(jobspec.computingSite))
        # get the queueConfig and corresponding objStoreID_ES
        queueConfigMapper = QueueConfigMapper()
        queueConfig = queueConfigMapper.get_queue(jobspec.computingSite)
        # write to debug log queueConfig.stager
        tmpLog.debug("jobspec.computingSite - {0} queueConfig.stager {1}".format(jobspec.computingSite, queueConfig.stager))
        # check queueConfig stager section to see if jobtype is set
        if "jobtype" in queueConfig.stager:
            if queueConfig.stager["jobtype"] == "Yoda":
                self.Yodajob = True
        # get destination endpoint
        nucleus = jobspec.jobParams["nucleus"]
        agis = self.dbInterface.get_cache("panda_queues.json").data
        dstRSE = [agis[x]["astorages"]["pr"][0] for x in agis if agis[x]["atlas_site"] == nucleus][0]
        # set the location of the files in fileSpec.objstoreID
        # see file /cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints.json
        ddm = self.dbInterface.get_cache("agis_ddmendpoints.json").data
        self.objstoreID = ddm[dstRSE]["id"]
        if self.Yodajob:
            self.pathConvention = int(queueConfig.stager["pathConvention"])
            tmpLog.debug("Yoda Job - PandaID = {0} objstoreID = {1} pathConvention ={2}".format(jobspec.PandaID, self.objstoreID, self.pathConvention))
        else:
            self.pathConvention = None
            tmpLog.debug("PandaID = {0} objstoreID = {1}".format(jobspec.PandaID, self.objstoreID))
        # set the location of the files in fileSpec.objstoreID
        self.set_FileSpec_objstoreID(jobspec, self.objstoreID, self.pathConvention)
        # Get the files grouped by Rucio Rule ID
        groups = jobspec.get_groups_of_output_files()
        if len(groups) == 0:
            tmpLog.debug("No Rucio Rules")
            return None, "No Rucio Rules"
        tmpLog.debug("#Rucio Rules - {0} - Rules - {1}".format(len(groups), groups))

        try:
            rucioAPI = RucioClient()
        except BaseException:
            tmpLog.error("failure to get Rucio Client try again later")
            return None, "failure to get Rucio Client try again later"

        # loop over the Rucio rules
        for rucioRule in groups:
            if rucioRule is None:
                continue
            # lock
            have_db_lock = self.dbInterface.get_object_lock(rucioRule, lock_interval=120)
            if not have_db_lock:
                msgStr = "escape since {0} is locked by another thread".format(rucioRule)
                tmpLog.debug(msgStr)
                return None, msgStr
            # get transfer status
            groupStatus = self.dbInterface.get_file_group_status(rucioRule)
            tmpLog.debug("rucioRule - {0} - groupStatus - {1}".format(rucioRule, groupStatus))
            if "transferred" in groupStatus:
                # already succeeded - set the fileSpec status for these files
                self.set_FileSpec_objstoreID(jobspec, self.objstoreID, self.pathConvention)
                pass
            elif "failed" in groupStatus:
                # transfer failure
                tmpStat = False
                tmpMsg = "rucio rule for {0}:{1} already failed".format(datasetScope, datasetName)
            elif "transferring" in groupStatus or "pending" in groupStatus:
                # transfer started in Rucio check status
                try:
                    result = rucioAPI.get_replication_rule(rucioRule, False)
                    if result["state"] == "OK":
                        # files transfered to nucleus
                        tmpLog.debug("Files for Rucio Rule {0} successfully transferred".format(rucioRule))
                        self.dbInterface.update_file_group_status(rucioRule, "transferred")
                        # set the fileSpec status for these files
                        self.set_FileSpec_objstoreID(jobspec, self.objstoreID, self.pathConvention)
                        self.set_FileSpec_status(jobspec, "finished")
                    elif result["state"] == "FAILED":
                        # failed Rucio Transfer
                        tmpStat = False
                        tmpMsg = "Failed Rucio Transfer - Rucio Rule - {0}".format(rucioRule)
                        tmpLog.debug(tmpMsg)
                        self.set_FileSpec_status(jobspec, "failed")
                    elif result["state"] == "STUCK":
                        tmpStat = None
                        tmpMsg = "Rucio Transfer Rule {0} Stuck".format(rucioRule)
                        tmpLog.debug(tmpMsg)
                except BaseException:
                    tmpStat = None
                    tmpMsg = "Could not get information or Rucio Rule {0}".format(rucioRule)
                    tmpLog.error(tmpMsg)
                    pass
            # release the lock
            if have_db_lock:
                tmpLog.debug("attempt to release DB lock for Rucio Rule {0}".format(rucioRule))
                release_db_lock = self.dbInterface.release_object_lock(rucioRule)
                if release_db_lock:
                    tmpLog.debug("released DB lock for rucioRule - {0}".format(rucioRule))
                    have_db_lock = False
                else:
                    msgStr = " Could not release DB lock for {}".format(rucioRule)
                    tmpLog.error(msgStr)
                    return None, msgStr

        tmpLog.debug("stop")
        return tmpStat, tmpMsg

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = self.make_logger(
            baseLogger, "PandaID={0} ThreadID={1}".format(jobspec.PandaID, threading.current_thread().ident), method_name="trigger_stage_out"
        )
        tmpLog.debug("start")
        # initialize some values
        tmpStat = None
        tmpMsg = ""
        srcRSE = None
        dstRSE = None
        datasetName = "panda.harvester.{0}.{1}".format(jobspec.PandaID, str(uuid.uuid4()))
        datasetScope = "transient"
        # check that jobspec.computingSite is defined
        if jobspec.computingSite is None:
            # not found
            tmpLog.error("jobspec.computingSite is not defined")
            return False, "jobspec.computingSite is not defined"
        else:
            tmpLog.debug("jobspec.computingSite : {0}".format(jobspec.computingSite))
        # get the queueConfig and corresponding objStoreID_ES
        queueConfigMapper = QueueConfigMapper()
        queueConfig = queueConfigMapper.get_queue(jobspec.computingSite)
        # write to debug log queueConfig.stager
        tmpLog.debug("jobspec.computingSite - {0} queueConfig.stager {1}".format(jobspec.computingSite, queueConfig.stager))
        # check queueConfig stager section to see if jobtype is set
        if "jobtype" in queueConfig.stager:
            if queueConfig.stager["jobtype"] == "Yoda":
                self.Yodajob = True
        # get destination endpoint
        nucleus = jobspec.jobParams["nucleus"]
        agis = self.dbInterface.get_cache("panda_queues.json").data
        dstRSE = [agis[x]["astorages"]["pr"][0] for x in agis if agis[x]["atlas_site"] == nucleus][0]
        # see file /cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints.json
        ddm = self.dbInterface.get_cache("agis_ddmendpoints.json").data
        self.objstoreID = ddm[dstRSE]["id"]
        if self.Yodajob:
            self.pathConvention = int(queueConfig.stager["pathConvention"])
            tmpLog.debug("Yoda Job - PandaID = {0} objstoreID = {1} pathConvention ={2}".format(jobspec.PandaID, self.objstoreID, self.pathConvention))
        else:
            self.pathConvention = None
            tmpLog.debug("PandaID = {0} objstoreID = {1}".format(jobspec.PandaID, self.objstoreID))
        # set the location of the files in fileSpec.objstoreID
        self.set_FileSpec_objstoreID(jobspec, self.objstoreID, self.pathConvention)
        self.RSE_dstpath = queueConfig.stager["RSE_dstPath"]
        # check queueConfig stager section to see if srcRSE is set
        if "srcRSE" in queueConfig.stager:
            srcRSE = queueConfig.stager["srcRSE"]
        else:
            tmpLog.debug("Warning srcRSE not defined in stager portion of queue config file")
        tmpLog.debug("srcRSE - {0} dstRSE - {1}".format(srcRSE, dstRSE))

        # loop over the output files and copy the files
        ifile = 0
        errors = []
        fileList = []
        lfns = []
        fileSpec_list = []
        fileSpec_list = jobspec.get_output_file_specs(skip_done=False)
        msgStr = "#(jobspec.get_output_file_specs(skip_done=False)) = {0}".format(len(fileSpec_list))
        tmpLog.debug(msgStr)
        for fileSpec in fileSpec_list:
            msgstr = "fileSpec: dataset scope - {0} file name - {1} size(Bytes) - {2} adler32 - {3}".format(
                datasetScope, fileSpec.lfn, fileSpec.fsize, fileSpec.chksum
            )
            if fileSpec.fileAttributes is not None and "guid" in fileSpec.fileAttributes:
                msgstr += " guid - {0}".format(fileSpec.fileAttributes["guid"])
            tmpLog.debug(msgstr)

        # for fileSpec in jobspec.get_output_file_specs(skip_done=True):
        for fileSpec in jobspec.get_output_file_specs(skip_done=False):
            scope = "panda"
            if fileSpec.scope is not None:
                scope = fileSpec.scope
            # for Yoda job set the scope to transient
            if self.Yodajob:
                scope = "transient"
            # only print to log file first 25 files
            if ifile < 25:
                msgStr = "fileSpec.lfn - {0} fileSpec.scope - {1}".format(fileSpec.lfn, fileSpec.scope)
                tmpLog.debug(msgStr)
            if ifile == 25:
                msgStr = "printed first 25 files skipping the rest".format(fileSpec.lfn, fileSpec.scope)
                tmpLog.debug(msgStr)
            hash = hashlib.md5()
            hash.update("%s:%s" % (scope, fileSpec.lfn))
            hash_hex = hash.hexdigest()
            correctedscope = "/".join(scope.split("."))
            srcURL = fileSpec.path
            dstURL = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(
                endPoint=self.RSE_dstPath, scope=correctedscope, hash1=hash_hex[0:2], hash2=hash_hex[2:4], lfn=fileSpec.lfn
            )
            if ifile < 25:
                tmpLog.debug("src={srcURL} dst={dstURL}".format(srcURL=srcURL, dstURL=dstURL))
            tmpFile = dict()
            # copy the source file from source to destination skip over if file already exists
            if os.path.exists(dstURL):
                tmpLog.debug("Already copied file {0}".format(dstURL))
                # save for adding to rucio dataset
                tmpFile["scope"] = datasetScope
                tmpFile["name"] = fileSpec.lfn
                tmpFile["bytes"] = fileSpec.fsize
                tmpFile["adler32"] = fileSpec.chksum
                if fileSpec.fileAttributes is not None and "guid" in fileSpec.fileAttributes:
                    tmpFile["meta"] = {"guid": fileSpec.fileAttributes["guid"]}
                else:
                    tmpLog.debug("File - {0} does not have a guid value".format(fileSpec.lfn))
                tmpLog.debug("Adding file {0} to fileList".format(fileSpec.lfn))
                fileList.append(tmpFile)
                lfns.append(fileSpec.lfn)
                # get source RSE
                if srcRSE is None and fileSpec.objstoreID is not None:
                    ddm = self.dbInterface.get_cache("agis_ddmendpoints.json").data
                    srcRSE = [x for x in ddm if ddm[x]["id"] == fileSpec.objstoreID][0]
                    tmpLog.debug("srcRSE - {0} defined from agis_ddmendpoints.json".format(srcRSE))
            else:
                if os.path.exists(srcURL):
                    # check if destination directory exists if not create it
                    dstDIR = os.path.dirname(dstURL)
                    try:
                        if not os.path.exists(dstDIR):
                            os.makedirs(dstDIR)
                            mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP
                            mode = mode | stat.S_IROTH | stat.S_IXOTH | stat.S_ISGID
                            os.chmod(dstDIR, mode)
                        # copy the source file to destination file
                        shutil.copy2(srcURL, dstURL)
                        # save for adding to rucio dataset
                        tmpFile["scope"] = datasetScope
                        tmpFile["name"] = fileSpec.lfn
                        tmpFile["bytes"] = fileSpec.fsize
                        tmpFile["adler32"] = fileSpec.chksum
                        if fileSpec.fileAttributes is not None and "guid" in fileSpec.fileAttributes:
                            tmpFile["meta"] = {"guid": fileSpec.fileAttributes["guid"]}
                        else:
                            tmpLog.debug("File - {0} does not have a guid value".format(fileSpec.lfn))
                        tmpLog.debug("Adding file {0} to fileList".format(fileSpec.lfn))
                        fileList.append(tmpFile)
                        lfns.append(fileSpec.lfn)
                        # get source RSE if not already set
                        if srcRSE is None and fileSpec.objstoreID is not None:
                            ddm = self.dbInterface.get_cache("agis_ddmendpoints.json").data
                            srcRSE = [x for x in ddm if ddm[x]["id"] == fileSpec.objstoreID][0]
                            tmpLog.debug("srcRSE - {0} defined from agis_ddmendpoints.json".format(srcRSE))
                    except (IOError, os.error) as why:
                        errors.append((srcURL, dstURL, str(why)))
                else:
                    errors.append((srcURL, dstURL, "Source file missing"))
            ifile += 1

        # test that srcRSE and dstRSE are defined
        tmpLog.debug("srcRSE - {0} dstRSE - {1}".format(srcRSE, dstRSE))
        errStr = ""
        if srcRSE is None:
            errStr = "Source RSE is not defined "
        if dstRSE is None:
            errStr = errStr + " Desitination RSE is not defined"
        if (srcRSE is None) or (dstRSE is None):
            tmpLog.error(errStr)
            return None, errStr

        # test to see if there are any files to add dataset
        if len(fileList) == 0:
            errStr = "There are no files to add to database"
            tmpLog.error(errStr)
            return None, errStr
        # print out the file list
        tmpLog.debug("fileList - {0}".format(fileList))

        # create the dataset and add files to it and create a transfer rule
        try:
            # register dataset
            rucioAPI = RucioClient()
            tmpLog.debug("register {0}:{1} rse = {2} meta=(hidden: True) lifetime = {3}".format(datasetScope, datasetName, srcRSE, (30 * 24 * 60 * 60)))
            try:
                rucioAPI.add_dataset(datasetScope, datasetName, meta={"hidden": True}, lifetime=30 * 24 * 60 * 60, rse=srcRSE)
            except DataIdentifierAlreadyExists:
                # ignore even if the dataset already exists
                pass
            except Exception:
                errMsg = "Could not create dataset {0}:{1} srcRSE - {2}".format(datasetScope, datasetName, srcRSE)
                core_utils.dump_error_message(tmpLog)
                tmpLog.error(errMsg)
                return None, errMsg
            # add files to dataset
            #  add 500 files at a time
            numfiles = len(fileList)
            maxfiles = 500
            numslices = numfiles / maxfiles
            if (numfiles % maxfiles) > 0:
                numslices = numslices + 1
            start = 0
            for i in range(numslices):
                try:
                    stop = start + maxfiles
                    if stop > numfiles:
                        stop = numfiles

                    rucioAPI.add_files_to_datasets(
                        [{"scope": datasetScope, "name": datasetName, "dids": fileList[start:stop], "rse": srcRSE}], ignore_duplicate=True
                    )
                    start = stop
                except FileAlreadyExists:
                    # ignore if files already exist
                    pass
                except Exception:
                    errMsg = "Could not add files to DS - {0}:{1}  rse - {2} files - {3}".format(datasetScope, datasetName, srcRSE, fileList)
                    core_utils.dump_error_message(tmpLog)
                    tmpLog.error(errMsg)
                    return None, errMsg
            # add rule
            try:
                tmpDID = dict()
                tmpDID["scope"] = datasetScope
                tmpDID["name"] = datasetName
                tmpRet = rucioAPI.add_replication_rule([tmpDID], 1, dstRSE, lifetime=30 * 24 * 60 * 60)
                ruleIDs = tmpRet[0]
                tmpLog.debug("registered dataset {0}:{1} with rule {2}".format(datasetScope, datasetName, str(ruleIDs)))
                # group the output files together by the Rucio transfer rule
                jobspec.set_groups_to_files({ruleIDs: {"lfns": lfns, "groupStatus": "pending"}})
                msgStr = "jobspec.set_groups_to_files -Rucio rule - {0}, lfns - {1}, groupStatus - pending".format(ruleIDs, lfns)
                tmpLog.debug(msgStr)
                tmpLog.debug("call self.dbInterface.set_file_group(jobspec.get_output_file_specs(skip_done=True),ruleIDs,pending)")
                tmpStat = self.dbInterface.set_file_group(jobspec.get_output_file_specs(skip_done=True), ruleIDs, "transferring")
                tmpLog.debug("called self.dbInterface.set_file_group(jobspec.get_output_file_specs(skip_done=True),ruleIDs,transferring)")
                tmpStat = True
                tmpMsg = "created Rucio rule successfully"
            except DuplicateRule:
                # ignore duplicated rule
                tmpLog.debug("rule is already available")
            except Exception:
                errMsg = "Error creating rule for dataset {0}:{1}".format(datasetScope, datasetName)
                core_utils.dump_error_message(tmpLog)
                tmpLog.debug(errMsg)
                return None, errMsg
            # update file group status
            self.dbInterface.update_file_group_status(ruleIDs, "transferring")
        except Exception:
            core_utils.dump_error_message(tmpLog)
            # treat as a temporary error
            tmpStat = None
            tmpMsg = "failed to add a rule for {0}:{1}".format(datasetScope, datasetName)

        #  Now test for any errors
        if errors:
            for error in errors:
                tmpLog.debug("copy error source {0} destination {1} Reason {2}".format(error[0], error[1], error[2]))
            raise Error(errors)
        # otherwise we are OK
        tmpLog.debug("stop")
        return tmpStat, tmpMsg

    # zip output files
    def zip_output(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        return self.simple_zip_output(jobspec, tmpLog)
