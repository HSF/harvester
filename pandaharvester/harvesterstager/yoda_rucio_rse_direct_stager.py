import datetime
import errno
import hashlib
import os
import os.path
import shutil
import stat
import string
import sys
import tarfile
import threading
import time
import uuid

from rucio.client import Client as RucioClient
from rucio.common.exception import (
    DataIdentifierAlreadyExists,
    DataIdentifierNotFound,
    DuplicateRule,
    FileAlreadyExists,
)

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestermover import mover_utils
from pandaharvester.harvesterstager.base_stager import BaseStager

from .base_stager import BaseStager


def dump(tmpLog, obj):
    for attr in dir(obj):
        if hasattr(obj, attr):
            tmpLog.debug(f"obj.{attr} = {getattr(obj, attr)}")


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
        tmpLog = self.make_logger(baseLogger, f"ThreadID={threading.current_thread().ident}", method_name="YodaRucioRseDirectStager __init__ ")
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
        tmpLog = self.make_logger(baseLogger, f"PandaID={jobspec.PandaID} ThreadID={threading.current_thread().ident}", method_name="check_stage_out_status")
        tmpLog.debug("start")
        # check that jobspec.computingSite is defined
        if jobspec.computingSite is None:
            # not found
            tmpLog.error("jobspec.computingSite is not defined")
            return False, "jobspec.computingSite is not defined"
        else:
            tmpLog.debug(f"jobspec.computingSite : {jobspec.computingSite}")
        # get the queueConfig and corresponding objStoreID_ES
        queueConfigMapper = QueueConfigMapper()
        queueConfig = queueConfigMapper.get_queue(jobspec.computingSite)
        # write to debug log queueConfig.stager
        tmpLog.debug(f"jobspec.computingSite - {jobspec.computingSite} queueConfig.stager {queueConfig.stager}")
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
            tmpLog.debug(f"Yoda Job - PandaID = {jobspec.PandaID} objstoreID = {self.objstoreID} pathConvention ={self.pathConvention}")
        else:
            self.pathConvention = None
            tmpLog.debug(f"PandaID = {jobspec.PandaID} objstoreID = {self.objstoreID}")
        # set the location of the files in fileSpec.objstoreID
        self.set_FileSpec_objstoreID(jobspec, self.objstoreID, self.pathConvention)
        # Get the files grouped by Rucio Rule ID
        groups = jobspec.get_groups_of_output_files()
        if len(groups) == 0:
            tmpLog.debug("No Rucio Rules")
            return None, "No Rucio Rules"
        tmpLog.debug(f"#Rucio Rules - {len(groups)} - Rules - {groups}")

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
                msgStr = f"escape since {rucioRule} is locked by another thread"
                tmpLog.debug(msgStr)
                return None, msgStr
            # get transfer status
            groupStatus = self.dbInterface.get_file_group_status(rucioRule)
            tmpLog.debug(f"rucioRule - {rucioRule} - groupStatus - {groupStatus}")
            if "transferred" in groupStatus:
                # already succeeded - set the fileSpec status for these files
                self.set_FileSpec_objstoreID(jobspec, self.objstoreID, self.pathConvention)
                pass
            elif "failed" in groupStatus:
                # transfer failure
                tmpStat = False
                tmpMsg = f"rucio rule for {datasetScope}:{datasetName} already failed"
            elif "transferring" in groupStatus or "pending" in groupStatus:
                # transfer started in Rucio check status
                try:
                    result = rucioAPI.get_replication_rule(rucioRule, False)
                    if result["state"] == "OK":
                        # files transfered to nucleus
                        tmpLog.debug(f"Files for Rucio Rule {rucioRule} successfully transferred")
                        self.dbInterface.update_file_group_status(rucioRule, "transferred")
                        # set the fileSpec status for these files
                        self.set_FileSpec_objstoreID(jobspec, self.objstoreID, self.pathConvention)
                        self.set_FileSpec_status(jobspec, "finished")
                    elif result["state"] == "FAILED":
                        # failed Rucio Transfer
                        tmpStat = False
                        tmpMsg = f"Failed Rucio Transfer - Rucio Rule - {rucioRule}"
                        tmpLog.debug(tmpMsg)
                        self.set_FileSpec_status(jobspec, "failed")
                    elif result["state"] == "STUCK":
                        tmpStat = None
                        tmpMsg = f"Rucio Transfer Rule {rucioRule} Stuck"
                        tmpLog.debug(tmpMsg)
                except BaseException:
                    tmpStat = None
                    tmpMsg = f"Could not get information or Rucio Rule {rucioRule}"
                    tmpLog.error(tmpMsg)
                    pass
            # release the lock
            if have_db_lock:
                tmpLog.debug(f"attempt to release DB lock for Rucio Rule {rucioRule}")
                release_db_lock = self.dbInterface.release_object_lock(rucioRule)
                if release_db_lock:
                    tmpLog.debug(f"released DB lock for rucioRule - {rucioRule}")
                    have_db_lock = False
                else:
                    msgStr = f" Could not release DB lock for {rucioRule}"
                    tmpLog.error(msgStr)
                    return None, msgStr

        tmpLog.debug("stop")
        return tmpStat, tmpMsg

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, f"PandaID={jobspec.PandaID} ThreadID={threading.current_thread().ident}", method_name="trigger_stage_out")
        tmpLog.debug("start")
        # initialize some values
        tmpStat = None
        tmpMsg = ""
        srcRSE = None
        dstRSE = None
        datasetName = f"panda.harvester.{jobspec.PandaID}.{str(uuid.uuid4())}"
        datasetScope = "transient"
        # check that jobspec.computingSite is defined
        if jobspec.computingSite is None:
            # not found
            tmpLog.error("jobspec.computingSite is not defined")
            return False, "jobspec.computingSite is not defined"
        else:
            tmpLog.debug(f"jobspec.computingSite : {jobspec.computingSite}")
        # get the queueConfig and corresponding objStoreID_ES
        queueConfigMapper = QueueConfigMapper()
        queueConfig = queueConfigMapper.get_queue(jobspec.computingSite)
        # write to debug log queueConfig.stager
        tmpLog.debug(f"jobspec.computingSite - {jobspec.computingSite} queueConfig.stager {queueConfig.stager}")
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
            tmpLog.debug(f"Yoda Job - PandaID = {jobspec.PandaID} objstoreID = {self.objstoreID} pathConvention ={self.pathConvention}")
        else:
            self.pathConvention = None
            tmpLog.debug(f"PandaID = {jobspec.PandaID} objstoreID = {self.objstoreID}")
        # set the location of the files in fileSpec.objstoreID
        self.set_FileSpec_objstoreID(jobspec, self.objstoreID, self.pathConvention)
        self.RSE_dstpath = queueConfig.stager["RSE_dstPath"]
        # check queueConfig stager section to see if srcRSE is set
        if "srcRSE" in queueConfig.stager:
            srcRSE = queueConfig.stager["srcRSE"]
        else:
            tmpLog.debug("Warning srcRSE not defined in stager portion of queue config file")
        tmpLog.debug(f"srcRSE - {srcRSE} dstRSE - {dstRSE}")

        # loop over the output files and copy the files
        ifile = 0
        errors = []
        fileList = []
        lfns = []
        fileSpec_list = []
        fileSpec_list = jobspec.get_output_file_specs(skip_done=False)
        msgStr = f"#(jobspec.get_output_file_specs(skip_done=False)) = {len(fileSpec_list)}"
        tmpLog.debug(msgStr)
        for fileSpec in fileSpec_list:
            msgstr = f"fileSpec: dataset scope - {datasetScope} file name - {fileSpec.lfn} size(Bytes) - {fileSpec.fsize} adler32 - {fileSpec.chksum}"
            if fileSpec.fileAttributes is not None and "guid" in fileSpec.fileAttributes:
                msgstr += f" guid - {fileSpec.fileAttributes['guid']}"
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
                msgStr = f"fileSpec.lfn - {fileSpec.lfn} fileSpec.scope - {fileSpec.scope}"
                tmpLog.debug(msgStr)
            if ifile == 25:
                msgStr = "printed first 25 files skipping the rest".format(fileSpec.lfn, fileSpec.scope)
                tmpLog.debug(msgStr)
            hash = hashlib.md5()
            hash.update(f"{scope}:{fileSpec.lfn}")
            hash_hex = hash.hexdigest()
            correctedscope = "/".join(scope.split("."))
            srcURL = fileSpec.path
            dstURL = f"{self.RSE_dstPath}/{correctedscope}/{hash_hex[0:2]}/{hash_hex[2:4]}/{fileSpec.lfn}"
            if ifile < 25:
                tmpLog.debug(f"src={srcURL} dst={dstURL}")
            tmpFile = dict()
            # copy the source file from source to destination skip over if file already exists
            if os.path.exists(dstURL):
                tmpLog.debug(f"Already copied file {dstURL}")
                # save for adding to rucio dataset
                tmpFile["scope"] = datasetScope
                tmpFile["name"] = fileSpec.lfn
                tmpFile["bytes"] = fileSpec.fsize
                tmpFile["adler32"] = fileSpec.chksum
                if fileSpec.fileAttributes is not None and "guid" in fileSpec.fileAttributes:
                    tmpFile["meta"] = {"guid": fileSpec.fileAttributes["guid"]}
                else:
                    tmpLog.debug(f"File - {fileSpec.lfn} does not have a guid value")
                tmpLog.debug(f"Adding file {fileSpec.lfn} to fileList")
                fileList.append(tmpFile)
                lfns.append(fileSpec.lfn)
                # get source RSE
                if srcRSE is None and fileSpec.objstoreID is not None:
                    ddm = self.dbInterface.get_cache("agis_ddmendpoints.json").data
                    srcRSE = [x for x in ddm if ddm[x]["id"] == fileSpec.objstoreID][0]
                    tmpLog.debug(f"srcRSE - {srcRSE} defined from agis_ddmendpoints.json")
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
                            tmpLog.debug(f"File - {fileSpec.lfn} does not have a guid value")
                        tmpLog.debug(f"Adding file {fileSpec.lfn} to fileList")
                        fileList.append(tmpFile)
                        lfns.append(fileSpec.lfn)
                        # get source RSE if not already set
                        if srcRSE is None and fileSpec.objstoreID is not None:
                            ddm = self.dbInterface.get_cache("agis_ddmendpoints.json").data
                            srcRSE = [x for x in ddm if ddm[x]["id"] == fileSpec.objstoreID][0]
                            tmpLog.debug(f"srcRSE - {srcRSE} defined from agis_ddmendpoints.json")
                    except (IOError, os.error) as why:
                        errors.append((srcURL, dstURL, str(why)))
                else:
                    errors.append((srcURL, dstURL, "Source file missing"))
            ifile += 1

        # test that srcRSE and dstRSE are defined
        tmpLog.debug(f"srcRSE - {srcRSE} dstRSE - {dstRSE}")
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
        tmpLog.debug(f"fileList - {fileList}")

        # create the dataset and add files to it and create a transfer rule
        try:
            # register dataset
            rucioAPI = RucioClient()
            tmpLog.debug(f"register {datasetScope}:{datasetName} rse = {srcRSE} meta=(hidden: True) lifetime = {30 * 24 * 60 * 60}")
            try:
                rucioAPI.add_dataset(datasetScope, datasetName, meta={"hidden": True}, lifetime=30 * 24 * 60 * 60, rse=srcRSE)
            except DataIdentifierAlreadyExists:
                # ignore even if the dataset already exists
                pass
            except Exception:
                errMsg = f"Could not create dataset {datasetScope}:{datasetName} srcRSE - {srcRSE}"
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
                    errMsg = f"Could not add files to DS - {datasetScope}:{datasetName}  rse - {srcRSE} files - {fileList}"
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
                tmpLog.debug(f"registered dataset {datasetScope}:{datasetName} with rule {str(ruleIDs)}")
                # group the output files together by the Rucio transfer rule
                jobspec.set_groups_to_files({ruleIDs: {"lfns": lfns, "groupStatus": "pending"}})
                msgStr = f"jobspec.set_groups_to_files -Rucio rule - {ruleIDs}, lfns - {lfns}, groupStatus - pending"
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
                errMsg = f"Error creating rule for dataset {datasetScope}:{datasetName}"
                core_utils.dump_error_message(tmpLog)
                tmpLog.debug(errMsg)
                return None, errMsg
            # update file group status
            self.dbInterface.update_file_group_status(ruleIDs, "transferring")
        except Exception:
            core_utils.dump_error_message(tmpLog)
            # treat as a temporary error
            tmpStat = None
            tmpMsg = f"failed to add a rule for {datasetScope}:{datasetName}"

        #  Now test for any errors
        if errors:
            for error in errors:
                tmpLog.debug(f"copy error source {error[0]} destination {error[1]} Reason {error[2]}")
            raise Error(errors)
        # otherwise we are OK
        tmpLog.debug("stop")
        return tmpStat, tmpMsg

    # zip output files
    def zip_output(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, f"PandaID={jobspec.PandaID}", method_name="zip_output")
        return self.simple_zip_output(jobspec, tmpLog)
