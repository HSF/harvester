from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestermover import mover_utils
from pandaharvester.harvesterstager import go_bulk_stager
from pandaharvester.harvesterstager.go_bulk_stager import GlobusBulkStager
from rucio.client import Client as RucioClient
from rucio.common.exception import (
    DataIdentifierAlreadyExists,
    DataIdentifierNotFound,
    DuplicateRule,
    FileAlreadyExists,
)

# logger
_logger = core_utils.setup_logger("go_rucio_stager")
go_bulk_stager._logger = _logger


# plugin with Globus + Rucio + bulk transfers
class GlobusRucioStager(GlobusBulkStager):
    # constructor
    def __init__(self, **kwarg):
        GlobusBulkStager.__init__(self, **kwarg)
        self.changeFileStatusOnSuccess = False

    # check status
    def check_stage_out_status(self, jobspec):
        # make logger
        tmpLog = self.make_logger(_logger, f"PandaID={jobspec.PandaID}", method_name="check_stage_out_status")
        tmpLog.debug("executing base check_stage_out_status")
        tmpStat, tmpMsg = GlobusBulkStager.check_stage_out_status(self, jobspec)
        tmpLog.debug(f"got {tmpStat} {tmpMsg}")
        if tmpStat is not True:
            return tmpStat, tmpMsg
        # get transfer groups
        groups = jobspec.get_groups_of_output_files()
        if len(groups) == 0:
            return tmpStat, tmpMsg
        # get the queueConfig and corresponding objStoreID_ES
        queueConfigMapper = QueueConfigMapper()
        queueConfig = queueConfigMapper.get_queue(jobspec.computingSite)
        # write to debug log queueConfig.stager
        tmpLog.debug(f"jobspec.computingSite - {jobspec.computingSite} queueConfig.stager {queueConfig.stager}")
        # check queueConfig stager section to see if srcRSE is set
        if "srcRSE" in queueConfig.stager:
            srcRSE = queueConfig.stager["srcRSE"]
        else:
            tmpLog.debug("Warning srcRSE not defined in stager portion of queue config file")
        # get destination endpoint
        nucleus = jobspec.jobParams["nucleus"]
        agis = self.dbInterface.get_cache("panda_queues.json").data
        dstRSE = [agis[x]["astorages"]["pr"][0] for x in agis if agis[x]["atlas_site"] == nucleus][0]
        # if debugging log source and destination RSEs
        tmpLog.debug(f"srcRSE - {srcRSE} dstRSE - {dstRSE}")
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
        # check queueConfig stager section to see if jobtype is set
        if "jobtype" in queueConfig.stager:
            if queueConfig.stager["jobtype"] == "Yoda":
                self.Yodajob = True
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
        # create the Rucio Client
        try:
            # register dataset
            rucioAPI = RucioClient()
        except Exception:
            core_utils.dump_error_message(tmpLog)
            # treat as a temporary error
            tmpStat = None
            tmpMsg = f"failed to add a rule for {datasetScope}:{datasetName}"
            return tmpStat, tmpMsg
        # loop over all transfers
        tmpStat = True
        tmpMsg = ""
        for transferID in groups:
            if transferID is None:
                continue
            datasetName = f"panda.harvester.{jobspec.PandaID}.{transferID}"
            datasetScope = "transient"
            # lock
            have_db_lock = self.dbInterface.get_object_lock(transferID, lock_interval=120)
            if not have_db_lock:
                msgStr = f"escape since {transferID} is locked by another thread"
                tmpLog.debug(msgStr)
                return None, msgStr
            # get transfer status
            groupStatus = self.dbInterface.get_file_group_status(transferID)
            if "hopped" in groupStatus:
                # already succeeded
                pass
            elif "failed" in groupStatus:
                # transfer failure
                tmpStat = False
                tmpMsg = f"rucio rule for {datasetScope}:{datasetName} already failed"
            elif "hopping" in groupStatus:
                # check rucio rule
                ruleStatus = "FAILED"
                try:
                    tmpLog.debug(f"check state for {datasetScope}:{datasetName}")
                    for ruleInfo in rucioAPI.list_did_rules(datasetScope, datasetName):
                        if ruleInfo["rse_expression"] != dstRSE:
                            continue
                        ruleStatus = ruleInfo["state"]
                        tmpLog.debug(f"got state={ruleStatus}")
                        if ruleStatus == "OK":
                            break
                except DataIdentifierNotFound:
                    tmpLog.error("dataset not found")
                except Exception:
                    core_utils.dump_error_message(tmpLog)
                    ruleStatus = None
                if ruleStatus in ["FAILED", "CANCELED"]:
                    # transfer failure
                    tmpStat = False
                    tmpMsg = f"rucio rule for {datasetScope}:{datasetName} failed with {ruleStatus}"
                    # update file group status
                    self.dbInterface.update_file_group_status(transferID, "failed")
                elif ruleStatus == "OK":
                    # update successful file group status
                    self.dbInterface.update_file_group_status(transferID, "hopped")
                else:
                    # replicating or temporary error
                    tmpStat = None
                    tmpMsg = f"replicating or temporary error for {datasetScope}:{datasetName}"
            else:
                # make rucio rule
                fileSpecs = self.dbInterface.get_files_with_group_id(transferID)
                fileList = []
                for fileSpec in fileSpecs:
                    tmpFile = dict()
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
                    # get source RSE
                    if srcRSE is None and fileSpec.objstoreID is not None:
                        ddm = self.dbInterface.get_cache("agis_ddmendpoints.json").data
                        srcRSE = [x for x in ddm if ddm[x]["id"] == fileSpec.objstoreID][0]
                try:
                    # register dataset
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
                        raise
                        # return None,errMsg
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
                    except DuplicateRule:
                        # ignore duplicated rule
                        tmpLog.debug("rule is already available")
                    except Exception:
                        errMsg = f"Error creating rule for dataset {datasetScope}:{datasetName}"
                        core_utils.dump_error_message(tmpLog)
                        tmpLog.debug(errMsg)
                        # raise
                        return None, errMsg
                    # update file group status
                    self.dbInterface.update_file_group_status(transferID, "hopping")
                except Exception:
                    core_utils.dump_error_message(tmpLog)
                    # treat as a temporary error
                    tmpStat = None
                    tmpMsg = f"failed to add a rule for {datasetScope}:{datasetName}"
            # release lock
            self.dbInterface.release_object_lock(transferID)
            # escape if already failed
            if tmpStat is False:
                break
        # all done
        if tmpStat is True:
            self.set_FileSpec_status(jobspec, "finished")
        tmpLog.debug(f"done with {tmpStat} : {tmpMsg}")
        return tmpStat, tmpMsg
