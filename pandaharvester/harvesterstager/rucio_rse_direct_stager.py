from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterstager import yoda_rucio_rse_direct_stager
from pandaharvester.harvesterstager.yoda_rucio_rse_direct_stager import (
    YodaRseDirectStager,
)
from rucio.client import Client as RucioClient
from rucio.common.exception import (
    DataIdentifierAlreadyExists,
    DataIdentifierNotFound,
    DuplicateRule,
    FileAlreadyExists,
)

# logger
_logger = core_utils.setup_logger("rucio_rse_direct_stager")

# stager plugin to use Rucio to transfer files from local RSE via local sitemover to nucleus


class RucioRseDirectStager(YodaRseDirectStager):
    # constructor
    def __init__(self, **kwarg):
        YodaRseDirectStager.__init__(self, **kwarg)
        self.changeFileStatusOnSuccess = False

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

    # check status of Rucio transfer
    # check status

    def check_stage_out_status(self, jobspec):
        tmpStat = True
        tmpMsg = ""
        # make logger
        tmpLog = self.make_logger(baseLogger, f"PandaID={jobspec.PandaID} ThreadID={threading.current_thread().ident}", method_name="check_stage_out_status")
        tmpLog.debug("start")
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
            if "transferred" in groupStatus:
                # already succeeded
                pass
            elif "failed" in groupStatus:
                # transfer failure
                tmpStat = False
                tmpMsg = f"rucio rule for {datasetScope}:{datasetName} already failed"
            elif "transferring" in groupStatus:
                # transfer started in Rucio check status
                try:
                    result = rucioAPI.get_replication_rule(rucioRule, False)
                    if result["state"] == "OK":
                        # files transfered to nucleus
                        tmpLog.debug(f"Files for Rucio Rule {rucioRule} successfully transferred")
                        self.dbInterface.update_file_group_status(rucioRule, "transferred")
                        # set the fileSpec status for these files
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

    # trigger stageout via Rucio to nucleus site

    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = self.make_logger(_logger, f"PandaID={jobspec.PandaID} ThreadID={threading.current_thread().ident} ", method_name="trigger_stage_out")
        tmpLog.debug("executing base trigger_stage_out")
        tmpStat, tmpMsg = YodaRseDirect.trigger_stage_out(self, jobspec)
        tmpLog.debug(f"got {tmpStat} {tmpMsg}")
        if tmpStat is not True:
            return tmpStat, tmpMsg
        # Now that output files have been all copied to Local RSE register transient dataset
        # loop over all transfers
        tmpStat = None
        tmpMsg = ""
        srcRSE = None
        dstRSE = None
        datasetName = f"panda.harvester.{jobspec.PandaID}.{str(uuid.uuid4())}"
        datasetScope = "transient"
        # get destination endpoint
        nucleus = jobspec.jobParams["nucleus"]
        agis = self.dbInterface.get_cache("panda_queues.json").data
        dstRSE = [agis[x]["astorages"]["pr"][0] for x in agis if agis[x]["atlas_site"] == nucleus][0]

        # get the list of output files to transfer
        fileSpecs = jobspec.get_output_file_specs(skip_done=True)
        fileList = []
        lfns = []
        for fileSpec in fileSpecs:
            tmpFile = dict()
            tmpFile["scope"] = datasetScope
            tmpFile["name"] = fileSpec.lfn
            tmpFile["bytes"] = fileSpec.fsize
            tmpFile["adler32"] = fileSpec.chksum
            tmpFile["meta"] = {"guid": fileSpec.fileAttributes["guid"]}
            fileList.append(tmpFile)
            lfns.append(fileSpec.lfn)
            # get source RSE
            if srcRSE is None and fileSpec.objstoreID is not None:
                ddm = self.dbInterface.get_cache("agis_ddmendpoints.json").data
                srcRSE = [x for x in ddm if ddm[x]["id"] == fileSpec.objstoreID][0]

        # test that srcRSE and dstRSE are defined
        errStr = ""
        if srcRSE is None:
            errStr = "Source RSE is not defined "
        if dstRSE is None:
            errStr = errStr + " Desitination RSE is not defined"
        if (srcRSE is None) or (dstRSE is None):
            tmpLog.error(errStr)
            return False, errStr

        # create the dataset and add files to it and create a transfer rule
        try:
            # register dataset
            tmpLog.debug(f"register {datasetScope}:{datasetName}")
            rucioAPI = RucioClient()
            try:
                rucioAPI.add_dataset(datasetScope, datasetName, meta={"hidden": True}, lifetime=30 * 24 * 60 * 60, rse=srcRSE)
            except DataIdentifierAlreadyExists:
                # ignore even if the dataset already exists
                pass
            except Exception:
                tmpLog.error(f"Could not create dataset with scope: {datasetScope} Name: {datasetName} in Rucio")
                raise

            # add files to dataset
            try:
                rucioAPI.add_files_to_datasets([{"scope": datasetScope, "name": datasetName, "dids": fileList, "rse": srcRSE}], ignore_duplicate=True)
            except FileAlreadyExists:
                # ignore if files already exist
                pass
            except Exception:
                tmpLog.error(f"Could add files to dataset with scope: {datasetScope} Name: {datasetName} in Rucio")
                raise

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
                tmpStat = self.dbInterface.set_file_group(jobspec.get_output_file_specs(skip_done=True), ruleIDs, "pending")
                tmpLog.debug("called self.dbInterface.set_file_group(jobspec.get_output_file_specs(skip_done=True),ruleIDs,pending)")
                tmpStat = True
                tmpMsg = "created Rucio rule successfully"
            except DuplicateRule:
                # ignore duplicated rule
                tmpLog.debug("rule is already available")
            except Exception:
                tmpLog.debug(f"Error creating rule for dataset {datasetScope}:{datasetName}")
                raise
            # update file group status
            self.dbInterface.update_file_group_status(ruleIDs, "transferring")
        except Exception:
            core_utils.dump_error_message(tmpLog)
            # treat as a temporary error
            tmpStat = None
            tmpMsg = f"failed to add a rule for {datasetScope}:{datasetName}"

        return tmpStat, tmpMsg
