import os
import sys
import os.path
import zipfile
import hashlib
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

# logger
_logger = core_utils.setup_logger("go_stager")


def dump(obj):
    for attr in dir(obj):
        if hasattr(obj, attr):
            print("obj.%s = %s" % (attr, getattr(obj, attr)))


# plugin for stager with FTS
class GlobusStager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        # create Globus Transfer Client
        tmpLog = self.make_logger(_logger, method_name="GlobusStager __init__ ")
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

    # set FileSpec.status
    def set_FileSpec_status(self, jobspec, status):
        # loop over all output files
        for fileSpec in jobspec.outFiles:
            fileSpec.status = status

    # check status
    def check_stage_out_status(self, jobspec):
        # make logger
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="check_stage_out_status")
        tmpLog.debug("start")
        # get label
        label = self.make_label(jobspec)
        tmpLog.debug("label={0}".format(label))
        # get transfer task
        tmpStat, transferTasks = globus_utils.get_transfer_tasks(tmpLog, self.tc, label)
        # return a temporary error when failed to get task
        if not tmpStat:
            errStr = "failed to get transfer task"
            tmpLog.error(errStr)
            return None, errStr
        # return a fatal error when task is missing # FIXME retry instead?
        if label not in transferTasks:
            errStr = "transfer task is missing"
            tmpLog.error(errStr)
            return False, errStr
        # succeeded
        transferID = transferTasks[label]["task_id"]
        if transferTasks[label]["status"] == "SUCCEEDED":
            tmpLog.debug("transfer task {} succeeded".format(transferID))
            self.set_FileSpec_status(jobspec, "finished")
            return True, ""
        # failed
        if transferTasks[label]["status"] == "FAILED":
            errStr = "transfer task {} failed".format(transferID)
            tmpLog.error(errStr)
            self.set_FileSpec_status(jobspec, "failed")
            return False, errStr
        # another status
        tmpStr = "transfer task {0} status: {1}".format(transferID, transferTasks[label]["status"])
        tmpLog.debug(tmpStr)
        return None, ""

    # trigger stage out

    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="trigger_stage_out")
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
        # get label
        label = self.make_label(jobspec)
        tmpLog.debug("label={0}".format(label))
        # get transfer tasks
        tmpStat, transferTasks = globus_utils.get_transfer_tasks(tmpLog, self.tc, label)
        if not tmpStat:
            errStr = "failed to get transfer tasks"
            tmpLog.error(errStr)
            return False, errStr
        # check if already queued
        if label in transferTasks:
            tmpLog.debug("skip since already queued with {0}".format(str(transferTasks[label])))
            return True, ""
        # set the Globus destination Endpoint id and path will get them from Agis eventually
        from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper

        queueConfigMapper = QueueConfigMapper()
        queueConfig = queueConfigMapper.get_queue(jobspec.computingSite)
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
                tmpLog.error(errMsg)
                tmpRetVal = (False, errMsg)
                return tmpRetVal
            # both endpoints activated now prepare to transfer data
            tdata = TransferData(self.tc, self.srcEndpoint, self.dstEndpoint, label=label, sync_level="checksum")
        except BaseException:
            errStat, errMsg = globus_utils.handle_globus_exception(tmpLog)
            tmpRetVal = (errStat, errMsg)
            return tmpRetVal
        # loop over all files
        fileAttrs = jobspec.get_output_file_attributes()
        lfns = []
        for fileSpec in jobspec.outFiles:
            scope = fileAttrs[fileSpec.lfn]["scope"]
            hash = hashlib.md5()
            hash.update("%s:%s" % (scope, fileSpec.lfn))
            hash_hex = hash.hexdigest()
            correctedscope = "/".join(scope.split("."))
            srcURL = fileSpec.path
            dstURL = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(
                endPoint=self.Globus_dstPath, scope=correctedscope, hash1=hash_hex[0:2], hash2=hash_hex[2:4], lfn=fileSpec.lfn
            )
            tmpLog.debug("src={srcURL} dst={dstURL}".format(srcURL=srcURL, dstURL=dstURL))
            # add files to transfer object - tdata
            if os.access(srcURL, os.R_OK):
                tmpLog.debug("tdata.add_item({},{})".format(srcURL, dstURL))
                tdata.add_item(srcURL, dstURL)
                lfns.append(fileSpec.lfn)
            else:
                errMsg = "source file {} does not exist".format(srcURL)
                tmpLog.error(errMsg)
                tmpRetVal = (False, errMsg)
                return tmpRetVal
        # submit transfer
        try:
            transfer_result = self.tc.submit_transfer(tdata)
            # check status code and message
            tmpLog.debug(str(transfer_result))
            if transfer_result["code"] == "Accepted":
                # succeeded
                # set transfer ID which are used for later lookup
                transferID = transfer_result["task_id"]
                tmpLog.debug("successfully submitted id={0}".format(transferID))
                jobspec.set_groups_to_files({transferID: {"lfns": lfns, "groupStatus": "active"}})
                # set
                for fileSpec in jobspec.outFiles:
                    if fileSpec.fileAttributes is None:
                        fileSpec.fileAttributes = {}
                        fileSpec.fileAttributes["transferID"] = transferID
            else:
                tmpRetVal = (False, transfer_result["message"])
        except Exception as e:
            errStat, errMsg = globus_utils.handle_globus_exception(tmpLog)
            if errMsg is None:
                errtype, errvalue = sys.exc_info()[:2]
                errMsg = "{0} {1}".format(errtype.__name__, errvalue)
            tmpRetVal = (errStat, errMsg)
        # return
        tmpLog.debug("done")
        return tmpRetVal

    # zip output files
    def zip_output(self, jobspec):
        # make logger
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        tmpLog.debug("start")
        try:
            for fileSpec in jobspec.outFiles:
                if self.zipDir == "${SRCDIR}":
                    # the same directory as src
                    zipDir = os.path.dirname(next(iter(fileSpec.associatedFiles)).path)
                else:
                    zipDir = self.zipDir
                zipPath = os.path.join(zipDir, fileSpec.lfn)
                # remove zip file just in case
                try:
                    os.remove(zipPath)
                except BaseException:
                    pass
                # make zip file
                with zipfile.ZipFile(zipPath, "w", zipfile.ZIP_STORED) as zf:
                    for assFileSpec in fileSpec.associatedFiles:
                        zf.write(assFileSpec.path, os.path.basename(assFileSpec.path))
                # set path
                fileSpec.path = zipPath
                # get size
                statInfo = os.stat(zipPath)
                fileSpec.fsize = statInfo.st_size
        except BaseException:
            errMsg = core_utils.dump_error_message(tmpLog)
            return False, "failed to zip with {0}".format(errMsg)
        tmpLog.debug("done")
        return True, ""

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
