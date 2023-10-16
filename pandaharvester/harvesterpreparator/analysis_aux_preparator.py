import os
import shutil

try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

import requests
import requests.exceptions

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermover import mover_utils

# logger
baseLogger = core_utils.setup_logger("analysis_aux_preparator")


# preparator plugin for analysis auxiliary inputs
class AnalysisAuxPreparator(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.containerRuntime = None
        self.externalCommand = {}
        self.maxAttempts = 3
        PluginBase.__init__(self, **kwarg)

    # trigger preparation
    def trigger_preparation(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="trigger_preparation")
        tmpLog.debug("start")
        # loop over all inputs
        allDone = True
        bulkExtCommand = {}
        tmpLog.debug("number of inFiles : {0}".format(len(jobspec.inFiles)))
        for tmpFileSpec in jobspec.inFiles:
            # local access path
            url = tmpFileSpec.url
            accPath = self.make_local_access_path(tmpFileSpec.scope, tmpFileSpec.lfn)
            accPathTmp = accPath + ".tmp"
            tmpLog.debug("url : {0} accPath : {1}".format(url, accPath))
            # check if already exits
            if os.path.exists(accPath):
                continue
            # make directories if needed
            if not os.path.isdir(os.path.dirname(accPath)):
                os.makedirs(os.path.dirname(accPath))
            # check if use an external command
            extCommand = None
            for protocol in self.externalCommand:
                if url.startswith(protocol):
                    extCommand = self.externalCommand[protocol]
                    # collect file info to execute the command later
                    bulkExtCommand.setdefault(protocol, {"command": extCommand, "url": [], "dst": [], "lfn": []})
                    bulkExtCommand[protocol]["url"].append(url)
                    bulkExtCommand[protocol]["dst"].append(accPath)
                    bulkExtCommand[protocol]["lfn"].append(tmpFileSpec.lfn)
                    break
            # execute the command later
            if extCommand is not None:
                continue
            # execute
            return_code = 1
            if url.startswith("http"):
                try:
                    tmpLog.debug("getting via http from {0} to {1}".format(url, accPathTmp))
                    res = requests.get(url, timeout=180, verify=False)
                    if res.status_code == 200:
                        with open(accPathTmp, "wb") as f:
                            f.write(res.content)
                        tmpLog.debug("Successfully fetched file - {0}".format(accPathTmp))
                        return_code = 0
                    else:
                        errMsg = "failed to get {0} with StatusCode={1} {2}".format(url, res.status_code, res.text)
                        tmpLog.error(errMsg)
                except requests.exceptions.ReadTimeout:
                    tmpLog.error("read timeout when getting data from {0}".format(url))
                except Exception:
                    core_utils.dump_error_message(tmpLog)
            elif url.startswith("docker"):
                if self.containerRuntime is None:
                    tmpLog.debug("container downloading is disabled")
                    continue
                if self.containerRuntime == "docker":
                    args = ["docker", "save", "-o", accPathTmp, url.split("://")[-1]]
                    return_code = self.make_image(jobspec, args)
                elif self.containerRuntime == "singularity":
                    args = ["singularity", "build", "--sandbox", accPathTmp, url]
                    return_code = self.make_image(jobspec, args)
                elif self.containerRuntime == "shifter":
                    args = ["shifterimg", "pull", url]
                    return_code = self.make_image(jobspec, args)
                else:
                    tmpLog.error("unsupported container runtime : {0}".format(self.containerRuntime))
            elif url.startswith("/"):
                try:
                    shutil.copyfile(url, accPathTmp)
                    return_code = 0
                except Exception:
                    core_utils.dump_error_message(tmpLog)
            else:
                tmpLog.error("unsupported protocol in {0}".format(url))
            # remove empty files
            if os.path.exists(accPathTmp) and os.path.getsize(accPathTmp) == 0:
                return_code = 1
                tmpLog.debug("remove empty file - {0}".format(accPathTmp))
                try:
                    os.remove(accPathTmp)
                except Exception:
                    core_utils.dump_error_message(tmpLog)
            # rename
            if return_code == 0:
                try:
                    os.rename(accPathTmp, accPath)
                except Exception:
                    return_code = 1
                    core_utils.dump_error_message(tmpLog)
            if return_code != 0:
                allDone = False
        # execute external command
        execIdMap = {}
        tmpLog.debug("bulkExtCommand : {0}".format(bulkExtCommand))
        for protocol in bulkExtCommand:
            args = []
            for arg in bulkExtCommand[protocol]["command"]["trigger"]["args"]:
                if arg == "{src}":
                    arg = ",".join(bulkExtCommand[protocol]["url"])
                elif arg == "{dst}":
                    arg = ",".join(bulkExtCommand[protocol]["dst"])
                args.append(arg)
            # execute
            try:
                tmpLog.debug("executing external command: " + " ".join(args))
                p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                stdout, stderr = p.communicate()
                return_code = p.returncode
                if stdout is None:
                    stdout = ""
                if stderr is None:
                    stderr = ""
                # get ID of command execution such as transfer ID and batch job ID
                executionID = None
                if return_code == 0 and "check" in bulkExtCommand[protocol]["command"]:
                    executionID = [s for s in stdout.split("\n") if s][-1]
                    dst = ",".join(bulkExtCommand[protocol]["dst"])
                    executionID = "{0}:{1}:{2}".format(protocol, executionID, dst)
                    tmpLog.debug("executionID - {0}".format(executionID))
                    execIdMap[executionID] = {"lfns": bulkExtCommand[protocol]["lfn"], "groupStatus": "active"}
                stdout = stdout.replace("\n", " ")
                stderr = stderr.replace("\n", " ")
                tmpLog.debug("stdout: {0}".format(stdout))
                tmpLog.debug("stderr: {0}".format(stderr))
                if executionID is not None:
                    tmpLog.debug("execution ID: {0}".format(executionID))
            except Exception:
                core_utils.dump_error_message(tmpLog)
                allDone = False
        # keep execution ID to check later
        tmpLog.debug("execIdMap : {0}".format(execIdMap))
        if execIdMap:
            jobspec.set_groups_to_files(execIdMap)
        # done
        if allDone:
            tmpLog.debug("succeeded")
            return True, ""
        else:
            errMsg = "failed"
            tmpLog.error(errMsg)
            # check attemptNr
            for tmpFileSpec in jobspec.inFiles:
                if tmpFileSpec.attemptNr >= self.maxAttempts:
                    errMsg = "gave up due to max attempts"
                    tmpLog.error(errMsg)
                    return (False, errMsg)
            return None, errMsg

    # check status
    def check_stage_in_status(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="check_stage_in_status")
        tmpLog.debug("start")
        allDone = True
        errMsg = ""
        transferGroups = jobspec.get_groups_of_input_files(skip_ready=True)
        for tmpGroupID in transferGroups:
            if tmpGroupID is None:
                continue
            tmpGroupID_parts = tmpGroupID.split(":", 2)
            tmpLog.debug("transfer group ID : {0} components: {1}".format(tmpGroupID, tmpGroupID_parts))
            protocol, executionID, dst = tmpGroupID.split(":", 2)
            args = []
            for arg in self.externalCommand[protocol]["check"]["args"]:
                if arg == "{id}":
                    arg = executionID
                elif arg == "{dst}":
                    arg = dst
                args.append(arg)
            # execute
            try:
                tmpLog.debug("executing external command: " + " ".join(args))
                p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                stdout, stderr = p.communicate()
                return_code = p.returncode
                if stdout is None:
                    stdout = ""
                if stderr is None:
                    stderr = ""
                stdout = stdout.replace("\n", " ")
                stderr = stderr.replace("\n", " ")
                tmpLog.debug("return_code: {0}".format(return_code))
                tmpLog.debug("stdout: {0}".format(stdout))
                tmpLog.debug("stderr: {0}".format(stderr))
                if return_code != 0:
                    errMsg = "{0} is not ready".format(tmpGroupID)
                    allDone = False
                    break
            except Exception:
                errMsg = core_utils.dump_error_message(tmpLog)
                allDone = False
                break
        if not allDone:
            tmpLog.debug("check_stage_in_status: Return : None errMsg : {0}".format(errMsg))
            return None, errMsg
        tmpLog.debug("check_stage_in_status: Return : True")
        return True, ""

    # resolve input file paths
    def resolve_input_paths(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="resolve_input_paths")
        pathInfo = dict()
        for tmpFileSpec in jobspec.inFiles:
            url = tmpFileSpec.lfn
            accPath = self.make_local_access_path(tmpFileSpec.scope, tmpFileSpec.lfn)
            pathInfo[tmpFileSpec.lfn] = {"path": accPath}
            tmpLog.debug("lfn: {0} scope : {1} accPath : {2} pathInfo : {3}".format(url, tmpFileSpec.scope, accPath, pathInfo))
        jobspec.set_input_file_paths(pathInfo)
        return True, ""

    # make local access path
    def make_local_access_path(self, scope, lfn):
        return mover_utils.construct_file_path(self.localBasePath, scope, lfn)

    # run the command to create the image
    def make_image(self, jobspec, args):
        # make logger
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="make_image")
        tmpLog.debug("start")
        return_code = 1
        try:
            tmpLog.debug("executing " + " ".join(args))
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            stdout, stderr = p.communicate()
            return_code = p.returncode
            if stdout is not None:
                stdout = stdout.replace("\n", " ")
            if stderr is not None:
                stderr = stderr.replace("\n", " ")
            tmpLog.debug("stdout: {0}".format(stdout))
            tmpLog.debug("stderr: [0}".format(stderr))
        except Exception:
            core_utils.dump_error_message(tmpLog)
        tmpLog.debug("end with return code {0}".format(return_code))
        return return_code
