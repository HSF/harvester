import os
import tempfile

try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermover import mover_utils

# logger
baseLogger = core_utils.setup_logger("xrdcp_preparator")


# preparator plugin with https://xrootd.slac.stanford.edu/  xrdcp
"""
  -- Example of plugin config
    "preparator": {
        "name": "XrdcpPreparator",
        "module": "pandaharvester.harvesterpreparator.xrdcp_preparator",
        # base path for source xrdcp server
        "srcBasePath": " root://dcgftp.usatlas.bnl.gov:1096//pnfs/usatlas.bnl.gov/BNLT0D1/rucio",
        # base path for local access to the copied files
        "localBasePath": "/hpcgpfs01/scratch/benjamin/harvester/rucio-data-area",
        # max number of attempts
        "maxAttempts": 3,
        # check paths under localBasePath.
        "checkLocalPath": true,
        # options for xrdcp
        "xrdcpOpts": "--retry 3 --cksum adler32 --debug 1"
    }
"""


class XrdcpPreparator(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.xrdcpOpts = None
        self.maxAttempts = 3
        self.timeout = None
        self.checkLocalPath = True
        PluginBase.__init__(self, **kwarg)

    # trigger preparation
    def trigger_preparation(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="trigger_preparation")
        tmpLog.debug("start")
        # get the environment
        harvester_env = os.environ.copy()
        # tmpLog.debug('Harvester environment : {}'.format(harvester_env))
        # loop over all inputs
        inFileInfo = jobspec.get_input_file_attributes()
        xrdcpInput = None
        allfiles_transfered = True
        overall_errMsg = ""
        for tmpFileSpec in jobspec.inFiles:
            # construct source and destination paths
            srcPath = mover_utils.construct_file_path(self.srcBasePath, inFileInfo[tmpFileSpec.lfn]["scope"], tmpFileSpec.lfn)
            # local path
            localPath = mover_utils.construct_file_path(self.localBasePath, inFileInfo[tmpFileSpec.lfn]["scope"], tmpFileSpec.lfn)
            if self.checkLocalPath:
                # check if already exits
                if os.path.exists(localPath):
                    # calculate checksum
                    checksum = core_utils.calc_adler32(localPath)
                    checksum = "ad:{0}".format(checksum)
                    if checksum == inFileInfo[tmpFileSpec.lfn]["checksum"]:
                        continue
                # make directories if needed
                if not os.path.isdir(os.path.dirname(localPath)):
                    os.makedirs(os.path.dirname(localPath))
                    tmpLog.debug("Make directory - {0}".format(os.path.dirname(localPath)))
            # collect list of input files
            if xrdcpInput is None:
                xrdcpInput = [srcPath]
            else:
                xrdcpInput.append[srcPath]
            # transfer using xrdcp one file at a time
            tmpLog.debug("execute xrdcp")
            args = ["xrdcp", "--nopbar", "--force"]
            args_files = [srcPath, localPath]
            if self.xrdcpOpts is not None:
                args += self.xrdcpOpts.split()
            args += args_files
            tmpFileSpec.attemptNr += 1
            try:
                xrdcp_cmd = " ".join(args)
                tmpLog.debug("execute: {0}".format(xrdcp_cmd))
                p = subprocess.Popen(xrdcp_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=harvester_env, shell=True)
                try:
                    stdout, stderr = p.communicate(timeout=self.timeout)
                except subprocess.TimeoutExpired:
                    p.kill()
                    stdout, stderr = p.communicate()
                    tmpLog.warning("command timeout")
                return_code = p.returncode
                if stdout is not None:
                    if not isinstance(stdout, str):
                        stdout = stdout.decode()
                    stdout = stdout.replace("\n", " ")
                if stderr is not None:
                    if not isinstance(stderr, str):
                        stderr = stderr.decode()
                    stderr = stderr.replace("\n", " ")
                tmpLog.debug("stdout: %s" % stdout)
                tmpLog.debug("stderr: %s" % stderr)
            except Exception:
                core_utils.dump_error_message(tmpLog)
                return_code = 1
            if return_code != 0:
                overall_errMsg += "file - {0} did not transfer error code {1} ".format(localPath, return_code)
                allfiles_transfered = False
                errMsg = "failed with {0}".format(return_code)
                tmpLog.error(errMsg)
                # check attemptNr
                if tmpFileSpec.attemptNr >= self.maxAttempts:
                    errMsg = "gave up due to max attempts"
                    tmpLog.error(errMsg)
                    return (False, errMsg)
        # end loop over input files
        # nothing to transfer
        if xrdcpInput is None:
            tmpLog.debug("done with no transfers")
            return True, ""
        # check if all files were transfered
        if allfiles_transfered:
            return True, ""
        else:
            return None, overall_errMsg

    # check status

    def check_stage_in_status(self, jobspec):
        return True, ""

    # resolve input file paths
    def resolve_input_paths(self, jobspec):
        #  input files
        inFileInfo = jobspec.get_input_file_attributes()
        pathInfo = dict()
        for tmpFileSpec in jobspec.inFiles:
            localPath = mover_utils.construct_file_path(self.localBasePath, inFileInfo[tmpFileSpec.lfn]["scope"], tmpFileSpec.lfn)
            pathInfo[tmpFileSpec.lfn] = {"path": localPath}
        jobspec.set_input_file_paths(pathInfo)
        return True, ""
