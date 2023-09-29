import os
import re
import tempfile

try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

from pandaharvester.harvestercore import core_utils
from .base_stager import BaseStager
from pandaharvester.harvestermover import mover_utils

# logger
baseLogger = core_utils.setup_logger("gridftp_stager")


# stager plugin with GridFTP
"""
  -- example of plugin config
  "stager":{
        "name":"GridFtpStager",
        "module":"pandaharvester.harvesterstager.gridftp_stager",
        "objstoreID":117,
        # base path for local access to the files to be copied
        "srcOldBasePath":"/tmp/workdirs",
        # base path for access through source GridFTP server to the files to be copied
        "srcNewBasePath":"gsiftp://dcdum02.aglt2.org/pnfs/aglt2.org",
        # base path for destination GridFTP server
        "dstBasePath":"gsiftp://dcgftp.usatlas.bnl.gov:2811/pnfs/usatlas.bnl.gov/atlasscratchdisk/rucio",
        # max number of attempts
        "maxAttempts": 3,
        # options for globus-url-copy
        "gulOpts":"-verify-checksum -v"
        # A list of base paths of intermediate locations: [ p1, p2, p3, ..., pn ] . The transfers will be done in serial: srcPath -> p1, p1 ->p2, ..., pn -> dstPath
        # where p1 can be a path (e.g. "gsiftp://..."), or a list of incoming and outgoing paths (if different, say due to FS mount) of the same files (e.g. ["file:///data/abc", "gsiftp://dtn.server//data/abc"])
        # If ommited, direct transfer srcPath -> dstPath occurs
        "intermediateBasePaths":[ ["file:///nfs/at3/scratch/at3sgm001/", "gsiftp://some.dtn.server//nfs/at3/scratch/at3sgm001/"], "gsiftp://another.dtn.server//scratch/data/" ]
    }
"""


class GridFtpStager(BaseStager):
    # constructor
    def __init__(self, **kwarg):
        self.scopeForTmp = "transient"
        self.pathConvention = 1000
        self.objstoreID = None
        self.gulOpts = None
        self.maxAttempts = 3
        self.timeout = None
        self.intermediateBasePaths = None
        BaseStager.__init__(self, **kwarg)

    # check status
    def check_stage_out_status(self, jobspec):
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            fileSpec.status = "finished"
            fileSpec.pathConvention = self.pathConvention
            fileSpec.objstoreID = self.objstoreID
        return True, ""

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="trigger_stage_out")
        tmpLog.debug("start")
        # loop over all files
        gucInput = None
        is_multistep = isinstance(self.intermediateBasePaths, list) and len(self.intermediateBasePaths) > 0
        guc_inputs_list = [None] * (len(self.intermediateBasePaths) + 1) if is_multistep else []
        for fileSpec in jobspec.outFiles:
            # skip if already done
            if fileSpec.status in ["finished", "failed"]:
                continue
            # scope
            if fileSpec.fileType in ["es_output", "zip_output"]:
                scope = self.scopeForTmp
            else:
                scope = fileSpec.fileAttributes.get("scope")
                if scope is None:
                    scope = fileSpec.scope
            # construct source and destination paths
            srcPath = re.sub(self.srcOldBasePath, self.srcNewBasePath, fileSpec.path)
            dstPath = mover_utils.construct_file_path(self.dstBasePath, scope, fileSpec.lfn)
            # make tempfiles of paths to transfer
            if is_multistep:
                # multi-step transfer
                for ibp_i in range(len(self.intermediateBasePaths) + 1):
                    base_paths_old = self.intermediateBasePaths[ibp_i - 1] if ibp_i > 0 else ""
                    base_paths_new = self.intermediateBasePaths[ibp_i] if ibp_i < len(self.intermediateBasePaths) else ""
                    src_base = base_paths_old[1] if isinstance(base_paths_old, list) else base_paths_old
                    dst_base = base_paths_new[0] if isinstance(base_paths_new, list) else base_paths_new
                    # construct temporary source and destination paths
                    tmp_src_path = re.sub(self.srcNewBasePath, src_base, srcPath)
                    tmp_dest_path = re.sub(self.srcNewBasePath, dst_base, srcPath)
                    # make input for globus-url-copy
                    if guc_inputs_list[ibp_i] is None:
                        guc_inputs_list[ibp_i] = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix="_guc_out_{0}.tmp".format(ibp_i))
                    guc_input = guc_inputs_list[ibp_i]
                    if ibp_i == 0:
                        guc_input.write("{0} {1}\n".format(srcPath, tmp_dest_path))
                        tmpLog.debug("step {0}: {1} {2}".format(ibp_i + 1, srcPath, tmp_dest_path))
                    elif ibp_i == len(self.intermediateBasePaths):
                        guc_input.write("{0} {1}\n".format(tmp_src_path, dstPath))
                        tmpLog.debug("step {0}: {1} {2}".format(ibp_i + 1, tmp_src_path, dstPath))
                    else:
                        guc_input.write("{0} {1}\n".format(tmp_src_path, tmp_dest_path))
                        tmpLog.debug("step {0}: {1} {2}".format(ibp_i + 1, tmp_src_path, tmp_dest_path))
            else:
                # single-step transfer
                # make input for globus-url-copy
                if gucInput is None:
                    gucInput = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix="_guc_out.tmp")
                gucInput.write("{0} {1}\n".format(srcPath, dstPath))
            fileSpec.attemptNr += 1
        # nothing to transfer
        if is_multistep:
            for guc_input in guc_inputs_list:
                if guc_input is None:
                    tmpLog.debug("done with no transfers (multistep)")
                    return True, ""
        else:
            if gucInput is None:
                tmpLog.debug("done with no transfers")
                return True, ""
        # transfer
        if is_multistep:
            [guc_input.close() for guc_input in guc_inputs_list]
            tmpLog.debug("start multistep transfer")
            guc_input_i = 1
            for guc_input in guc_inputs_list:
                args = ["globus-url-copy", "-f", guc_input.name, "-cd"]
                if self.gulOpts is not None:
                    args += self.gulOpts.split()
                try:
                    tmpLog.debug("execute: " + " ".join(args))
                    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
                os.remove(guc_input.name)
                if return_code == 0:
                    tmpLog.debug("step {0} succeeded".format(guc_input_i))
                    guc_input_i += 1
                else:
                    errMsg = "step {0} failed with {1}".format(guc_input_i, return_code)
                    tmpLog.error(errMsg)
                    # check attemptNr
                    for fileSpec in jobspec.inFiles:
                        if fileSpec.attemptNr >= self.maxAttempts:
                            errMsg = "gave up due to max attempts"
                            tmpLog.error(errMsg)
                            return (False, errMsg)
                    return None, errMsg
            tmpLog.debug("multistep transfer ({0} steps) succeeded".format(len(guc_inputs_list)))
            return True, ""
        else:
            gucInput.close()
            args = ["globus-url-copy", "-f", gucInput.name, "-cd"]
            if self.gulOpts is not None:
                args += self.gulOpts.split()
            try:
                tmpLog.debug("execute: " + " ".join(args))
                p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
            os.remove(gucInput.name)
            if return_code == 0:
                tmpLog.debug("succeeded")
                return True, ""
            else:
                errMsg = "failed with {0}".format(return_code)
                tmpLog.error(errMsg)
                # check attemptNr
                for fileSpec in jobspec.inFiles:
                    if fileSpec.attemptNr >= self.maxAttempts:
                        errMsg = "gave up due to max attempts"
                        tmpLog.error(errMsg)
                        return (False, errMsg)
                return None, errMsg
