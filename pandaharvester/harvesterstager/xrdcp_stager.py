import os
import tempfile
import gc

try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

from pandaharvester.harvestermover import mover_utils
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterstager.base_stager import BaseStager

import uuid

# logger
_logger = core_utils.setup_logger("xrdcp_stager")

# stager plugin with https://xrootd.slac.stanford.edu/  xrdcp
"""
  -- Example of plugin config
    "stager": {
        "name": "XrdcpStager",
        "module": "pandaharvester.harvesterstager.xrdcp_stager",
        # base path for destinattion xrdcp server
        "dstBasePath": " root://dcgftp.usatlas.bnl.gov:1096//pnfs/usatlas.bnl.gov/BNLT0D1/rucio",
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


# dummy plugin for stager
class XrdcpStager(BaseStager):
    # constructor
    def __init__(self, **kwarg):
        BaseStager.__init__(self, **kwarg)
        if not hasattr(self, "xrdcpOpts"):
            self.xrdcpOpts = None
        if not hasattr(self, "maxAttempts"):
            self.maxAttempts = 3
        if not hasattr(self, "timeout"):
            self.timeout = None
        if not hasattr(self, "checkLocalPath"):
            self.checkLocalPath = True

    # check status
    def check_stage_out_status(self, jobspec):
        """Check the status of stage-out procedure. If staging-out is done synchronously in trigger_stage_out
        this method should always return True.
        Output files are available through jobspec.get_outfile_specs(skip_done=False) which gives
        a list of FileSpecs not yet done.
        FileSpec.attemptNr shows how many times the transfer was checked for the file.
        If the file was successfully transferred, status should be set to 'finished'.
        Or 'failed', if the file failed to be transferred. Once files are set to 'finished' or 'failed',
        jobspec.get_outfile_specs(skip_done=False) ignores them.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True: transfer success, False: fatal transfer failure,
                 None: on-going or temporary failure) and error dialog
        :rtype: (bool, string)
        """
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            fileSpec.status = "finished"
        return True, ""

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        """Trigger the stage-out procedure for the job.
        Output files are available through jobspec.get_outfile_specs(skip_done=False) which gives
        a list of FileSpecs not yet done.
        FileSpec.attemptNr shows how many times transfer was tried for the file so far.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True: success, False: fatal failure, None: temporary failure)
                 and error dialog
        :rtype: (bool, string)
        """

        # let gc clean up memory
        gc.collect()

        # make logger
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="trigger_stage_out")
        tmpLog.debug("start")
        # get the environment
        harvester_env = os.environ.copy()
        # tmpLog.debug('Harvester environment : {}'.format(harvester_env))

        xrdcpOutput = None
        allfiles_transfered = True
        overall_errMsg = ""
        fileAttrs = jobspec.get_output_file_attributes()
        # loop over all output files
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            # fileSpec.objstoreID = 123
            # fileSpec.fileAttributes['guid']
            # construct source and destination paths
            dstPath = mover_utils.construct_file_path(self.dstBasePath, fileAttrs[fileSpec.lfn]["scope"], fileSpec.lfn)
            # local path
            localPath = mover_utils.construct_file_path(self.localBasePath, fileAttrs[fileSpec.lfn]["scope"], fileSpec.lfn)
            tmpLog.debug("fileSpec.path - {0} fileSpec.lfn = {1}".format(fileSpec.path, fileSpec.lfn))
            localPath = fileSpec.path
            if self.checkLocalPath:
                # check if already exits
                if os.path.exists(localPath):
                    # calculate checksum
                    checksum = core_utils.calc_adler32(localPath)
                    checksum = "ad:{0}".format(checksum)
                    if checksum == fileAttrs[fileSpec.lfn]["checksum"]:
                        continue
            # collect list of output files
            if xrdcpOutput is None:
                xrdcpOutput = [dstPath]
            else:
                if dstPath not in xrdcpOutput:
                    xrdcpOutput.append(dstPath)
            # transfer using xrdcp one file at a time
            tmpLog.debug("execute xrdcp")
            args = ["xrdcp", "--nopbar", "--force"]
            args_files = [localPath, dstPath]
            if self.xrdcpOpts is not None:
                args += self.xrdcpOpts.split()
            args += args_files
            fileSpec.attemptNr += 1
            try:
                xrdcp_cmd = " ".join(args)
                tmpLog.debug("execute: {0}".format(xrdcp_cmd))
                process = subprocess.Popen(xrdcp_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=harvester_env, shell=True)
                try:
                    stdout, stderr = process.communicate(timeout=self.timeout)
                except subprocess.TimeoutExpired:
                    process.kill()
                    stdout, stderr = process.communicate()
                    tmpLog.warning("command timeout")
                return_code = process.returncode
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
            if return_code == 0:
                fileSpec.status = "finished"
            else:
                overall_errMsg += "file - {0} did not transfer error code {1} ".format(localPath, return_code)
                allfiles_transfered = False
                errMsg = "failed with {0}".format(return_code)
                tmpLog.error(errMsg)
                # check attemptNr
                if fileSpec.attemptNr >= self.maxAttempts:
                    tmpLog.error("reached maxattempts: {0}, marked it as failed".format(self.maxAttempts))
                    fileSpec.status = "failed"

            # force update
            fileSpec.force_update("status")
            tmpLog.debug("file: {0} status: {1}".format(fileSpec.lfn, fileSpec.status))
            del process, stdout, stderr

        # end loop over output files

        # nothing to transfer
        if xrdcpOutput is None:
            tmpLog.debug("done with no transfers")
            return True, ""
        # check if all files were transfered
        tmpLog.debug("done")
        if allfiles_transfered:
            return True, ""
        else:
            return None, overall_errMsg

    # zip output files

    def zip_output(self, jobspec):
        """OBSOLETE : zip functions should be implemented in zipper plugins.
        Zip output files. This method loops over jobspec.outFiles, which is a list of zip file's FileSpecs,
        to make a zip file for each zip file's FileSpec. FileSpec.associatedFiles is a list of FileSpecs of
        associated files to be zipped. The path of each associated file is available in associated
        file's FileSpec.path. Once zip files are made, their FileSpec.path, FileSpec.fsize and
        FileSpec.chksum need to be set.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        # make logger
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        return self.simple_zip_output(jobspec, tmpLog)

    # asynchronous zip output
    def async_zip_output(self, jobspec):
        """OBSOLETE : zip functions should be implemented in zipper plugins.
        Zip output files asynchronously. This method is followed by post_zip_output(),
        which is typically useful to trigger an asynchronous zipping mechanism such as batch job.
        This method loops over jobspec.outFiles, which is a list of zip file's FileSpecs, to make
        a zip file for each zip file's FileSpec. FileSpec.associatedFiles is a list of FileSpecs
        of associated files to be zipped. The path of each associated file is available in associated
        file's FileSpec.path.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        # make logger
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        # set some ID which can be used for lookup in post_zip_output()
        groupID = str(uuid.uuid4())
        lfns = []
        for fileSpec in jobspec.outFiles:
            lfns.append(fileSpec.lfn)
        jobspec.set_groups_to_files({groupID: {"lfns": lfns, "groupStatus": "zipping"}})
        return True, ""

    # post zipping
    def post_zip_output(self, jobspec):
        """OBSOLETE : zip functions should be implemented in zipper plugins.
        This method is executed after async_zip_output(), to do post-processing for zipping.
        Once zip files are made, this method needs to look over jobspec.outFiles to set their
        FileSpec.path, FileSpec.fsize, and FileSpec.chksum.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        # make logger
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        # get groups for lookup
        groups = jobspec.get_groups_of_output_files()
        # do something with groupIDs
        pass
        # update file attributes
        for fileSpec in jobspec.outFiles:
            fileSpec.path = "/path/to/zip"
            fileSpec.fsize = 12345
            fileSpec.chksum = "66bb0985"
        return True, ""
