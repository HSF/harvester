try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess
import uuid
import os

from pandaharvester.harvestercore import core_utils
from .base_stager import BaseStager

# TODO: retry of failed transfers

# logger
baseLogger = core_utils.setup_logger("rucio_stager_hpc")


# plugin for stage-out with Rucio on an HPC site that must copy output elsewhere
class RucioStagerHPC(BaseStager):
    # constructor
    def __init__(self, **kwarg):
        BaseStager.__init__(self, **kwarg)
        if not hasattr(self, "scopeForTmp"):
            self.scopeForTmp = "panda"
        if not hasattr(self, "pathConvention"):
            self.pathConvention = None
        if not hasattr(self, "objstoreID"):
            self.objstoreID = None
        if not hasattr(self, "maxAttempts"):
            self.maxAttempts = 3
        if not hasattr(self, "objectstore_additions"):
            self.objectstore_additions = None

    # check status
    def check_stage_out_status(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="check_stage_out_status")
        tmpLog.debug("start")
        return (True, "")

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="trigger_stage_out")
        tmpLog.debug("start")
        # loop over all files
        allChecked = True
        ErrMsg = "These files failed to upload : "
        zip_datasetName = "harvester_stage_out.{0}".format(str(uuid.uuid4()))
        fileAttrs = jobspec.get_output_file_attributes()
        for fileSpec in jobspec.outFiles:
            # fileSpec.fileAttributes['transferID'] = None  # synchronius transfer
            # skip already done
            tmpLog.debug("file: %s status: %s" % (fileSpec.lfn, fileSpec.status))
            if fileSpec.status in ["finished", "failed"]:
                continue

            fileSpec.pathConvention = self.pathConvention
            fileSpec.objstoreID = self.objstoreID
            # set destination RSE
            if fileSpec.fileType in ["es_output", "zip_output", "output"]:
                dstRSE = self.dstRSE_Out
            elif fileSpec.fileType == "log":
                dstRSE = self.dstRSE_Log
            else:
                errMsg = "unsupported file type {0}".format(fileSpec.fileType)
                tmpLog.error(errMsg)
                return (False, errMsg)
            # skip if destination is None
            if dstRSE is None:
                continue

            # get/set scope and dataset name
            if fileSpec.fileType == "log":
                if fileSpec.lfn in fileAttrs:
                    scope = fileAttrs[fileSpec.lfn]["scope"]
                    datasetName = fileAttrs[fileSpec.lfn]["dataset"]
                else:
                    lfnWithoutWorkerID = ".".join(fileSpec.lfn.split(".")[:-1])
                    scope = fileAttrs[lfnWithoutWorkerID]["scope"]
                    datasetName = fileAttrs[lfnWithoutWorkerID]["dataset"]
            elif fileSpec.fileType != "zip_output" and fileSpec.lfn in fileAttrs:
                scope = fileAttrs[fileSpec.lfn]["scope"]
                datasetName = fileAttrs[fileSpec.lfn]["dataset"]
            else:
                # use panda scope for zipped files
                scope = self.scopeForTmp
                datasetName = zip_datasetName

            # for now mimic behaviour and code of pilot v2 rucio copy tool (rucio download) change when needed

            executable_prefix = None
            pfn_prefix = None
            if self.objectstore_additions and dstRSE in self.objectstore_additions:
                if "storage_id" in self.objectstore_additions[dstRSE]:
                    fileSpec.objstoreID = self.objectstore_additions[dstRSE]["storage_id"]
                if (
                    "access_key" in self.objectstore_additions[dstRSE]
                    and "secret_key" in self.objectstore_additions[dstRSE]
                    and "is_secure" in self.objectstore_additions[dstRSE]
                ):
                    executable_prefix = "export S3_ACCESS_KEY=%s; export S3_SECRET_KEY=%s; export S3_IS_SECURE=%s" % (
                        self.objectstore_additions[dstRSE]["access_key"],
                        self.objectstore_additions[dstRSE]["secret_key"],
                        self.objectstore_additions[dstRSE]["is_secure"],
                    )
                if "pfn_prefix" in self.objectstore_additions[dstRSE]:
                    pfn_prefix = self.objectstore_additions[dstRSE]["pfn_prefix"]

            executable = ["/usr/bin/env", "rucio", "-v", "upload"]
            executable += ["--no-register"]
            if hasattr(self, "lifetime"):
                executable += ["--lifetime", ("%d" % self.lifetime)]
                if fileSpec.fileAttributes is not None and "guid" in fileSpec.fileAttributes:
                    executable += ["--guid", fileSpec.fileAttributes["guid"]]

            executable += ["--rse", dstRSE]
            executable += ["--scope", scope]
            if pfn_prefix:
                executable += ["--pfn %s" % os.path.join(pfn_prefix, os.path.basename(fileSpec.path))]
            else:
                executable += [("%s:%s" % (scope, datasetName))]
            executable += [("%s" % fileSpec.path)]

            tmpLog.debug("rucio upload command: {0} ".format(executable))
            tmpLog.debug("rucio upload command (for human): %s " % " ".join(executable))

            if executable_prefix:
                cmd = executable_prefix + "; " + " ".join(executable)
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
            else:
                process = subprocess.Popen(executable, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

            stdout, stderr = process.communicate()
            fileSpec.attemptNr += 1
            stdout = stdout.decode() + " attemptNr: %s" % fileSpec.attemptNr
            tmpLog.debug("stdout: %s" % stdout)
            tmpLog.debug("stderr: %s" % stderr)
            if process.returncode == 0:
                fileSpec.status = "finished"
            else:
                # check what failed
                file_exists = False
                rucio_sessions_limit_error = False
                for line in stdout.split("\n"):
                    if "File name in specified scope already exists" in line:
                        file_exists = True
                        break
                    elif "File already exists on RSE" in line:
                        # can skip if file exist on RSE since no register
                        tmpLog.warning("rucio skipped upload and returned stdout: %s" % stdout)
                        file_exists = True
                        break
                    elif "exceeded simultaneous SESSIONS_PER_USER limit" in line:
                        rucio_sessions_limit_error = True
                if file_exists:
                    tmpLog.debug("file exists, marking transfer as finished")
                    fileSpec.status = "finished"
                elif rucio_sessions_limit_error:
                    # do nothing
                    tmpLog.warning("rucio returned error, will retry: stdout: %s" % stdout)
                    # do not change fileSpec.status and Harvester will retry if this function returns False
                    allChecked = False
                    continue
                else:
                    tmpLog.error("rucio upload failed with stdout: %s" % stdout)
                    ErrMsg += '%s failed with rucio error stdout="%s"' % (fileSpec.lfn, stdout)
                    allChecked = False
                    if fileSpec.attemptNr >= self.maxAttempts:
                        tmpLog.error("reached maxattempts: %s, marked it as failed" % self.maxAttempts)
                        fileSpec.status = "failed"

            # force update
            fileSpec.force_update("status")

            tmpLog.debug("file: %s status: %s" % (fileSpec.lfn, fileSpec.status))

        # return
        tmpLog.debug("done")
        if allChecked:
            return True, ""
        else:
            return False, ErrMsg

    # zip output files
    def zip_output(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        return self.simple_zip_output(jobspec, tmpLog)
