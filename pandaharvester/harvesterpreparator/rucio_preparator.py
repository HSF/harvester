import json
import os
import subprocess
import traceback
from future.utils import iteritems

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermover import mover_utils

# logger
baseLogger = core_utils.setup_logger("rucio_preparator")


def get_num_files(logs):
    total_files = None
    total_filtered_files = None
    downloaded_files = 0
    already_downloaded_files = None
    cannot_download_files = None
    for line in logs.split("\n"):
        if "Total files (DID):" in line:
            total_files = int(line.replace("Total files (DID):", "").strip())
        if "Total files (filtered):" in line:
            total_filtered_files = int(line.replace("Total files (filtered):", "").strip())
        if "Downloaded files:" in line:
            downloaded_files = int(line.replace("Downloaded files:", "").strip())
        if "Files already found locally:" in line:
            already_downloaded_files = int(line.replace("Files already found locally:", "").strip())
        if "Files that cannot be downloaded:" in line:
            cannot_download_files = int(line.replace("Files that cannot be downloaded:", "").strip())
    if total_filtered_files:
        total_files = total_filtered_files
    if already_downloaded_files:
        downloaded_files += already_downloaded_files
    return total_files, downloaded_files, cannot_download_files


class RucioPreparator(PluginBase):
    """
    Praparator bring files from remote ATLAS/Rucio storage to local facility.
    """

    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        if not hasattr(self, "rucioEnv"):
            self.rucioEnv = None
        if not hasattr(self, "timeout"):
            self.timeout = 30 * 60

        # Default x509 proxy for a queue
        try:
            self.x509UserProxy
        except AttributeError:
            self.x509UserProxy = os.getenv("X509_USER_PROXY")

        try:
            self.cacheDir
        except AttributeError:
            self.cacheDir = "/tmp"

        try:
            self.defaultDest
        except AttributeError:
            self.defaultDest = None

    # check status
    def check_stage_in_status(self, jobspec):
        return True, ""

    # trigger preparation
    def trigger_preparation(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="trigger_preparation")
        tmpLog.debug("Start. Trigger data transfer for job: {0}".format(jobspec.PandaID))

        try:
            params = json.loads(jobspec.jobParams["jobPars"])
            if "input_datasets" not in params or "input_location" not in params:
                errMsg = "input_datasets or input_location not in job parameters"
                tmpLog.error(errMsg)
                return True, errMsg

            datasets = params["input_datasets"]  # a comma-separated string
            datasets = datasets.split(",")
            base_dir = params["input_location"]  # dir name in EOS

            if not base_dir:
                tmpLog.debug("input_location is not defined. will use harvester defaultDest: %s" % self.defaultDest)
                base_dir = self.defaultDest
                # tmpLog.error("input_location is not defined.")

            total_datasets = len(datasets)
            downloaded_datasets = 0
            final_exit_code = 0

            for dataset in datasets:
                upload_src_dir = os.path.join(self.cacheDir, dataset)
                if self.rucioEnv:
                    command = "%s; export X509_USER_PROXY=%s; rucio download --dir %s %s; gfal-copy -f -r -v %s %s" % (
                        self.rucioEnv,
                        self.x509UserProxy,
                        self.cacheDir,
                        dataset,
                        upload_src_dir,
                        base_dir,
                    )
                else:
                    # command = "rucio download --dir %s %s" % (base_dir, dataset)
                    command = "export X509_USER_PROXY=%s; rucio download --dir %s %s; gfal-copy -f -r -v %s %s" % (
                        self.x509UserProxy,
                        self.cacheDir,
                        dataset,
                        upload_src_dir,
                        base_dir,
                    )
                tmpLog.debug("execute: " + command)
                exit_code = 0
                try:
                    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8", errors="replace")
                    stdout, stderr = p.communicate(timeout=self.timeout)
                    exit_code = p.poll()
                except subprocess.TimeoutExpired:
                    p.kill()
                    stdout, stderr = p.communicate()
                    exit_code = -1
                    tmpLog.warning("command timeout")

                tmpLog.debug("stdout: %s" % stdout)
                tmpLog.debug("stderr: %s" % stderr)

                if exit_code != 0:
                    final_exit_code = exit_code

                total_files, downloaded_files, cannot_download_files = get_num_files(stdout)
                if total_files is None or downloaded_files is None:
                    errMsg = "Failed to download dataset %s: cannot parse total files or downloaded files: stdout: %s, stderr: %s" % (dataset, stdout, stderr)
                    tmpLog.error(errMsg)
                elif total_files > downloaded_files or cannot_download_files:
                    errMsg = "Not all files are downloaded for dataset %s: stdout: %s, stderr: %s" % (dataset, stdout, stderr)
                    tmpLog.error(errMsg)
                else:
                    tmpLog.info("All files are downloaded for dataset %s: stdout: %s, stderr: %s" % (dataset, stdout, stderr))
                    downloaded_datasets += 1
            if final_exit_code == 0 and total_datasets == downloaded_datasets:
                tmpLog.info("All datasets have been downloaded")
                return True, ""
            else:
                errMsg = "Not all datasets have been downloaded"
                tmpLog.error(errMsg)
                return False, errMsg
        except Exception as ex:
            tmpLog.error(ex)
            tmpLog.error(traceback.format_exc())
            return False, str(ex)

    # resolve input file paths
    def resolve_input_paths(self, jobspec):
        # get input base location
        params = json.loads(jobspec.jobParams["jobPars"])
        base_dir = params["input_location"]  # dir name in EOS

        # get input files
        inFiles = jobspec.get_input_file_attributes()
        # set path to each file
        for inLFN, inFile in iteritems(inFiles):
            inFile["path"] = mover_utils.construct_file_path(base_dir, inFile["scope"], inLFN)
        # set
        jobspec.set_input_file_paths(inFiles)
        return True, ""
