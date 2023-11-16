import os
import os.path
import threading
import time

from future.utils import iteritems
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermover import mover_utils
from pilot.api import data
from pilot.info import infosys
from pilot.info.filespec import FileSpec as PilotFileSpec

# logger
baseLogger = core_utils.setup_logger("pilotmover_mt_preparator_kari")


# plugin for preparator based on Pilot2.0 Data API, MultipleThreads
# Pilot 2.0 should be deployed as library
# default self.basePath came from preparator section of configuration file

# Modified by FaHui Lin to be compatible with current pilot 2 code


class PilotmoverMTPreparator(PluginBase):
    """
    Praparator bring files from remote ATLAS/Rucio storage to local facility.
    """

    # constructor
    def __init__(self, **kwarg):
        self.n_threads = 3
        PluginBase.__init__(self, **kwarg)
        if self.n_threads < 1:
            self.n_threads = 1

    # check status
    def check_stage_in_status(self, jobspec):
        return True, ""

    def stage_in(self, tmpLog, jobspec, files):
        tmpLog.debug(f"To stagein files[] {files}")
        # get infosys
        # infoservice = InfoService()
        # infoservice.init(jobspec.computingSite, infosys.confinfo, infosys.extinfo)
        infosys.init(jobspec.computingSite, infosys.confinfo, infosys.extinfo)
        # always disable remote/direct io
        infosys.queuedata.direct_access_lan = False
        infosys.queuedata.direct_access_wan = False
        # set data client, always use rucio
        data_client = data.StageInClient(infosys, acopytools={"default": ["rucio"]}, default_copytools="rucio")
        allChecked = True
        ErrMsg = "These files failed to download : "
        # change directory to basPath for input to pass pilot check_availablespace
        os.chdir(self.basePath)
        # transfer
        if len(files) > 0:
            try:
                result = data_client.transfer(files)
            except Exception as e:
                tmpLog.error(f"error when stage_in: {e.__class__.__name__} ; {e}")
                raise
            else:
                tmpLog.debug(f"pilot.api data.StageInClient.transfer(files) result: {result}")

                # loop over each file check result all must be true for entire result to be true
                if result:
                    for answer in result:
                        if answer.status_code != 0:
                            allChecked = False
                            ErrMsg = ErrMsg + f" {answer.lfn} "
                else:
                    tmpLog.info(f"Looks like all files already inplace: {files}")
        # return
        tmpLog.debug("stop thread")
        if allChecked:
            return True, ""
        else:
            return False, ErrMsg

    # trigger preparation
    def trigger_preparation(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, f"PandaID={jobspec.PandaID}", method_name="trigger_preparation")
        tmpLog.debug("start")

        # check that jobspec.computingSite is defined
        if jobspec.computingSite is None:
            # not found
            tmpLog.error("jobspec.computingSite is not defined")
            return False, "jobspec.computingSite is not defined"
        else:
            tmpLog.debug(f"jobspec.computingSite : {jobspec.computingSite}")
        # get input files
        files = []
        inFiles = jobspec.get_input_file_attributes(skip_ready=True)
        # set path to each file
        for inLFN, inFile in iteritems(inFiles):
            inFile["path"] = mover_utils.construct_file_path(self.basePath, inFile["scope"], inLFN)
            tmpLog.debug(f"To check file: {inFile}")
            if os.path.exists(inFile["path"]):
                # checksum = core_utils.calc_adler32(inFile['path'])
                # checksum = 'ad:%s' % checksum
                # tmpLog.debug('checksum for file %s is %s' % (inFile['path'], checksum))
                # if 'checksum' in inFile and inFile['checksum'] and inFile['checksum'] == checksum:
                #     tmpLog.debug('File %s already exists at %s' % (inLFN, inFile['path']))
                #     continue

                # lazy but unsafe check to be faster...
                file_size = os.stat(inFile["path"]).st_size
                tmpLog.debug(f"file size for file {inFile['path']} is {file_size}")
                if "fsize" in inFile and inFile["fsize"] and inFile["fsize"] == file_size:
                    tmpLog.debug(f"File {inLFN} already exists at {inFile['path']}")
                    continue
            dstpath = os.path.dirname(inFile["path"])
            # check if path exists if not create it.
            if not os.access(dstpath, os.F_OK):
                os.makedirs(dstpath)
            file_data = {
                "scope": inFile["scope"],
                "dataset": inFile.get("dataset"),
                "lfn": inLFN,
                "ddmendpoint": inFile.get("endpoint"),
                "guid": inFile.get("guid"),
                "workdir": dstpath,
            }
            pilotfilespec = PilotFileSpec(type="input", **file_data)
            files.append(pilotfilespec)
        # tmpLog.debug('files[] {0}'.format(files))
        tmpLog.debug("path set")

        allChecked = True
        ErrMsg = "These files failed to download : "
        if files:
            threads = []
            n_files_per_thread = (len(files) + self.n_threads - 1) // self.n_threads
            tmpLog.debug(f"num files per thread: {n_files_per_thread}")
            for i in range(0, len(files), n_files_per_thread):
                sub_files = files[i : i + n_files_per_thread]
                thread = threading.Thread(
                    target=self.stage_in,
                    kwargs={
                        "tmpLog": tmpLog,
                        "jobspec": jobspec,
                        "files": sub_files,
                    },
                )
                threads.append(thread)
            [t.start() for t in threads]
            while len(threads) > 0:
                time.sleep(1)
                threads = [t for t in threads if t and t.isAlive()]

            tmpLog.info(f"Checking all files: {files}")
            for file in files:
                if file.status_code != 0:
                    allChecked = False
                    ErrMsg = ErrMsg + f" {file.lfn} "
            for inLFN, inFile in iteritems(inFiles):
                if not os.path.isfile(inFile["path"]):
                    allChecked = False
                    ErrMsg = ErrMsg + f" {file.lfn} "
        # return
        tmpLog.debug("stop")
        if allChecked:
            tmpLog.info("Looks like all files are successfully downloaded.")
            return True, ""
        else:
            # keep retrying
            return None, ErrMsg
            # return False, ErrMsg

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
