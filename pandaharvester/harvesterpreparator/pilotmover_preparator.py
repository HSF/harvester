import os
import os.path

from pilot.api import data

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermover import mover_utils

# logger
baseLogger = core_utils.setup_logger("pilotmover_preparator")


# plugin for preparator based on Pilot2.0 Data API
# Pilot 2.0 should be deployed as library
# default self.basePath came from preparator section of configuration file


class PilotmoverPreparator(PluginBase):
    """
    Praparator bring files from remote ATLAS/Rucio storage to local facility.
    """

    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check status
    def check_stage_in_status(self, jobspec):
        return True, ""

    # trigger preparation
    def trigger_preparation(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, f"PandaID={jobspec.PandaID}", method_name="trigger_preparation")
        tmpLog.debug(f"Start. Trigger data transfer for job: {jobspec.PandaID}")

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
        tmpLog.info("Prepare files to download (construct path and verifiy existing files)")
        for inLFN, inFile in inFiles.items():
            inFile["path"] = mover_utils.construct_file_path(self.basePath, inFile["scope"], inLFN)
            # check if file exist. Skip alrady downoladed files
            if os.path.exists(inFile["path"]):
                checksum = core_utils.calc_adler32(inFile["path"])
                checksum = f"ad:{checksum}"
                # tmpLog.debug('checksum for file %s is %s' % (inFile['path'], checksum))
                if "checksum" in inFile and inFile["checksum"] and inFile["checksum"] == checksum:
                    # tmpLog.debug('File %s already exists at %s' % (inLFN, inFile['path']))
                    continue
            dstpath = os.path.dirname(inFile["path"])
            # check if path exists if not create it.
            if not os.access(dstpath, os.F_OK):
                os.makedirs(dstpath)
            files.append({"scope": inFile["scope"], "name": inLFN, "destination": dstpath})
        tmpLog.info(f"Number of files to dowload: {len(files)} for job: {jobspec.PandaID}")
        # tmpLog.debug('files {0}'.format(files))
        tmpLog.info("Setup of Pilot2 API client")
        data_client = data.StageInClient(site=jobspec.computingSite)
        allChecked = True
        ErrMsg = "These files failed to download : "
        if len(files) > 0:
            tmpLog.info(f"Going to transfer {len(files)} of files with one call to Pilot2 Data API")
            try:
                result = data_client.transfer(files)
            except Exception as e:
                tmpLog.error(f"Pilot2 Data API rise error: {e.message}")
            tmpLog.debug(f"data_client.transfer(files) result:\n{result}")
            tmpLog.info("Transfer call to Pilot2 Data API completed")
            # loop over each file check result all must be true for entire result to be true
            if result:
                for answer in result:
                    if answer["errno"] != 0:
                        allChecked = False
                        ErrMsg = ErrMsg + f" {answer['name']} "
            else:
                tmpLog.info(f"Looks like all files in place. Number of files: {len(files)}")
        # return
        tmpLog.debug(f"Finished data transfer with {len(files)} files for job {jobspec.PandaID}")
        if allChecked:
            return True, ""
        else:
            return False, ErrMsg

    # resolve input file paths
    def resolve_input_paths(self, jobspec):
        # get input files
        inFiles = jobspec.get_input_file_attributes()
        # set path to each file
        for inLFN, inFile in inFiles.items():
            inFile["path"] = mover_utils.construct_file_path(self.basePath, inFile["scope"], inLFN)
        # set
        jobspec.set_input_file_paths(inFiles)
        return True, ""
