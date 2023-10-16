import uuid
import datetime
import threading
from future.utils import iteritems

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterconfig import harvester_config

# dummy transfer identifier
dummy_transfer_id_base = "dummy_id_for_in"

# logger
_logger = core_utils.setup_logger("dummy_bulk_preparator")

# lock to get a unique ID
uLock = threading.Lock()

# number to get a unique ID
uID = 0


# dummy plugin for preparator with bulk transfers. For JobSpec and DBInterface methods, see
# https://github.com/PanDAWMS/panda-harvester/wiki/Utilities#file-grouping-for-file-transfers
class DummyBulkPreparator(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        with uLock:
            global uID
            self.dummy_transfer_id = "{0}_{1}".format(dummy_transfer_id_base, uID)
            uID += 1
            uID %= harvester_config.preparator.nThreads

    # trigger preparation
    def trigger_preparation(self, jobspec):
        # set the dummy transfer ID which will be replaced with a real ID in check_stage_in_status()
        inFiles = jobspec.get_input_file_attributes(skip_ready=True)
        lfns = inFiles.keys()
        for inLFN in inFiles.keys():
            lfns.append(inLFN)
        jobspec.set_groups_to_files({self.dummy_transfer_id: {"lfns": lfns, "groupStatus": "pending"}})
        return True, ""

    # check status
    def check_stage_in_status(self, jobspec):
        # get groups of input files except ones already in ready state
        groups = jobspec.get_groups_of_input_files(skip_ready=True)
        # lock if the dummy transfer ID is used to avoid submitting duplicated transfer requests
        if self.dummy_transfer_id in groups:
            # lock for 120 sec
            locked = self.dbInterface.get_object_lock(self.dummy_transfer_id, lock_interval=120)
            if not locked:
                # escape since locked by another thread
                msgStr = "escape since locked by another thread"
                return None, msgStr
            # refresh group information since that could have been updated by another thread before getting the lock
            self.dbInterface.refresh_file_group_info(jobspec)
            # get transfer groups again with refreshed info
            groups = jobspec.get_groups_of_input_files(skip_ready=True)
            # the dummy transfer ID is still there
            if self.dummy_transfer_id in groups:
                groupUpdateTime = groups[self.dummy_transfer_id]["groupUpdateTime"]
                # get files with the dummy transfer ID across jobs
                fileSpecs = self.dbInterface.get_files_with_group_id(self.dummy_transfer_id)
                # submit transfer if there are more than 10 files or the group was made before more than 10 min.
                # those thresholds may be config params.
                if len(fileSpecs) >= 10 or groupUpdateTime < datetime.datetime.utcnow() - datetime.timedelta(minutes=10):
                    # submit transfer and get a real transfer ID
                    # ...
                    transferID = str(uuid.uuid4())
                    # set the real transfer ID
                    self.dbInterface.set_file_group(fileSpecs, transferID, "running")
                    msgStr = "real transfer submitted with ID={0}".format(transferID)
                else:
                    msgStr = "wait until enough files are pooled with {0}".format(self.dummy_transfer_id)
                # release the lock
                self.dbInterface.release_object_lock(self.dummy_transfer_id)
                # return None to retry later
                return None, msgStr
            # release the lock
            self.dbInterface.release_object_lock(self.dummy_transfer_id)
        # check transfer with real transfer IDs
        # ...
        # then update transfer status if successful
        for transferID, transferInfo in iteritems(groups):
            jobspec.update_group_status_in_files(transferID, "done")
        return True, ""

    # resolve input file paths
    def resolve_input_paths(self, jobspec):
        """Set input file paths to jobspec.get_input_file_attributes[LFN]['path'] for the job.
        New input file attributes need to be set to jobspec using jobspec.set_input_file_paths()
        after setting the file paths.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        # get input files
        inFiles = jobspec.get_input_file_attributes()
        # set path to each file
        for inLFN, inFile in iteritems(inFiles):
            inFile["path"] = "dummypath/{0}".format(inLFN)
        # set
        jobspec.set_input_file_paths(inFiles)
        return True, ""
