import datetime
import uuid

from pandaharvester.harvestercore import core_utils
from .base_stager import BaseStager

# dummy transfer identifier
dummy_transfer_id = "dummy_id_for_out"

# logger
baseLogger = core_utils.setup_logger("dummy_bulk_stager")


# dummy plugin for stager with bulk transfers. For JobSpec and DBInterface methods, see
# https://github.com/PanDAWMS/panda-harvester/wiki/Utilities#file-grouping-for-file-transfers
class DummyBulkStager(BaseStager):
    # constructor
    def __init__(self, **kwarg):
        BaseStager.__init__(self, **kwarg)

    # check status
    def check_stage_out_status(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="check_stage_out_status")
        tmpLog.debug("start")
        # get transfer groups
        groups = jobspec.get_groups_of_output_files()
        # lock if the dummy transfer ID is used to avoid submitting duplicated transfer requests
        if dummy_transfer_id in groups:
            # lock for 120 sec
            locked = self.dbInterface.get_object_lock(dummy_transfer_id, lock_interval=120)
            if not locked:
                # escape since locked by another thread
                msgStr = "escape since locked by another thread"
                tmpLog.debug(msgStr)
                return None, msgStr
            # refresh group information since that could have been updated by another thread before getting the lock
            self.dbInterface.refresh_file_group_info(jobspec)
            # get transfer groups again with refreshed info
            groups = jobspec.get_groups_of_output_files()
            # the dummy transfer ID is still there
            if dummy_transfer_id in groups:
                groupUpdateTime = groups[dummy_transfer_id]["groupUpdateTime"]
                # get files with the dummy transfer ID across jobs
                fileSpecs = self.dbInterface.get_files_with_group_id(dummy_transfer_id)
                # submit transfer if there are more than 10 files or the group was made before more than 10 min.
                # those thresholds may be config params.
                if len(fileSpecs) >= 10 or groupUpdateTime < datetime.datetime.utcnow() - datetime.timedelta(minutes=10):
                    # submit transfer and get a real transfer ID
                    # ...
                    transferID = str(uuid.uuid4())
                    # set the real transfer ID
                    self.dbInterface.set_file_group(fileSpecs, transferID, "running")
                    msgStr = "submitted transfer with ID={0}".format(transferID)
                    tmpLog.debug(msgStr)
                else:
                    msgStr = "wait until enough files are pooled"
                    tmpLog.debug(msgStr)
                # release the lock
                self.dbInterface.release_object_lock(dummy_transfer_id)
                # return None to retry later
                return None, msgStr
            # release the lock
            self.dbInterface.release_object_lock(dummy_transfer_id)
        # check transfer with real transfer IDs
        # ...
        # then set file status if successful
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            fileSpec.status = "finished"
        tmpLog.debug("all finished")
        return True, ""

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        # set the dummy transfer ID which will be replaced with a real ID in check_stage_out_status()
        lfns = []
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            lfns.append(fileSpec.lfn)
        jobspec.set_groups_to_files({dummy_transfer_id: {"lfns": lfns, "groupStatus": "pending"}})
        return True, ""

    # zip output files
    def zip_output(self, jobspec):
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        return self.simple_zip_output(jobspec, tmpLog)
