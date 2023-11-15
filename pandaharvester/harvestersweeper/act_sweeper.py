import os
import shutil

from act.atlas.aCTDBPanda import aCTDBPanda
from act.common.aCTConfig import aCTConfigARC
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger("act_sweeper")


# plugin for aCT sweeper
class ACTSweeper(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        self.log = core_utils.make_logger(baseLogger, "aCT sweeper", method_name="__init__")
        try:
            self.actDB = aCTDBPanda(self.log)
        except Exception as e:
            self.log.error(f"Could not connect to aCT database: {str(e)}")
            self.actDB = None

    # kill a worker

    def kill_worker(self, workspec):
        """Mark aCT job as tobekilled.

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, f"workerID={workspec.workerID}", method_name="kill_worker")

        if workspec.batchID is None:
            tmpLog.info(f"workerID={workspec.workerID} has no batch ID so assume was not submitted - skipped")
            return True, ""

        try:
            # Only kill jobs which are still active
            self.actDB.updateJobs(
                f"id={workspec.batchID} AND actpandastatus IN ('sent', 'starting', 'running')", {"actpandastatus": "tobekilled", "pandastatus": None}
            )
        except Exception as e:
            if self.actDB:
                tmpLog.error(f"Failed to cancel job {workspec.batchID} in aCT: {str(e)}")
            return False, str(e)

        tmpLog.info(f"Job {workspec.batchID} cancelled in aCT")
        return True, ""

    # cleanup for a worker

    def sweep_worker(self, workspec):
        """Clean up access point. aCT takes care of archiving its own jobs.

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, f"workerID={workspec.workerID}", method_name="sweep_worker")
        # clean up worker directory
        if os.path.exists(workspec.accessPoint):
            shutil.rmtree(workspec.accessPoint)
            tmpLog.info(f"removed {workspec.accessPoint}")
        else:
            tmpLog.info(f"access point {workspec.accessPoint} already removed.")
        # return
        return True, ""
