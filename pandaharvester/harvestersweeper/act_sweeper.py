import os
import shutil

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

from act.common.aCTConfig import aCTConfigARC
from act.atlas.aCTDBPanda import aCTDBPanda

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
            self.log.error("Could not connect to aCT database: {0}".format(str(e)))
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
        tmpLog = core_utils.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="kill_worker")

        if workspec.batchID is None:
            tmpLog.info("workerID={0} has no batch ID so assume was not submitted - skipped".format(workspec.workerID))
            return True, ""

        try:
            # Only kill jobs which are still active
            self.actDB.updateJobs(
                "id={0} AND actpandastatus IN ('sent', 'starting', 'running')".format(workspec.batchID), {"actpandastatus": "tobekilled", "pandastatus": None}
            )
        except Exception as e:
            if self.actDB:
                tmpLog.error("Failed to cancel job {0} in aCT: {1}".format(workspec.batchID, str(e)))
            return False, str(e)

        tmpLog.info("Job {0} cancelled in aCT".format(workspec.batchID))
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
        tmpLog = core_utils.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="sweep_worker")
        # clean up worker directory
        if os.path.exists(workspec.accessPoint):
            shutil.rmtree(workspec.accessPoint)
            tmpLog.info("removed {0}".format(workspec.accessPoint))
        else:
            tmpLog.info("access point {0} already removed.".format(workspec.accessPoint))
        # return
        return True, ""
