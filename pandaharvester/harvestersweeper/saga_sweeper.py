import os
import shutil

import saga

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestersubmitter.saga_submitter import SAGASubmitter

# logger
baseLogger = core_utils.setup_logger("saga_sweeper")


# dummy plugin for sweeper
class SAGASweeper(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        tmpLog = core_utils.make_logger(baseLogger, method_name="__init__")
        tmpLog.info("[{0}] SAGA adaptor will be used".format(self.adaptor))

    # kill a worker
    def kill_worker(self, workspec):
        """Kill a worker in a scheduling system like batch systems and computing elements.

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        job_service = saga.job.Service(self.adaptor)
        tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="kill_worker")
        tmpLog.info("[{0}] SAGA adaptor will be used to kill worker {1} with batchid {2}".format(self.adaptor, workspec.workerID, workspec.batchID))
        errStr = ""

        if workspec.batchID:
            saga_submission_id = "[{0}]-[{1}]".format(self.adaptor, workspec.batchID)
            try:
                worker = job_service.get_job(saga_submission_id)
                tmpLog.info("SAGA State for submission with batchid: {0} is: {1}".format(workspec.batchID, worker.state))
                harvester_job_state = SAGASubmitter.status_translator(worker.state)
                tmpLog.info("Worker state with batchid: {0} is: {1}".format(workspec.batchID, harvester_job_state))
                if worker.state in [saga.job.PENDING, saga.job.RUNNING]:
                    worker.cancel()
                    tmpLog.info("Worker {0} with batchid {1} canceled".format(workspec.workerID, workspec.batchID))
            except saga.SagaException as ex:
                errStr = ex.get_message()
                tmpLog.info("An exception occured during canceling of worker: {0}".format(errStr))

                # probably 'failed' is not proper state in this case, 'undefined' looks a bit better
                # harvester_job_state = workspec.ST_failed

        job_service.close()

        return True, errStr

    # cleanup for a worker
    def sweep_worker(self, workspec):
        """Perform cleanup procedures for a worker, such as deletion of work directory.

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """

        # Make logger
        tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="sweep_worker")

        # Clean up worker directory
        if os.path.exists(workspec.accessPoint):
            shutil.rmtree(workspec.accessPoint)
            tmpLog.info(" removed {1}".format(workspec.workerID, workspec.accessPoint))
        else:
            tmpLog.info("access point already removed.")
        # Return
        return True, ""
