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
        tmpLog.info(f"[{self.adaptor}] SAGA adaptor will be used")

    # kill a worker
    def kill_worker(self, workspec):
        """Kill a worker in a scheduling system like batch systems and computing elements.

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        job_service = saga.job.Service(self.adaptor)
        tmpLog = self.make_logger(baseLogger, f"workerID={workspec.workerID}", method_name="kill_worker")
        tmpLog.info(f"[{self.adaptor}] SAGA adaptor will be used to kill worker {workspec.workerID} with batchid {workspec.batchID}")
        errStr = ""

        if workspec.batchID:
            saga_submission_id = f"[{self.adaptor}]-[{workspec.batchID}]"
            try:
                worker = job_service.get_job(saga_submission_id)
                tmpLog.info(f"SAGA State for submission with batchid: {workspec.batchID} is: {worker.state}")
                harvester_job_state = SAGASubmitter.status_translator(worker.state)
                tmpLog.info(f"Worker state with batchid: {workspec.batchID} is: {harvester_job_state}")
                if worker.state in [saga.job.PENDING, saga.job.RUNNING]:
                    worker.cancel()
                    tmpLog.info(f"Worker {workspec.workerID} with batchid {workspec.batchID} canceled")
            except saga.SagaException as ex:
                errStr = ex.get_message()
                tmpLog.info(f"An exception occured during canceling of worker: {errStr}")

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
        tmpLog = self.make_logger(baseLogger, f"workerID={workspec.workerID}", method_name="sweep_worker")

        # Clean up worker directory
        if os.path.exists(workspec.accessPoint):
            shutil.rmtree(workspec.accessPoint)
            tmpLog.info(" removed {1}".format(workspec.workerID, workspec.accessPoint))
        else:
            tmpLog.info("access point already removed.")
        # Return
        return True, ""
