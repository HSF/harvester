import os
import json
import requests

from pandaharvester.harvestersweeper.base_sweeper import BaseSweeper
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.superfacility_utils import SuperfacilityClient


baseLogger = core_utils.setup_logger("sf_sweeper")


class SuperfacilitySweeper(BaseSweeper):
    def __init__(self, **kwargs):
        BaseSweeper.__init__(self, **kwargs)
        self.cred_dir = kwarg.get("sf_cred_dir")
        self.sf_client = SuperfacilityClient(self.cred_dir)

    def kill_worker(self, workspec):
        tmpLog = self.make_logger(baseLogger, f"workerID={workspec.workerID}", method_name="kill_worker")
        jobid = workspec.batchID
        if not jobid:
            return False, "no batchID to kill"

        try:
            r = self.sf_client.delete(f"/compute/jobs/perlmutter/{jobid}")
            data = r.json()
        except Exception as e:
            errStr = f"Submission of a job cancelling fail for jobid = {jobid} with error: {e}"
            tmpLog.error(errStr)
            return False, errStr

        if data.get('status') == 'success':
            tmpLog.info(f"Succeeded to kill workerID={workspec.workerID} batchID={workspec.workerID}")
        else:
            errStr = f"Failed to cancel job {jobid}: status: {data.get('status')}"
            tmpLog.error(errStr)
            return False, errStr
        return True, ""

    def sweep_worker(self, workspec):
        tmpLog = self.make_logger(baseLogger, f"workerID={workspec.workerID}", method_name="sweep_worker")
        ap = workspec.accessPoint
        if ap and os.path.exists(ap):
            try:
                shutil.rmtree(ap)
                logger.info(f"Removed directory {ap}")
            except Exception as e:
                err = f"Failed to remove {ap}: {e}"
                logger.error(err)
                return False, err
        else:
            logger.info("Access point already removed or none provided.")
        return True, ""
