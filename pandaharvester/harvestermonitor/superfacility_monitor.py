import requests
import json
import os

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestermisc.superfacility_utils import SuperfacilityClient

# logger
baseLogger = core_utils.setup_logger("sf_monitor")

# monitor for SuperFacility API
class SuperfacilityMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.cred_dir = kwarg.get("sf_cred_dir")
        self.sf_client = SuperfacilityClient(self.cred_dir)
 
    def check_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, f"workerID={workSpec.workerID}", method_name="check_workers")

            jobid = workSpec.batchID
            if not jobid:
                retList.append((WorkSpec.ST_failed, "no batchID, job is not submitted!"))
                continue

            try:
                r = self.sf_client.get(f"/compute/jobs/perlmutter/{jobid}?sacct=true&cached=false")
                data = r.json()
            #FIXME: How to handle httperror?
            except requests.HTTPError as e:
                newStatus = WorkSpec.ST_failed
                retList.append((WorkSpec.ST_failed, f"can not get query slurm job {jobid} due to {e}"))
                continue
            
            batchStatus = data["output"][0]['state'].upper()
            #FIXME: Are these mapping correct? Some do not exist, and some seem mismatch
            if batchStatus in ["RUNNING", "COMPLETING", "STOPPED", "SUSPENDED"]:
                newStatus = WorkSpec.ST_running
            elif batchStatus in ["COMPLETED", "PREEMPTED", "TIMEOUT"]:
                newStatus = WorkSpec.ST_finished
            elif batchStatus in ["CANCELLED"]:
                newStatus = WorkSpec.ST_cancelled
            elif batchStatus in ["CONFIGURING", "PENDING"]:
                newStatus = WorkSpec.ST_submitted
            else:
                newStatus = WorkSpec.ST_failed
            tmpLog.debug(f"batchStatus {batchStatus} -> workerStatus {newStatus}")
            retList.append((newStatus, ""))
        return True, retList
