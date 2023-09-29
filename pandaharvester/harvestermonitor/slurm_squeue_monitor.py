import re

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

import json
import os

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger("slurm_squeue_monitor")


# monitor for SLURM batch system with squeue
class SlurmSqueueMonitor(PluginBase):
    _HARVESTER_POSTMORTEM_FILENAME = "FINISHED"

    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check workers
    def check_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workSpec.workerID), method_name="check_workers")
            # here try to load file
            current_postmortem_fname = "%s/%s" % (workSpec.accessPoint, SlurmSqueueMonitor._HARVESTER_POSTMORTEM_FILENAME)

            if os.path.exists(current_postmortem_fname):
                with open(current_postmortem_fname) as postmortem:
                    try:
                        worker_status_json = json.load(postmortem)
                        if "worker_status" in worker_status_json:
                            worker_status = None
                            if worker_status_json["worker_status"] == "finished":
                                worker_status = WorkSpec.ST_finished
                            if worker_status_json["worker_status"] == "failed":
                                worker_status = WorkSpec.ST_failed
                            if worker_status is not None:
                                retList.append((worker_status, ""))
                                continue
                    except json.JSONDecodeError:
                        tmpLog.debug("Not able to parse JSON in postmortem for a worker: %s, continung with SLURM CLI" % current_postmortem_fname)

            # command
            comStr = "squeue -j {0}".format(workSpec.batchID)
            # check
            tmpLog.debug("check with {0}".format(comStr))
            p = subprocess.Popen(comStr.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            newStatus = workSpec.status
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug("retCode={0}".format(retCode))
            errStr = ""
            stdOut_str = stdOut if (isinstance(stdOut, str) or stdOut is None) else stdOut.decode()
            stdErr_str = stdErr if (isinstance(stdErr, str) or stdErr is None) else stdErr.decode()
            if retCode == 0:
                for tmpLine in stdOut_str.split("\n"):
                    tmpMatch = re.search("{0} ".format(workSpec.batchID), tmpLine)
                    if tmpMatch is not None:
                        errStr = tmpLine
                        batchStatus = tmpLine.split()[4]
                        if batchStatus in ["R", "RUNNING", "COMPLETING", "STOPPED", "SUSPENDED"]:
                            newStatus = WorkSpec.ST_running
                        elif batchStatus in ["COMPLETED", "PREEMPTED", "TIMEOUT"]:
                            newStatus = WorkSpec.ST_finished
                        elif batchStatus in ["CANCELLED"]:
                            newStatus = WorkSpec.ST_cancelled
                        elif batchStatus in ["PD", "CONFIGURING", "PENDING"]:
                            newStatus = WorkSpec.ST_submitted
                        else:
                            newStatus = WorkSpec.ST_failed
                        tmpLog.debug("batchStatus {0} -> workerStatus {1}".format(batchStatus, newStatus))
                        break
                retList.append((newStatus, errStr))
            else:
                # squeue does not show finished jobs, gives return code 1
                # Assume finished for now. Maybe look in workdir.
                newStatus = WorkSpec.ST_finished
                errStr = "{0} {1}".format(stdOut_str, stdErr_str)
                tmpLog.error(errStr)
                # if 'slurm_load_jobs error: Invalid job id specified' in errStr:
                #    newStatus = WorkSpec.ST_failed
                retList.append((newStatus, errStr))
        return True, retList

    def _get_worker_completion_details():
        # try to open FINISHED file
        pass
