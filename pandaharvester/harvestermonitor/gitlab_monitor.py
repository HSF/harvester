import json
import requests
import os.path
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.gitlab_utils import get_job_params

# logger
baseLogger = core_utils.setup_logger("gitlab_monitor")


# dummy monitor
class GitlabMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.timeout = 180
        PluginBase.__init__(self, **kwarg)

    # check workers
    def check_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workSpec.workerID), method_name="check_workers")
            try:
                params = get_job_params(workSpec)
                url = "{}/{}/pipelines/{}".format(params["project_api"], params["project_id"], workSpec.batchID.split()[0])
                try:
                    tmpLog.debug("check pipeline at {}".format(url))
                    r = requests.get(url, headers={"PRIVATE-TOKEN": params["secrets"][params["access_token"]]}, timeout=self.timeout)
                    response = r.json()
                    tmpLog.debug("got {}".format(str(response)))
                except Exception:
                    err_str = core_utils.dump_error_message(tmpLog)
                    retList.append((WorkSpec.ST_idle, err_str))
                    continue
                newMsg = ""
                if "status" not in response:
                    newStatus = WorkSpec.ST_idle
                    if "message" in response:
                        newMsg = response["message"]
                    else:
                        newMsg = "failed to check due to unknown reason"
                else:
                    if response["status"] == "success":
                        newStatus = WorkSpec.ST_finished
                    elif response["status"] == "failed":
                        newStatus = WorkSpec.ST_failed
                    elif response["status"] == "created":
                        newStatus = WorkSpec.ST_submitted
                    elif response["status"] == "pending":
                        newStatus = WorkSpec.ST_pending
                    else:
                        newStatus = WorkSpec.ST_running
                tmpLog.debug("newStatus={0}".format(newStatus))
                retList.append((newStatus, newMsg))
            except Exception:
                err_str = core_utils.dump_error_message(tmpLog)
                retList.append((WorkSpec.ST_idle, err_str))
        return True, retList
