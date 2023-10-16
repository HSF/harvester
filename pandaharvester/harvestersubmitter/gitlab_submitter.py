import uuid
import os
import json
import requests
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.gitlab_utils import store_job_params

# setup base logger
baseLogger = core_utils.setup_logger("gitlab_submitter")


# dummy submitter
class GitlabSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.timeout = 180
        PluginBase.__init__(self, **kwarg)

    # trigger pipeline jobs
    def submit_workers(self, workspec_list):
        tmpLog = self.make_logger(baseLogger, method_name="submit_workers")
        tmpLog.debug("start nWorkers={0}".format(len(workspec_list)))
        retList = []
        for workSpec in workspec_list:
            try:
                jobSpec = workSpec.get_jobspec_list()[0]
                secrets = jobSpec.jobParams["secrets"]
                params = json.loads(jobSpec.jobParams["jobPars"])
                params["secrets"] = secrets
                store_job_params(workSpec, params)
                url = "{}/{}/trigger/pipeline".format(params["project_api"], params["project_id"])
                data = {"token": secrets[params["trigger_token"]], "ref": params["ref"]}
                try:
                    tmpLog.debug("trigger pipeline at {}".format(url))
                    r = requests.post(url, data=data, timeout=self.timeout)
                    response = r.json()
                    tmpLog.debug("got {}".format(str(response)))
                except Exception:
                    err_str = core_utils.dump_error_message(tmpLog)
                    retList.append((False, err_str))
                    continue
                if response["status"] == "created":
                    workSpec.batchID = "{} {}".format(response["id"], response["project_id"])
                    tmpLog.debug("succeeded with {}".format(workSpec.batchID))
                    retList.append((True, ""))
                else:
                    err_str = "failed to trigger with {}".format(response["status"])
                    tmpLog.error(err_str)
                    retList.append((False, err_str))
            except Exception:
                err_str = core_utils.dump_error_message(tmpLog)
                retList.append((False, err_str))
        tmpLog.debug("done")
        return retList
