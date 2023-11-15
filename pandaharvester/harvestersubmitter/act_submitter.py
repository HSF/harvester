import json
import socket
import time
import urllib.parse

import arc
from act.atlas.aCTDBPanda import aCTDBPanda
from act.common.aCTConfig import aCTConfigARC
from act.common.aCTProxy import aCTProxy
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper

# logger
baseLogger = core_utils.setup_logger("act_submitter")

# submitter for aCT


class ACTSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        self.hostname = socket.getfqdn()
        # Set up aCT DB connection
        self.log = core_utils.make_logger(baseLogger, "aCT submitter", method_name="__init__")
        self.actDB = aCTDBPanda(self.log)
        # Credential dictionary role: proxy file
        self.certs = dict(zip([r.split("=")[1] for r in list(harvester_config.credmanager.voms)], list(harvester_config.credmanager.outCertFile)))
        # Map of role to aCT proxyid
        self.proxymap = {}

        # Get proxy info
        # TODO: better to send aCT the proxy file and let it handle it
        for role, proxy in self.certs.items():
            cred_type = arc.initializeCredentialsType(arc.initializeCredentialsType.SkipCredentials)
            uc = arc.UserConfig(cred_type)
            uc.ProxyPath(str(proxy))
            cred = arc.Credential(uc)
            dn = cred.GetIdentityName()

            actp = aCTProxy(self.log)
            attr = "/atlas/Role=" + role
            proxyid = actp.getProxyId(dn, attr)
            if not proxyid:
                raise Exception(f"Proxy with DN {dn} and attribute {attr} was not found in proxies table")

            self.proxymap[role] = proxyid

    # submit workers

    def submit_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            tmpLog = core_utils.make_logger(baseLogger, f"workerID={workSpec.workerID}", method_name="submit_workers")

            queueconfigmapper = QueueConfigMapper()
            queueconfig = queueconfigmapper.get_queue(workSpec.computingSite)
            prodSourceLabel = queueconfig.get_source_label(workSpec.jobType)

            # If jobSpec is defined we are in push mode, if not pull mode
            # Both assume one to one worker to job mapping
            jobSpec = workSpec.get_jobspec_list()
            if jobSpec:
                jobSpec = jobSpec[0]
                tmpLog.debug(f"JobSpec: {jobSpec.values_map()}")
                # Unified queues: take prodsourcelabel from job
                prodSourceLabel = jobSpec.jobParams.get("prodSourceLabel", prodSourceLabel)

            desc = {}
            # If we need to prefetch events, set aCT status waiting.
            # feed_events in act_messenger will fill events and release the job
            if queueconfig.prefetchEvents:
                desc["pandastatus"] = "waiting"
                desc["actpandastatus"] = "waiting"
                desc["arcjobid"] = -1  # dummy id to prevent submission
            else:
                desc["pandastatus"] = "sent"
                desc["actpandastatus"] = "sent"
            desc["siteName"] = workSpec.computingSite
            desc["proxyid"] = self.proxymap["pilot" if prodSourceLabel in ["user", "panda"] else "production"]
            desc["prodSourceLabel"] = prodSourceLabel
            desc["sendhb"] = 0
            metadata = {
                "harvesteraccesspoint": workSpec.get_access_point(),
                "schedulerid": f"harvester-{harvester_config.master.harvester_id}",
                "harvesterid": harvester_config.master.harvester_id,
                "harvesterworkerid": workSpec.workerID,
            }
            desc["metadata"] = json.dumps(metadata)

            if jobSpec:
                # push mode: aCT takes the url-encoded job description (like it gets from panda server)
                pandaid = jobSpec.PandaID
                actjobdesc = urllib.parse.urlencode(jobSpec.jobParams)
            else:
                # pull mode: set pandaid (to workerid), prodsourcelabel, resource type and requirements
                pandaid = workSpec.workerID
                actjobdesc = "&".join(
                    [
                        f"PandaID={pandaid}",
                        f"prodSourceLabel={prodSourceLabel}",
                        f"resourceType={workSpec.resourceType}",
                        f"minRamCount={workSpec.minRamCount}",
                        f"coreCount={workSpec.nCore}",
                        f"logFile={pandaid}.pilot.log",
                    ]
                )

            tmpLog.info(f"Inserting job {pandaid} into aCT DB: {str(desc)}")
            try:
                batchid = self.actDB.insertJob(pandaid, actjobdesc, desc)["LAST_INSERT_ID()"]
            except Exception as e:
                result = (False, f"Failed to insert job into aCT DB: {str(e)}")
            else:
                tmpLog.info(f"aCT batch id {batchid}")
                workSpec.batchID = str(batchid)
                workSpec.submissionHost = self.hostname
                workSpec.nativeStatus = desc["actpandastatus"]
                # Set log files in workSpec
                today = time.strftime("%Y-%m-%d", time.gmtime())
                logurl = "/".join([queueconfig.submitter.get("logBaseURL"), today, workSpec.computingSite, str(pandaid)])
                workSpec.set_log_file("batch_log", f"{logurl}.log")
                workSpec.set_log_file("stdout", f"{logurl}.out")
                workSpec.set_log_file("stderr", f"{logurl}.err")
                workSpec.set_log_file("jdl", f"{logurl}.jdl")
                result = (True, "")
            retList.append(result)

        return retList
