import arc
import json
import time
import urllib

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvesterconfig import harvester_config

from act.common.aCTConfig import aCTConfigARC
from act.common.aCTProxy import aCTProxy
from act.atlas.aCTDBPanda import aCTDBPanda

# logger
baseLogger = core_utils.setup_logger('act_submitter')

# submitter for aCT
class ACTSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        # Set up aCT DB connection
        self.log = core_utils.make_logger(baseLogger, 'aCT submitter', method_name='__init__')
        self.actDB = aCTDBPanda(self.log)
        # Credential dictionary role: proxy file
        self.certs = dict(zip([r.split('=')[1] for r in list(harvester_config.credmanager.voms)],
                              list(harvester_config.credmanager.outCertFile)))
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
            self.log.info("Proxy {0} with DN {1} and role {2}".format(proxy, dn, role))
    
            actp = aCTProxy(self.log)
            attr = '/atlas/Role='+role
            proxyid = actp.getProxyId(dn, attr)
            if not proxyid:
                raise Exception("Proxy with DN {0} and attribute {1} was not found in proxies table".format(dn, attr))

            self.proxymap[role] = proxyid


    # submit workers
    def submit_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:

            tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workSpec.workerID),
                                            method_name='submit_workers')

            queueconfigmapper = QueueConfigMapper()
            queueconfig = queueconfigmapper.get_queue(workSpec.computingSite)
            prodSourceLabel = queueconfig.get_source_label()

            # If jobSpec is defined we are in push mode, if not pull mode
            # Both assume one to one worker to job mapping
            jobSpec = workSpec.get_jobspec_list()
            if jobSpec:
                jobSpec = jobSpec[0]
                tmpLog.debug("JobSpec: {0}".format(jobSpec.values_map()))

            desc = {}
            desc['pandastatus'] = 'sent'
            desc['actpandastatus'] = 'sent'
            desc['siteName'] = workSpec.computingSite
            desc['proxyid'] = self.proxymap['pilot' if prodSourceLabel == 'user' else 'production']
            desc['sendhb'] = 0
            metadata = {'harvesteraccesspoint': workSpec.get_access_point(),
                        'schedulerid': 'harvester-{}'.format(harvester_config.master.harvester_id)}
            desc['metadata'] = json.dumps(metadata)

            if jobSpec:
                # push mode: aCT takes the url-encoded job description (like it gets from panda server)
                pandaid = jobSpec.PandaID
                actjobdesc = urllib.urlencode(jobSpec.jobParams)
            else:
                # pull mode: just set pandaid (to workerid) and prodsourcelabel
                pandaid = workSpec.workerID
                actjobdesc = 'PandaID=%d&prodSourceLabel=%s' % (pandaid, prodSourceLabel)

            tmpLog.info("Inserting job {0} into aCT DB: {1}".format(pandaid, str(desc)))
            try:
                batchid = self.actDB.insertJob(pandaid, actjobdesc, desc)['LAST_INSERT_ID()']
            except Exception as e:
                result = (False, "Failed to insert job into aCT DB: {0}".format(str(e)))
            else:
                tmpLog.info("aCT batch id {0}".format(batchid))
                workSpec.batchID = str(batchid)
                # Set log files in workSpec
                today = time.strftime('%Y-%m-%d', time.gmtime())
                logurl = '/'.join([queueconfig.submitter.get('logBaseURL'), today, workSpec.computingSite, str(pandaid)])
                workSpec.set_log_file('batch_log', '{0}.log'.format(logurl))
                workSpec.set_log_file('stdout', '{0}.out'.format(logurl))
                workSpec.set_log_file('stderr', '{0}.err'.format(logurl))
                result = (True, '')
            retList.append(result)

        return retList
