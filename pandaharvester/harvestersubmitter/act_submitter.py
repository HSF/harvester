import arc
import urllib

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

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
        self.conf = aCTConfigARC()
        self.actDB = aCTDBPanda(self.log, self.conf.get(["db", "file"]))

        # Get proxy info
        # TODO: specify DN in conf instead
        cred_type = arc.initializeCredentialsType(arc.initializeCredentialsType.SkipCredentials)
        uc = arc.UserConfig(cred_type)
        uc.ProxyPath(str(self.conf.get(['voms', 'proxypath'])))
        cred = arc.Credential(uc)
        dn = cred.GetIdentityName()
        self.log.info("Running under DN %s" % dn)

        # Set up proxy map (prod/pilot roles)
        self.proxymap = {}
        actp = aCTProxy(self.log)
        for role in self.conf.getList(['voms', 'roles', 'item']):
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

            # Assume for aCT that jobs are always pre-fetched (no late-binding)
            for jobSpec in workSpec.get_jobspec_list():

                tmpLog.debug("JobSpec: {0}".format(jobSpec.values_map()))
                desc = {}
                desc['pandastatus'] = 'sent'
                desc['actpandastatus'] = 'sent'
                desc['siteName'] = jobSpec.computingSite
                desc['proxyid'] = self.proxymap['pilot' if jobSpec.jobParams['prodSourceLabel'] == 'user' else 'production']
                try:
                    desc['sendhb'] = 'running' in self.noHeartbeat
                except:
                    desc['sendhb'] = 0

                # aCT takes the url-encoded job description (like it gets from panda server)
                actjobdesc = urllib.urlencode(jobSpec.jobParams)
                try:
                    tmpLog.info("Inserting job {0} into aCT DB: {1}".format(jobSpec.PandaID, str(desc)))
                    batchid = self.actDB.insertJob(jobSpec.PandaID, actjobdesc, desc)['LAST_INSERT_ID()']
                    tmpLog.info("aCT batch id {0}".format(batchid))
                    workSpec.batchID = str(batchid)
                    result = (True, '')
                except Exception as e:
                    result = (False, "Failed to insert job into aCT DB: {0}".format(str(e)))

                retList.append(result)

        return retList
