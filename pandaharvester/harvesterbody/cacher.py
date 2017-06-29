import json
import datetime
import requests

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvesterbody.agent_base import AgentBase

# logger
_logger = core_utils.setup_logger()


# cache information
class Cacher(AgentBase):
    # constructor
    def __init__(self, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()


    # main loop
    def run(self):
        while True:
            mainLog = core_utils.make_logger(_logger, 'id={0}'.format(self.ident))
            mainLog.debug('getting information')
            timeLimit = datetime.datetime.utcnow() - \
                        datetime.timedelta(minutes=harvester_config.cacher.refreshInterval)
            for tmpStr in harvester_config.cacher.data:
                tmpItems = tmpStr.split('|')
                if len(tmpItems) != 3:
                    continue
                mainKey, subKey, infoURL = tmpItems
                if subKey == '':
                    subKey = None
                # check last update time
                lastUpdateTime = self.dbProxy.get_cache_last_update_time(mainKey, subKey)
                if lastUpdateTime is not None and lastUpdateTime > timeLimit:
                    continue
                # get information
                tmpStat, newInfo = self.get_data(infoURL, mainLog)
                if not tmpStat:
                    mainLog.error('failed to get info for key={0} subKey={1}'.format(mainKey, subKey))
                    continue
                # update
                tmpStat = self.dbProxy.refresh_cache(mainKey, subKey, newInfo)
                if tmpStat:
                    mainLog.debug('refreshed key={0} subKey={1}'.format(mainKey, subKey))
            mainLog.debug('done')
            # check if being terminated
            if self.terminated(harvester_config.cacher.sleepTime):
                mainLog.debug('terminated')
                return


    # get new data
    def get_data(self, info_url, tmp_log):
        retStat = False
        retVal = None
        if info_url.startswith('file:'):
            try:
                with open(info_url.split(':')[-1], 'r') as infoFile:
                    retVal = infoFile.read()
                    try:
                        retVal = json.loads(retVal)
                    except:
                        pass
            except:
                core_utils.dump_error_message(tmp_log)
        elif info_url.startswith('http:'):
            try:
                res = requests.get(info_url)
                if res.status_code == 200:
                    try:
                        retVal = res.json()
                    except:
                        retVal = res.text
                else:
                    errMsg = 'failed with StatusCode={0} {1}'.format(res.status_code, res.text)
                    tmp_log.error(errMsg)
            except:
                core_utils.dump_error_message(tmp_log)
        else:
            errMsg = 'unsupported protocol for {0}'.format(info_url)
            tmp_log.error(errMsg)
        if retVal is not None:
            retStat = True
        return retStat, retVal

    # set single mode
    def set_single_mode(self, single_mode):
        self.singleMode = single_mode
