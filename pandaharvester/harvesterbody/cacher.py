import datetime
import json
import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor

import requests
import requests.exceptions

from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy

# logger
_logger = core_utils.setup_logger("cacher")


# cache information
class Cacher(AgentBase):
    # constructor
    def __init__(self, communicator, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.communicator = communicator

    # main loop
    def run(self):
        while True:
            # execute
            self.execute()
            # check if being terminated
            if self.terminated(harvester_config.cacher.sleepTime, randomize=False):
                return

    # main
    def execute(self, force_update=False, skip_lock=False, n_threads=0):
        mainLog = self.make_logger(_logger, f"id={self.get_pid()}", method_name="execute")
        # get lock
        locked = self.dbProxy.get_process_lock("cacher", self.get_pid(), harvester_config.cacher.sleepTime)
        if locked or skip_lock:
            mainLog.debug("getting information")
            timeLimit = core_utils.naive_utcnow() - datetime.timedelta(minutes=harvester_config.cacher.refreshInterval)
            itemsList = []
            nItems = 4
            for tmpStr in harvester_config.cacher.data:
                tmpItems = tmpStr.split("|")
                if len(tmpItems) < 3:
                    continue
                tmpItems += [None] * (nItems - len(tmpItems))
                tmpItems = tmpItems[:nItems]
                itemsList.append(tmpItems)
            # refresh cache function

            def _refresh_cache(inputs):
                mainKey, subKey, infoURL, dumpFile = inputs
                if subKey == "":
                    subKey = None
                # check last update time
                lastUpdateTime = self.dbProxy.get_cache_last_update_time(mainKey, subKey)
                if (not force_update) and lastUpdateTime is not None and lastUpdateTime > timeLimit:
                    return
                # get information
                tmpStat, newInfo = self.get_data(infoURL, mainLog)
                if not tmpStat:
                    mainLog.error(f"failed to get info for key={mainKey} subKey={subKey}")
                    return
                # update
                tmpStat = self.dbProxy.refresh_cache(mainKey, subKey, newInfo)
                if tmpStat:
                    mainLog.debug(f"refreshed key={mainKey} subKey={subKey}")
                    if dumpFile is not None:
                        try:
                            tmpFileName = dumpFile + ".tmp"
                            with open(tmpFileName, "w") as tmpFile:
                                json.dump(newInfo, tmpFile)
                            shutil.move(tmpFileName, dumpFile)
                        except Exception:
                            core_utils.dump_error_message(mainLog)
                else:
                    mainLog.error(f"failed to refresh key={mainKey} subKey={subKey} due to a DB error")

            # loop over all items
            if n_threads:
                mainLog.debug(f"refresh cache with {n_threads} threads")
                with ThreadPoolExecutor(n_threads) as thread_pool:
                    thread_pool.map(_refresh_cache, itemsList)
            else:
                mainLog.debug("refresh cache")
                for inputs in itemsList:
                    _refresh_cache(inputs)
            mainLog.debug("done")

    # get new data

    def get_data(self, info_url, tmp_log):
        retStat = False
        retVal = None
        # resolve env variable
        match = re.search(r"\$\{*([^\}]+)\}*", info_url)
        if match:
            var_name = match.group(1)
            if var_name not in os.environ:
                errMsg = f"undefined environment variable: {var_name}"
                tmp_log.error(errMsg)
            else:
                info_url = os.environ[var_name]
        if info_url.startswith("file:"):
            try:
                with open(info_url.split(":")[-1], "r") as infoFile:
                    retVal = infoFile.read()
                    try:
                        retVal = json.loads(retVal)
                    except Exception:
                        pass
            except Exception:
                core_utils.dump_error_message(tmp_log)
        elif info_url.startswith("http:"):
            try:
                res = requests.get(info_url, timeout=60)
                if res.status_code == 200:
                    try:
                        retVal = res.json()
                    except Exception:
                        errMsg = f"corrupted json from {info_url} : {res.text}"
                        tmp_log.error(errMsg)
                else:
                    errMsg = f"failed to get {info_url} with StatusCode={res.status_code} {res.text}"
                    tmp_log.error(errMsg)
            except requests.exceptions.ReadTimeout:
                tmp_log.error(f"read timeout when getting data from {info_url}")
            except Exception:
                core_utils.dump_error_message(tmp_log)
        elif info_url.startswith("https:"):
            try:
                try:
                    # try with pandacon certificate
                    cert_file = harvester_config.pandacon.cert_file
                    key_file = harvester_config.pandacon.key_file
                    ca_cert = harvester_config.pandacon.ca_cert
                    if ca_cert is False:
                        cert = None
                    else:
                        cert = (cert_file, key_file)
                    res = requests.get(info_url, cert=cert, verify=ca_cert, timeout=60)
                except requests.exceptions.SSLError:
                    # try without certificate
                    res = requests.get(info_url, timeout=60)
            except requests.exceptions.ReadTimeout:
                tmp_log.error(f"read timeout when getting data from {info_url}")
            except Exception:
                core_utils.dump_error_message(tmp_log)
            else:
                if res.status_code == 200:
                    try:
                        retVal = res.json()
                    except Exception:
                        errMsg = f"corrupted json from {info_url} : {res.text}"
                        tmp_log.error(errMsg)
                else:
                    errMsg = f"failed to get {info_url} with StatusCode={res.status_code} {res.text}"
                    tmp_log.error(errMsg)
        elif info_url.startswith("panda_cache:"):
            try:
                publicKey, privateKey = info_url.split(":")[-1].split("&")
                retVal, outStr = self.communicator.get_key_pair(publicKey, privateKey)
                if retVal is None:
                    tmp_log.error(outStr)
            except Exception:
                core_utils.dump_error_message(tmp_log)
        elif info_url.startswith("panda_server:"):
            try:
                method_name = info_url.split(":")[-1]
                method_function = getattr(self.communicator, method_name)
                retVal, outStr = method_function()
                if not retVal:
                    tmp_log.error(outStr)
            except Exception:
                core_utils.dump_error_message(tmp_log)
        else:
            errMsg = f"unsupported protocol for {info_url}"
            tmp_log.error(errMsg)
        if retVal is not None:
            retStat = True
        return retStat, retVal

    # set single mode
    def set_single_mode(self, single_mode):
        self.singleMode = single_mode
