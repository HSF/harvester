import datetime
import json
import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor

import requests
import requests.exceptions

from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterbody.agent_plugin_utils import as_list
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
        main_log = self.make_logger(_logger, f"id={self.get_pid()}", method_name="run")
        while True:
            # execute
            try:
                self.execute()
            except Exception:
                core_utils.dump_error_message(main_log)
            # check if being terminated
            if self.terminated(harvester_config.cacher.sleepTime, randomize=False):
                return

    # parse cacher.data, one main_key|sub_key|URL|dump_file(optional) per line, into (main_key, sub_key, url, dump_file)
    @staticmethod
    def get_items(tmp_log):
        items = []
        for line in as_list(harvester_config.cacher.data):
            fields = [field.strip() for field in line.split("|")]
            if len(fields) < 3:
                tmp_log.error(f"skipped malformed data line: {line}")
                continue
            main_key, sub_key, info_url = fields[:3]
            dump_file = fields[3] if len(fields) > 3 else None
            items.append((main_key, sub_key or None, info_url, dump_file or None))
        return items

    # refresh cache
    def execute(self, force_update=False, skip_lock=False, n_threads=0):
        main_log = self.make_logger(_logger, f"id={self.get_pid()}", method_name="execute")
        # get lock
        locked = self.dbProxy.get_process_lock("cacher", self.get_pid(), harvester_config.cacher.sleepTime)
        if not locked and not skip_lock:
            return
        main_log.debug("getting information")
        items = self.get_items(main_log)
        # loop over all items
        if n_threads:
            main_log.debug(f"refresh cache with {n_threads} threads")
            with ThreadPoolExecutor(n_threads) as thread_pool:
                for item in items:
                    thread_pool.submit(self.refresh_item, item, force_update, main_log)
        else:
            main_log.debug("refresh cache")
            for item in items:
                self.refresh_item(item, force_update, main_log)
        main_log.debug("done")

    # refresh one item of cache if expired or forced
    def refresh_item(self, item, force_update, tmp_log):
        main_key, sub_key, info_url, dump_file = item
        try:
            # check last update time; a key listed in refreshSecondsByKey has its own refresh interval in seconds
            if not force_update:
                last_update_time = self.dbProxy.get_cache_last_update_time(main_key, sub_key)
                refresh_seconds = getattr(harvester_config.cacher, "refreshSecondsByKey", {}).get(main_key)
                if refresh_seconds is None:
                    refresh_seconds = harvester_config.cacher.refreshInterval * 60
                if last_update_time is not None and last_update_time > core_utils.naive_utcnow() - datetime.timedelta(seconds=refresh_seconds):
                    return
            # get information
            tmp_stat, new_info = self.get_data(info_url, tmp_log)
            if not tmp_stat:
                tmp_log.error(f"failed to get info for key={main_key} subKey={sub_key}")
                return
            tmp_log.debug(f"got data for key={main_key} subKey={sub_key} from {info_url}")
            # update
            if not self.dbProxy.refresh_cache(main_key, sub_key, new_info):
                tmp_log.error(f"failed to refresh key={main_key} subKey={sub_key} due to a DB error")
                return
            tmp_log.debug(f"refreshed key={main_key} subKey={sub_key}")
            # dump to file
            if dump_file is not None:
                tmp_file_name = dump_file + ".tmp"
                with open(tmp_file_name, "w") as tmp_file:
                    json.dump(new_info, tmp_file)
                shutil.move(tmp_file_name, dump_file)
        except Exception:
            core_utils.dump_error_message(tmp_log)

    # get new data
    def get_data(self, info_url, tmp_log):
        ret_val = None
        # resolve env variable
        match = re.search(r"\$\{*([^\}]+)\}*", info_url)
        if match:
            var_name = match.group(1)
            if var_name not in os.environ:
                tmp_log.error(f"undefined environment variable: {var_name}")
            else:
                info_url = os.environ[var_name]
        try:
            if info_url.startswith("file:"):
                with open(info_url.split(":")[-1], "r") as info_file:
                    ret_val = info_file.read()
                try:
                    ret_val = json.loads(ret_val)
                except Exception:
                    pass
            elif info_url.startswith(("http:", "https:")):
                ret_val = self.get_url_data(info_url, tmp_log)
            elif info_url.startswith("panda_cache:"):
                public_key, private_key = info_url.split(":")[-1].split("&")
                ret_val, out_str = self.communicator.get_key_pair(public_key, private_key)
                if ret_val is None:
                    tmp_log.error(out_str)
            elif info_url.startswith("panda_server:"):
                method_name = info_url.split(":")[-1]
                ret_val, out_str = getattr(self.communicator, method_name)()
                if not ret_val:
                    tmp_log.error(out_str)
            else:
                tmp_log.error(f"unsupported protocol for {info_url}")
        except requests.exceptions.ReadTimeout:
            tmp_log.error(f"read timeout when getting data from {info_url}")
        except Exception:
            core_utils.dump_error_message(tmp_log)
        return ret_val is not None, ret_val

    # get json data from an http or https URL; None on failure
    @staticmethod
    def get_url_data(info_url, tmp_log):
        if info_url.startswith("https:"):
            try:
                # try with pandacon certificate
                ca_cert = harvester_config.pandacon.ca_cert
                if ca_cert is False:
                    cert = None
                else:
                    cert = (harvester_config.pandacon.cert_file, harvester_config.pandacon.key_file)
                res = requests.get(info_url, cert=cert, verify=ca_cert, timeout=60)
            except requests.exceptions.SSLError:
                # try without certificate
                res = requests.get(info_url, timeout=60)
        else:
            res = requests.get(info_url, timeout=60)
        if res.status_code != 200:
            tmp_log.error(f"failed to get {info_url} with StatusCode={res.status_code} {res.text}")
            return None
        try:
            return res.json()
        except Exception:
            tmp_log.error(f"corrupted json from {info_url} : {res.text}")
            return None

    # set single mode
    def set_single_mode(self, single_mode):
        self.singleMode = single_mode
