import os
import json
import re
import time
import traceback

from .base_cred_manager import BaseCredManager
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestermisc.token_utils import endpoint_to_filename, WLCG_scopes, IssuerBroker

# logger
_logger = core_utils.setup_logger("iam_token_cred_manager")

# allowed target types
ALL_TARGET_TYPES = ["common", "ce"]

# default port for CEs
default_port_map = {
    "htcondor-ce": 9619,
}

# credential manager with IAM token


class IamTokenCredManager(BaseCredManager):
    # constructor
    def __init__(self, **kwarg):
        BaseCredManager.__init__(self, **kwarg)
        # make logger
        tmp_log = self.make_logger(_logger, "config={0}".format(self.setup_name), method_name="__init__")
        # attributes
        if hasattr(self, "inFile"):
            # parse inFile setup configuration
            try:
                with open(self.inFile) as f:
                    self.setupMap = json.load(f)
            except Exception as e:
                tmp_log.error("Error with inFile. {0}: {1}".format(e.__class__.__name__, e))
                self.setupMap = {}
                raise
        else:
            # set up with direct attributes
            self.setupMap = dict(vars(self))
        # validate setupMap
        try:
            self.client_cred_file = self.setupMap["client_cred_file"]
            with open(self.client_cred_file) as f:
                client_cred_dict = json.load(f)
                self.issuer = client_cred_dict["issuer"]
                self.client_id = client_cred_dict["client_id"]
                self.client_secret = client_cred_dict["client_secret"]
            self.target_type = self.setupMap["target_type"]
            self.out_dir = self.setupMap["out_dir"]
            self.lifetime = self.setupMap.get("lifetime", 14 * 24 * 60 * 60)
            self.target_list = self.setupMap.get("target_list")
            self.target_list_file = self.setupMap.get("target_list_file")
            self.update_ts_path = self.setupMap.get("update_ts_path", os.path.join(self.out_dir, "_last_update"))
            self.check_interval = self.setupMap.get("check_interval", 300)
            self.refresh_interval = self.setupMap.get("refresh_interval", 3600)
        except KeyError as e:
            tmp_log.error("Missing attributes in setup. {0}".format(traceback.format_exc()))
            raise
        else:
            if self.target_type not in ALL_TARGET_TYPES:
                tmp_log.error("Unsupported target_type: {0}".format(self.target_type))
                raise Exception("Unsupported target_type")
        # initialize
        self.targets_dict = dict()
        # handle targets
        self._handle_target_types()
        # issuer broker
        self.issuer_broker = IssuerBroker(self.issuer, self.client_id, self.client_secret, name=self.setup_name)

    def _is_updated(self):
        now_time = time.time()
        ret = False
        if os.path.isfile(self.update_ts_path) and now_time - os.path.getmtime(self.update_ts_path) < self.check_interval:
            ret = True
        return ret

    def _is_fresh(self, token_path):
        now_time = time.time()
        ret = False
        if os.path.isfile(token_path) and os.path.getsize(token_path) > 0 and now_time - os.path.getmtime(token_path) < self.refresh_interval:
            ret = True
        return ret

    def _update_ts(self):
        tmp_log = self.make_logger(_logger, "config={0}".format(self.setup_name), method_name="_update_ts")
        with open(self.update_ts_path, "w") as f:
            f.write(str(self.out_dir))
        tmp_log.debug("updated timestamp file {0}".format(self.update_ts_path))

    def _clean_up(self):
        tmp_log = self.make_logger(_logger, "config={0}".format(self.setup_name), method_name="_clean_up")
        now_time = time.time()
        for filename in os.listdir(self.out_dir):
            file_path = os.path.join(self.out_dir, filename)
            if now_time - os.path.getmtime(file_path) > self.lifetime:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    tmp_log.debug("deleted old token file {0}".format(file_path))

    def _handle_target_types(self):
        # make logger
        tmp_log = self.make_logger(_logger, "config={0}".format(self.setup_name), method_name="_handle_target_types")
        try:
            self.panda_queues_dict = PandaQueuesDict()
        except Exception as e:
            tmp_log.error("Problem calling PandaQueuesDict. {0}".format(traceback.format_exc()))
            raise
        if self.target_type == "common":
            if not self.target_list:
                pass
            else:
                for target in self.target_list:
                    self.targets_dict[target] = {}
                # scope
                self.scope = ""
        elif self.target_type == "ce":
            try:
                # retrieve CEs from CRIC
                for site, val in self.panda_queues_dict.items():
                    if val.get("status") == "offline":
                        # do not generate token for offline PQs, but for online, brokeroff, pause, ...
                        continue
                    ce_q_list = val.get("queues")
                    if ce_q_list:
                        # loop over all ce queues
                        for ce_q in ce_q_list:
                            # ce_status = ce_q.get('ce_status')
                            # if not ce_status or ce_status == 'DISABLED':
                            #     # skip disabled ce queues
                            #     continue
                            ce_endpoint = ce_q.get("ce_endpoint")
                            ce_hostname = re.sub(":\w*", "", ce_endpoint)
                            ce_flavour = ce_q.get("ce_flavour")
                            ce_flavour_str = str(ce_flavour).lower()
                            ce_endpoint_modified = ce_endpoint
                            if ce_endpoint == ce_hostname:
                                # no port, add default port
                                if ce_flavour_str in default_port_map:
                                    default_port = default_port_map[ce_flavour_str]
                                    ce_endpoint_modified = "{0}:{1}".format(ce_hostname, default_port)
                            if ce_endpoint_modified and ce_flavour:
                                target_attr_dict = {
                                    "ce_flavour": ce_flavour,
                                }
                                self.targets_dict[ce_endpoint_modified] = target_attr_dict
                    else:
                        # do not generate token if no queues of CE
                        continue
            except Exception as e:
                tmp_log.error("Problem retrieving CEs from CRIC. {0}".format(traceback.format_exc()))
                raise
            # retrieve CEs from local file
            if self.target_list_file:
                try:
                    with open(self.target_list_file) as f:
                        for target_str in f.readlines():
                            if target_str:
                                target = target_str.rstrip()
                                target_attr_dict = {
                                    "ce_flavour": None,
                                }
                                self.targets_dict[target] = target_attr_dict
                except Exception as e:
                    tmp_log.error("Problem retrieving CEs from local file. {0}".format(traceback.format_exc()))
                    raise
            # scope for CE
            self.scope = WLCG_scopes.COMPUTE_ALL

    # check proxy
    def check_credential(self):
        # make logger
        tmp_log = self.make_logger(_logger, "config={0}".format(self.setup_name), method_name="check_credential")
        # clean up
        self._clean_up()
        # same update period as credmanager agent
        is_fresh = self._is_updated()
        if is_fresh:
            tmp_log.debug("last renewal is still recent, skipped")
        else:
            tmp_log.debug("to renew tokens")
        return is_fresh

    # renew proxy
    def renew_credential(self):
        # make logger
        tmp_log = self.make_logger(_logger, "config={0}".format(self.setup_name), method_name="renew_credential")
        # go
        all_ok = True
        all_err_str = ""
        for target in self.targets_dict:
            try:
                # write to file
                token_filename = endpoint_to_filename(target)
                token_path = os.path.join(self.out_dir, token_filename)
                # check token freshness
                if self._is_fresh(token_path):
                    # token still fresh, skip it
                    tmp_log.debug("token for {0} at {1} still fresh; skipped".format(target, token_path))
                else:
                    # renew access token of target
                    access_token = self.issuer_broker.get_access_token(aud=target, scope=self.scope)
                    with open(token_path, "w") as f:
                        f.write(access_token)
                    tmp_log.info("renewed token for {0} at {1}".format(target, token_path))
            except Exception as e:
                err_str = "Problem getting token for {0}. {1}".format(target, traceback.format_exc())
                tmp_log.error(err_str)
                all_ok = False
                all_err_str = "failed to get some tokens. Check the plugin log for details "
                continue
        # update last timestamp
        self._update_ts()
        tmp_log.debug("done")
        # return
        return all_ok, all_err_str
