import json
import os
import socket
import traceback

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestermisc.lancium_utils import SECRETS_PATH, LanciumClient

from .base_cred_manager import BaseCredManager

# logger
_logger = core_utils.setup_logger("lancium_cred_manager")


# upload cred to Lancium periodically
class LanciumCredManager(BaseCredManager):
    def __init__(self, **kwarg):
        self.hostname = socket.getfqdn()
        BaseCredManager.__init__(self, **kwarg)

        tmp_log = self.make_logger(_logger, method_name="__init__")

        # attributes
        if hasattr(self, "inFile") or hasattr(self, "inCertFile"):
            # set up with json in inFile
            try:
                self.inFile
            except AttributeError:
                self.inFile = self.inCertFile
            # parse inFile setup configuration
            try:
                with open(self.inFile) as f:
                    self.setupMap = json.load(f)
            except Exception as e:
                tmp_log.error(f"Error with inFile/inCertFile . {e.__class__.__name__}: {e}")
                self.setupMap = {}
                raise
        else:
            # set up with direct attributes
            self.setupMap = dict(vars(self))
        # validate setupMap
        try:
            self.proxy_files = self.setupMap["proxy_files"]
            self.secret_name = self.setupMap.get("secret_name", "proxy-secret")
        except KeyError as e:
            tmp_log.error(f"Missing attributes in setup. {e.__class__.__name__}: {e}")
            raise

        try:
            self.panda_queues_dict = PandaQueuesDict()
            self.lancium_client = LanciumClient(self.hostname, queue_name=self.queueName)
        except Exception as e:
            tmp_log.error("Problem instantiating lancium client. {1}".format(traceback.format_exc()))
            raise

    # check proxy
    def check_credential(self):
        # same update period as credmanager agent
        return False

    def upload_proxies(self, proxy_files):
        tmp_log = self.make_logger(_logger, method_name="upload_proxies")

        tmp_log.debug("Start uploading proxies")
        for local_file in proxy_files:
            try:
                tmp_log.debug(f"Uploading proxy {local_file}...")
                base_name = os.path.basename(local_file)
                lancium_file = os.path.join(SECRETS_PATH, base_name)
                self.lancium_client.upload_file(local_file, lancium_file)
            except Exception:
                tmp_log.error(f"Problem uploading proxy {local_file}. {traceback.format_exc()}")

        tmp_log.debug("Done uploading proxies")

    # renew proxy
    def renew_credential(self):
        tmp_log = self.make_logger(_logger, f"queueName={self.queueName}", method_name="renew_credential")

        try:
            self.upload_proxies(self.proxy_files)
            tmp_log.debug("done")
        except KeyError as e:
            err_str = f"Error renewing proxy secret. {e.__class__.__name__}: {e}"
            return False, err_str
        else:
            return True, ""
