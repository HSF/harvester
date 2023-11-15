import json
import traceback

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.info_utils_k8s import PandaQueuesDictK8s
from pandaharvester.harvestermisc.k8s_utils import k8s_Client

from .base_cred_manager import BaseCredManager

# logger
_logger = core_utils.setup_logger("k8s_secret_cred_manager")


# credential manager with k8s secret
class K8sSecretCredManager(BaseCredManager):
    # constructor
    def __init__(self, **kwarg):
        BaseCredManager.__init__(self, **kwarg)
        # make logger
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
            self.k8s_config_file = self.setupMap["k8s_config_file"]
            self.proxy_files = self.setupMap["proxy_files"]
            self.secret_name = self.setupMap.get("secret_name", "proxy-secret")
        except KeyError as e:
            tmp_log.error(f"Missing attributes in setup. {e.__class__.__name__}: {e}")
            raise

        try:
            # retrieve the k8s namespace from CRIC
            self.panda_queues_dict = PandaQueuesDictK8s()
            self.namespace = self.panda_queues_dict.get_k8s_namespace(self.queueName)
            # k8s client
            self.k8s_client = k8s_Client(namespace=self.namespace, queue_name=self.queueName, config_file=self.k8s_config_file)
        except Exception as e:
            tmp_log.error(f"Problem instantiating k8s client for {self.k8s_config_file}. {traceback.format_exc()}")
            raise

    # check proxy
    def check_credential(self):
        # make logger
        # same update period as credmanager agent
        return False

    # renew proxy
    def renew_credential(self):
        # make logger
        tmp_log = self.make_logger(_logger, f"queueName={self.queueName}", method_name="renew_credential")
        # go
        try:
            rsp = self.k8s_client.create_or_patch_secret(file_list=self.proxy_files, secret_name=self.secret_name)
            tmp_log.debug("done")
        except KeyError as e:
            errStr = f"Error when renew proxy secret . {e.__class__.__name__}: {e}"
            return False, errStr
        else:
            return True, ""
