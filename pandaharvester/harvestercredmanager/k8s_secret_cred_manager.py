import os
import time
import json

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.k8s_utils import k8s_Client


# logger
_logger = core_utils.setup_logger('k8s_secret_cred_manager')


# credential manager with k8s secret
class K8sSecretCredManager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        # make logger
        mainLog = self.make_logger(_logger, method_name='__init__')
        # attributes
        try:
            self.inFile
        except AttributeError:
            self.inFile = self.inCertFile
        # parse inFile setup conficuration
        try:
            with open(self.inFile) as f:
                self.setupMap = json.load(f)
        except Exception as e:
            mainLog.error('Error with inFile/inCertFile . {0}: {1}'.format(
                                e.__class__.__name__, e))
            self.setupMap = {}
            raise
        # validate setupMap
        try:
            self.k8s_namespace = self.setupMap['k8s_namespace']
            self.k8s_config_file = self.setupMap['k8s_config_file']
            self.proxy_file_list = self.setupMap['proxy_files']
            self.secret_name = self.setupMap.get('secret_name', 'proxy-secret')
        except KeyError as e:
            mainLog.error('Missing setup in inFile/inCertFile . {0}: {1}'.format(
                                e.__class__.__name__, e))
            raise
        # k8s client
        self.k8s_client = k8s_Client(self.k8s_namespace, _logger, config_file=self.k8s_config_file)

    # check proxy
    def check_credential(self):
        # make logger
        mainLog = self.make_logger(_logger, method_name='check_credential')
        # same update period as credmanager agent
        return False

    # renew proxy
    def renew_credential(self):
        # make logger
        mainLog = self.make_logger(_logger, method_name='renew_credential')
        # go
        try:
            rsp = self.k8s_client.create_or_patch_secret(
                            file_list=self.proxy_file_list, secret_name=self.secret_name)
            mainLog.debug('done')
        except KeyError as e:
            errStr = 'Error when renew proxy secret . {0}: {1}'.format(
                                e.__class__.__name__, e)
            return (False, errStr)
        else:
            return (True, '')
