import json

import keystoneauth1
import novaclient
import cinderclient


class OS_SimpleClient(object):
    def __init__(self, auth_config_json_file=None):
        with open auth_config_json_file as _f:
            auth_config_dict = json.load(_f)

        # Openstack API version
        version = '2.0'
        if version == '2.0':
            loader = keystoneauth1.loading.get_plugin_loader('v2password')
        elif version >= '3.0':
            loader = keystoneauth1.loading.get_plugin_loader('password')

        auth = loader.load_from_options(**auth_config_dict)
        sess = keystoneauth1.session.Session(auth=auth)

        self.nova = novaclient.client.Client(version, session=sess)
        self.cinder = cinderclient.client.Client(version, session=sess)
