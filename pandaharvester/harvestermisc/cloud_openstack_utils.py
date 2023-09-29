import json

from keystoneauth1 import loading as loading
from keystoneauth1 import session as session
from novaclient import client as nova_cl

# from cinderclient import client as cinder_cl


class OS_SimpleClient(object):
    def __init__(self, auth_config_json_file=None):
        with open(auth_config_json_file, "r") as _f:
            auth_config_dict = json.load(_f)

        # Openstack API version
        version = "2.0"  # FIXME
        if version == "2.0":
            loader = loading.get_plugin_loader("v2password")
        elif version >= "3.0":
            loader = loading.get_plugin_loader("password")

        auth = loader.load_from_options(**auth_config_dict)
        # sess = keystoneauth1.session.Session(auth=auth)
        sess = session.Session(auth=auth)

        # self.nova = novaclient.client.Client(version, session=sess)
        self.nova = nova_cl.Client(version, session=sess)
        # self.cinder = cinderclient.client.Client(version, session=sess)
        # self.cinder = cinder_cl.client.Client(version, session=sess)
