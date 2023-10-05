import copy
import hashlib
import json
import time

import six
import requests


def _md5sum(data):
    """
    get md5sum hexadecimal string of data
    """
    hash = hashlib.md5()
    hash.update(six.b(data))
    hash_hex = hash.hexdigest()
    return hash_hex


def endpoint_to_filename(endpoint):
    """
    get token file name according to service (CE, storage, etc.) endpoint
    currently directly take its md5sum hash as file name
    """
    filename = _md5sum(endpoint)
    return filename


class WLCG_scopes(object):
    COMPUTE_ALL = "compute.read compute.modify compute.create compute.cancel"


class IssuerBroker(object):
    """
    Talk to token issuer with client credentials flow
    """

    def __init__(self, issuer, client_id, client_secret, name="unknown"):
        self.issuer = issuer
        self.client_id = client_id
        self.client_secret = client_secret
        self.name = name
        self.timeout = 3
        # derived attributes
        self.token_request_url = "{0}/token".format(self.issuer)
        self._base_post_data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

    def _post(self, **kwarg):
        data_dict = copy.deepcopy(self._base_post_data)
        data_dict.update(kwarg)
        data = copy.deepcopy(data_dict)
        resp = requests.post(self.token_request_url, data=data, timeout=self.timeout)
        return resp

    def get_access_token(self, aud=None, scope=None):
        resp = self._post(audience=aud, scope=scope)
        if resp.status_code == requests.codes.ok:
            try:
                resp_dict = json.loads(resp.text)
            except Exception as e:
                raise
            token = resp_dict["access_token"]
            return token
        else:
            resp.raise_for_status()
