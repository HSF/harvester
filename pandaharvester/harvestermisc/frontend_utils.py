import time

import jwt

from pandaharvester.harvesterconfig import harvester_config


class HarvesterToken:
    """
    Methods of JSON Web Token used in harvester frontend
    """

    algorithm = "HS256"

    def __init__(self, **kwarg):
        # load secret from file
        with open(harvester_config.frontend.secretFile) as _f:
            self.secret = _f.read()
        # wheter verify the token
        self.verify = True
        if harvester_config.frontend.verifyToken is False:
            self.verify = False
        # default token lifetime in second. 4 days.
        self.default_lifetime = 345600
        # default payload spec
        self.default_payload_dict = {
            "sub": "Subject",
            "exp": 0,
            "iss": harvester_config.master.harvester_id,
            "iat": 0,
        }

    def generate(self, payload=None, header=None):
        """
        Generate a harvester token.
        Additional payload can be upadated, argument as a dict.
        """
        timestamp_now = int(time.time())
        payload_dict = self.default_payload_dict.copy()
        payload_dict["iat"] = timestamp_now
        payload_dict["exp"] = timestamp_now + self.default_lifetime
        if payload:
            payload_dict.update(payload)
        token = jwt.encode(payload_dict, key=self.secret, algorithm=self.algorithm, headers=header)
        return token

    def get_payload(self, token):
        """
        Decode a harvester token to a python object, typically dict.
        One can set verify=False if other proxy/gateway service already verifies token.
        """
        payload = jwt.decode(token, key=self.secret, algorithms=self.algorithm, verify=self.verify)
        return payload
