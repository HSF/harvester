import os
import json
import time
import jwt
from authlib.integrations.requests_client import OAuth2Session
from authlib.oauth2.rfc7523 import PrivateKeyJWT


class SFClient:
    def __init__(self, cred_dir, base_url="https://api.nersc.gov/api/v1.2"):
        # cred_dir is a symlink pointing to the latest versioned cred directory
        self.cred_dir = os.path.abspath(cred_dir)
        self.base_url = base_url.rstrip("/")
        self.token_url = "https://oidc.nersc.gov/c2id/token"
        self._session = None
        self._token = None
        self._token_exp = 0

    def _load_creds(self):
        real_dir = os.path.realpath(self.cred_dir)
        id_path = os.path.join(real_dir, "clientid.txt")
        key_path = os.path.join(real_dir, "priv_key.jwk")
        with open(id_path, "r") as f:
            client_id = f.read().strip()
        with open(key_path, "r") as f:
            private_key = json.load(f)
        return client_id, private_key

    def _fetch_token(self):
        client_id, private_key = self._load_creds()
        self._session = OAuth2Session(
            client_id,
            private_key,
            PrivateKeyJWT(self.token_url),
            grant_type="client_credentials",
            token_endpoint=self.token_url,
        )
        token_response = self._session.fetch_token()
        access_token = token_response.get("access_token")
        if access_token is None:
            raise RuntimeError("Failed to fetch access_token using OAuth2Session")

        decoded = jwt.decode(access_token, options={"verify_signature": False})
        exp = decoded.get("exp")
        if exp is None:
            raise RuntimeError("Fetched token has no 'exp' attribute!.")

        self._token = access_token
        self._token_exp = int(exp)

    def _ensure_token(self):
        now = int(time.time())
        if self._token is None or (now + 30) >= self._token_exp:
            self._fetch_token()

    def _request(self, method, path, **kwargs):
        self._ensure_token()
        url = f"{self.base_url}{path}"
        r = self._session.request(method=method, url=url, **kwargs)
        r.raise_for_status()
        return r

    def get(self, path, **kwargs):
        return self._request("GET", path, **kwargs)

    def post(self, path, **kwargs):
        return self._request("POST", path, **kwargs)

    def delete(self, path, **kwargs):
        return self._request("DELETE", path, **kwargs)
