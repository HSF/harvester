import os
import traceback
from typing import Dict, Any

from globus_compute_sdk import Executor, Client
from globus_compute_sdk.errors.error_types import TaskExecutionFailed
from globus_compute_sdk.sdk.shell_function import ShellFunction, ShellResult

from cryptography.fernet import Fernet

from pandaharvester.harvestercore import core_utils

def _remote_write_token(encrypted_token: str, remote_token_path: str, key_file: str) -> str:
    """
    Remote function executed on the Globus Compute endpoint (HPC site, for example).
    It first reads the key from key_file, then decrypts encrypted_token based on key file
    Finally writes the plaintext to remote_token_path atomically (via tmp + os.replace)
    """
    import os
    from cryptography.fernet import Fernet

    with open(key_file, "rb") as f:
        key = f.read().strip()
    fernet = Fernet(key)
    plaintext = fernet.decrypt(encrypted_token.encode("utf-8"))

    dirname = os.path.dirname(remote_token_path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    tmp_path = remote_token_path + ".tmp"

    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as fp:
        fp.write(plaintext)
        fp.flush()
        os.fsync(fp.fileno())
    os.replace(tmp_path, remote_token_path)

    return remote_token_path


class GlobusComputeTokenReplicator:
    """
    Wrapper over globus compute client that is used by IamTokenCredManagerRemoteGlobusCompute to sync tokens with a remote site (like HPC).
    It first encrypts the token on local harvester machine using Fernet, 
    then submits a Globus Compute task which decrypts and writes the token file on the remote site.
    """

    def __init__(self, gc_cfg: Dict[str, Any], logger):
        """
        An example of gc_cfg is expected to look like:
        {
          "endpoint_id": "<UUID of GC endpoint>",
          "local_key_file": "/path/on/harvester/harvester_gc_token.key",
          "remote_key_file": "/path/on/endpoint/harvester_gc_token.key",
          "task_timeout": 120
        }
        The first three items are mandatory!
        """
        self.logger = logger

        try:
            self.endpoint_id = gc_cfg["endpoint_id"]
            self.local_key_file = gc_cfg["local_key_file"]
            self.remote_key_file = gc_cfg.get("remote_key_file", self.local_key_file)
        except KeyError as e:
            raise RuntimeError(f"GlobusComputeTokenReplicator missing required config key: {e}") from e

        self.executor = Executor(endpoint_id=self.endpoint_id)
        self.task_timeout = gc_cfg.get("task_timeout", 120)

        try:
            with open(self.local_key_file, "rb") as f:
                key = f.read().strip()
            self.fernet = Fernet(key)
        except Exception:
            self.logger.error(f"Failed to load local Fernet key from {self.local_key_file}\n{traceback.format_exc()}")
            raise

    def do_it(self, token_str: str, remote_token_path: str) -> bool:
        encrypted = self.fernet.encrypt(token_str.encode("utf-8")).decode("ascii")

        try:
            future = self.executor.submit(
                _remote_write_token, encrypted, remote_token_path, self.remote_key_file
            )
            result = future.result(timeout=self.task_timeout)
            self.logger.debug(f"Remote token sync to {remote_token_path} finished with result: {result}")
            return True
        except TaskExecutionFailed as e:
            self.logger.error(f"Globus Compute task failed for {remote_token_path}: {e}")
            return False
        except Exception:
            self.logger.error(f"Unexpected error during remote token sync for {remote_token_path}\n{traceback.format_exc()}")
            return False
