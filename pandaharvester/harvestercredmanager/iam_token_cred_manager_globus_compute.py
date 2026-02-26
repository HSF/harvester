import os
import traceback

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.token_utils import endpoint_to_filename
from pandaharvester.harvestermisc.globus_compute_token_sync import GlobusComputeTokenReplicator

from .iam_token_cred_manager import IamTokenCredManager

_logger = core_utils.setup_logger("iam_token_cred_manager_gc")

class IamTokenCredManagerRemoteGlobusCompute(IamTokenCredManager):
    def __init__(self, **kwarg):
        super().__init__(**kwarg)
        tmp_log = self.make_logger(_logger, f"config={self.setup_name}", method_name="__init__")

        self.remote_out_dir = self.setupMap.get("remote_out_dir", "")
        gc_cfg = self.setupMap.get("globus_compute", {})
        if gc_cfg and self.remote_out_dir:
            self.replicator = GlobusComputeTokenReplicator(gc_cfg, tmp_log)
        else:
            tmp_log.debug(f"replicator is not initialized as either gc_cfg or remote_out_dir is missing. They are gc_cfg = {gc_cfg} and remote_out_dir = {self.remote_out_dir}")
            self.replicator = None

    # If not specify remote token replicator, then do nothing
    # First get the final remote token filepath
    # Then inside token replicator, encrypt the token locally as a string
    # Next within token replicator submit a task to transfer the token to a tmp place
    # Inside that task, next decrypt the token and atomically replace the old remote token file
    def _sync_remote(self, token_filename: str, token_str: str, logger):
        if not self.replicator:
            return
        remote_token_path = os.path.join(self.remote_out_dir, token_filename)
        status = self.replicator.do_it(token_str, remote_token_path)
        if status:
            logger.info(f"Synchronized token '{token_filename}' to remote: {remote_token_path}")
        else:
            logger.error(f"FAILED to synchronize token '{token_filename}' to remote: {remote_token_path}")

    def renew_credential(self):
        # make logger
        tmp_log = self.make_logger(_logger, f"config={self.setup_name}", method_name="renew_credential")
        # go
        all_ok = True
        all_err_str = ""
        for target in self.targets_dict:
            try:
                # write to file
                if self.target_type == "panda":
                    token_filename = self.panda_token_filename
                else:
                    token_filename = endpoint_to_filename(target)
                token_path = os.path.join(self.out_dir, token_filename)
                # check token freshness
                if self._is_fresh(token_path):
                    # token still fresh, skip it
                    tmp_log.debug(f"token for {target} at {token_path} still fresh; skipped")
                    continue
                # renew access token of target locally
                access_token = self.issuer_broker.get_access_token(aud=target, scope=self.scope)
                with open(token_path, "w") as f:
                    f.write(access_token)
                tmp_log.info(f"renewed token for {target} at {token_path}")
                # encrypt, and sync to remote (if configured) per token file mapping, and then decrypt remotely
                try:
                    self._sync_remote(token_filename, access_token, tmp_log)
                except Exception:
                    all_ok = False
                    tmp_log.error(f"Remote sync failed for {target}. {traceback.format_exc()}")
                    all_err_str = "failed to sync some tokens; see plugin log for details "

            except Exception as e:
                err_str = f"Problem getting token for {target}. {traceback.format_exc()}"
                tmp_log.error(err_str)
                all_ok = False
                all_err_str = "failed to get some tokens. Check the plugin log for details "
                continue
        # update last timestamp
        self._update_ts()
        tmp_log.debug("done")
        # return
        return all_ok, all_err_str
