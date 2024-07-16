import datetime
import os.path
import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool

from .base_cred_manager import BaseCredManager

# logger
_logger = core_utils.setup_logger("token_key_cred_manager")


# credential manager for token key
class TokenKeyCredManager(BaseCredManager):
    # constructor
    def __init__(self, **kwarg):
        BaseCredManager.__init__(self, **kwarg)
        # set default
        if not hasattr(self, "refresh_interval"):
            self.refresh_interval = 60

    # check token key
    def check_credential(self):
        # make logger
        main_log = self.make_logger(_logger, method_name="check_credential")
        main_log.debug("start")
        try:
            # check if the key file exists
            if not os.path.exists(self.token_key_file):
                main_log.debug(f"token key file {self.token_key_file} not found")
                return False
            # check if the key file is fresh
            mod_time = datetime.datetime.fromtimestamp(os.stat(self.token_key_file).st_mtime, datetime.timezone.utc)
            if datetime.datetime.now(datetime.timezone.utc) - mod_time < datetime.timedelta(minutes=self.refresh_interval):
                main_log.debug(f"token key {self.token_key_file} is fresh")
                return True
            main_log.debug(f"token key {self.token_key_file} is outdated")
        except Exception:
            core_utils.dump_error_message(main_log)
        return False

    # renew proxy
    def renew_credential(self):
        # make logger
        main_log = self.make_logger(_logger, method_name="renew_credential")
        main_log.debug("start")
        # make communication channel to PanDA
        com = CommunicatorPool()
        token_key, msg = com.get_token_key(self.client_name)
        if token_key:
            is_ok = True
            with open(self.token_key_file, "w") as f:
                f.write(token_key)
            main_log.debug(f"got new token key in {self.token_key_file}")
        else:
            is_ok = False
            main_log.error(f"failed to renew token key with a server message : {msg}")
        return is_ok, msg
