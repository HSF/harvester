import os
from datetime import datetime, timedelta

from pandaharvester.harvestercore import core_utils

from .base_cred_manager import BaseCredManager

# logger
_logger = core_utils.setup_logger("superfacility_cred_manager")


class SuperfacilityCredManager(BaseCredManager):
    # constructor
    def __init__(self, **kwarg):
        BaseCredManager.__init__(self, **kwarg)
        # make logger
        main_log = self.make_logger(_logger, method_name="__init__")
        # set up with direct attributes
        self.setupMap = dict(vars(self))
        # setupMap
        self.cred_dir = self.setupMap.get("cred_dir")
        self.lifetime_days = int(self.setupMap.get("lifetime_days", 30))
        self.date_format = "%Y-%m-%d-%H%M"

    # check proxy lifetime for monitoring/alerting purposes
    def check_credential_lifetime(self):
        main_log = self.make_logger(_logger, method_name="check_credential_lifetime")
        lifetime = None
        try:
            real_dir = os.path.realpath(self.cred_dir)
            basename = os.path.basename(real_dir)
            created_dt = datetime.strptime(basename, self.date_format)
            age = datetime.now() - created_dt
            lifetime_all = timedelta(days=self.lifetime_days)
            main_log.debug(f"age={age}, lifetime_all={lifetime_all}")
            timediff = lifetime_all - age
            lifetime = timediff.days * 24 + timediff.seconds / 3600
        except Exception:
            core_utils.dump_error_message(main_log)
        if isinstance(lifetime, float):
            main_log.debug(f"returning lifetime (in hour) {lifetime:.3f}")
        else:
            main_log.debug(f"returning lifetime (in hour) {lifetime}")
        return lifetime

    def check_credential(self):
        # make logger
        main_log = self.make_logger(_logger, method_name="check_credential")
        try:
            real_dir = os.path.realpath(self.cred_dir)
            basename = os.path.basename(real_dir)
            created_dt = datetime.strptime(basename, self.date_format)
            age = datetime.now() - created_dt
            lifetime_all = timedelta(days=self.lifetime_days)
            main_log.debug(f"age={age}, lifetime_all={lifetime_all}")
            return age <= lifetime_all
        except Exception as e:
            main_log.debug(f"[SuperfacilityCredManager] check_credential func call with error: {e}")
            core_utils.dump_error_message(main_log)
            return False

    def renew_credential(self):
        # Does not support automatic cred updates
        # If this method is called, should return error immediately
        real_dir = os.path.realpath(self.cred_dir)
        msg = f"SuperfacilityClient credentials expired or invalid: symlink {self.cred_dir} points to {real_dir} which is older than {self.lifetime_days} days. "
        return False, msg
