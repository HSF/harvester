from .base_cred_manager import BaseCredManager


# dummy credential manager
class DummyCredManager(BaseCredManager):
    # constructor
    def __init__(self, **kwarg):
        BaseCredManager.__init__(self, **kwarg)

    # check proxy
    def check_credential(self):
        return True

    # renew proxy
    def renew_credential(self):
        return True, ""
