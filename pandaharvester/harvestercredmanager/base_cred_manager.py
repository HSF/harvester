from pandaharvester.harvestercore.plugin_base import PluginBase


# base credential manager
class BaseCredManager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check credential lifetime for monitoring/alerting purposes
    def check_credential_lifetime(self):
        return

    # check proxy
    def check_credential(self):
        return True

    # renew proxy
    def renew_credential(self):
        return True, ""
