from pandaharvester.harvestercore.plugin_base import PluginBase


# dummy credential manager
class DummyCredManager(PluginBase):

    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check proxy
    def check_credential(self):
        return True

    # renew proxy
    def renew_credential(self):
        return True, ''
