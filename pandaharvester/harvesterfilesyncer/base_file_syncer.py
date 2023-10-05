from pandaharvester.harvestercore.plugin_base import PluginBase


# base file syncer
class BaseFileSyncer(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check whether to update
    def check(self):
        return True

    # update files
    def update(self):
        return True, ""
