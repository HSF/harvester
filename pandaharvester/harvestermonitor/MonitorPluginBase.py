from pandaharvester.harvestercore.PluginBase import PluginBase


# base class for monitor plugins
class MonitorPluginBase (PluginBase):
    
    # constructor
    def __init__(self,**kwarg):
        PluginBase.__init__(self,**kwarg)



    # set messenger
    def setMessenger(self,messenger):
        self.messenger = messenger

