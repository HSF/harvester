from pandaharvester.harvestercore.PluginBase import PluginBase


# dummy plugin for preparator
class DummyPreparator (PluginBase):
    
    # constructor
    def __init__(self,**kwarg):
        PluginBase.__init__(self,**kwarg)


    # check status
    def checkStatus(self,jobSpec):
        return True,''



    # trigger preparation
    def triggerPreparation(self,jobSpec):
        return True,''

