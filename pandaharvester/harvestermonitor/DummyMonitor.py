from pandaharvester.harvestercore.WorkSpec import WorkSpec
from MonitorPluginBase import MonitorPluginBase

# dummy monitor
class DummyMonitor (MonitorPluginBase):

    # constructor
    def __init__(self,**kwarg):
        MonitorPluginBase.__init__(self,**kwarg)



    # check workers
    def checkWorkers(self,workSpecs):
        retVal = []
        for workSpec in workSpecs:
            workAttributes = self.messenger.getWorkAttributes(workSpec)
            outputFiles = self.messenger.getOutputFiles(workSpec)
            retVal.append((WorkSpec.ST_finished,'',workAttributes,outputFiles))
        return True,retVal


