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
            workStatus = None
            # job-level late binding
            if workSpec.hasJob == 0:
                # check if job is requested
                jobRequested = self.messenger.jobRequested(workSpec)
                if jobRequested:
                    # set ready when job is requested 
                    workStatus = WorkSpec.ST_ready
                else:
                    workStatus = workSpec.status
                workAttributes = None
                outputFiles = None
            else:
                # get work attributes and output files
                workAttributes = self.messenger.getWorkAttributes(workSpec)
                outputFiles = self.messenger.getFilesToStageOut(workSpec)
            # go to finished
            if workStatus == None:
                workStatus = WorkSpec.ST_finished
            retVal.append((workStatus,'',workAttributes,outputFiles))
        return True,retVal


