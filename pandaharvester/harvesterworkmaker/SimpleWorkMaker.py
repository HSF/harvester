import datetime

from pandaharvester.harvestercore.WorkSpec import WorkSpec
from pandaharvester.harvestercore.PluginBase import PluginBase

# simple maker
class SimpleWorkMaker (PluginBase):

    # constructor
    def __init__(self,**kwarg):
        PluginBase.__init__(self,**kwarg)



    # make a worker from a job with a disk access point
    def makeWorker(self,jobChunk,queueConifg):
        jobSpec = jobChunk[0]
        workSpec = WorkSpec()
        workSpec.computingSite = queueConifg.queueName
        workSpec.creationTime = datetime.datetime.utcnow()
        workSpec.setDiskAccessPoint(queueConifg.workMaker['workDir'])
        workSpec.nCore = jobSpec.jobParams['coreCount']
        return workSpec
