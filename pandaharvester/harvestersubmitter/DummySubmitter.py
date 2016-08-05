import uuid

from pandaharvester.harvestercore.PluginBase import PluginBase


# dummy submitter
class DummySubmitter (PluginBase):

    # constructor
    def __init__(self,**kwarg):
        PluginBase.__init__(self,**kwarg)



    # submit workers
    def submitWorkers(self,workerList):
        retList = []
        retStrList = []
        for worker in workerList:
            worker.batchID = uuid.uuid4().hex
            retList.append(True)
            retStrList.append('')
        return retList,retStrList

