import uuid

from pandaharvester.harvestercore.PluginBase import PluginBase


# dummy submitter
class DummySubmitter (PluginBase):

    # constructor
    def __init__(self,**kwarg):
        PluginBase.__init__(self,**kwarg)



    # submit workers
    def submitWorkers(self,workSpecs):
        retList = []
        retStrList = []
        for workSpec in workSpecs:
            workSpec.batchID = uuid.uuid4().hex
            retList.append((True,''))
        return retList

