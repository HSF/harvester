import os.path
from pandaharvester.harvestercore.WorkSpec import WorkSpec
from pandaharvester.harvestercore.PluginBase import PluginBase


# dummy monitor
class DummyMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check workers
    def checkWorkers(self, workSpecs):
        retList = []
        for workSpec in workSpecs:
            dummyFilePath = os.path.join(workSpec.getAccessPoint(), 'status.txt')
            newStatus = WorkSpec.ST_finished
            with open(dummyFilePath) as dummyFile:
                newStatus = dummyFile.readline()
                newStatus = newStatus.strip()
            retList.append((newStatus, ''))
        return True, retList
