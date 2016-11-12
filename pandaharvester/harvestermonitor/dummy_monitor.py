import os.path
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase


# dummy monitor
class DummyMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check workers
    def check_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            dummyFilePath = os.path.join(workSpec.get_access_point(), 'status.txt')
            newStatus = WorkSpec.ST_finished
            with open(dummyFilePath) as dummyFile:
                newStatus = dummyFile.readline()
                newStatus = newStatus.strip()
            retList.append((newStatus, ''))
        return True, retList
