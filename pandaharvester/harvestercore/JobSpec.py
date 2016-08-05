"""
Job spec class

"""

import datetime

from SpecBase import SpecBase

class JobSpec(SpecBase):
    # attributes
    attributesWithTypes = ('PandaID:integer primary key',
                           'taskID:integer',
                           'status:text',
                           'subStatus:text',
                           'currentPriority:integer',
                           'computingSite:text',
                           'creationTime:timestamp',
                           'modificationTime:timestamp',
                           'stateChangeTime:timestamp',
                           'jobParams:blob',
                           'lockedBy:text',
                           'propagatorLock:text',
                           'propagatorTime:timestamp',
                           'preparatorTime:timestamp',
                           'submitterTime:timestamp',
                           )


    # constructor
    def __init__(self):
        SpecBase.__init__(self)



    # convert from Job JSON
    def convertJobJson(self,data):
        self.PandaID = data['PandaID']
        self.taskID = data['taskID']
        self.currentPriority = data['currentPriority']
        self.jobParams = data



    # trigger propagation
    def triggerPropagation(self):
        self.propagatorTime = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
