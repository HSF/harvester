"""
Job spec class

"""

import json
import datetime

from SpecBase import SpecBase

class JobSpec(SpecBase):
    # attributes
    attributesWithTypes = ('PandaID:int primary key',
                           'status:text',
                           'subStatus:text',
                           'currentPriority:int',
                           'computingSite:text',
                           'creationTime:timestamp',
                           'modificationTime:timestamp',
                           'stateChangeTime:timestamp',
                           'jobParams:blob',
                           'propagatorLock:text',
                           'propagatorTime:timestamp',
                           'preparatorLock:text',
                           'preparatorTime:timestamp',
                           )


    # constructor
    def __init__(self):
        SpecBase.__init__(self)



    # convert from Job JSON
    def convertJobJson(self,data):
        self.PandaID = data['PandaID']
        self.currentPriority = data['currentPriority']
        self.jobParams = data



    # trigger propagation
    def triggerPropagation(self):
        self.propagatorTime = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
