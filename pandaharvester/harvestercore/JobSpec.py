"""
Job spec class

"""

import json

from SpecBase import SpecBase

class JobSpec(SpecBase):
    # attributes
    attributesWithTypes = ('PandaID:int',
                           'status:text',
                           'subStatus:text',
                           'currentPriority:int',
                           'computingSite:text',
                           'creationTime:timestamp',
                           'modificationTime:timestamp',
                           'stateChangeTime:timestamp',
                           'jobParams:blob'
                           )


    # constructor
    def __init__(self):
        SpecBase.__init__(self)



    # convert from Job JSON
    def convertJobJson(self,data):
        self.PandaID = data['PandaID']
        self.currentPriority = data['currentPriority']
        self.jobParams = data
