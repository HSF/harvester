"""
Work spec class

"""

from SpecBase import SpecBase


# protocols for access point
AP_file = 'file://'

# work spec
class WorkSpec(SpecBase):
    # attributes
    attributesWithTypes = ('workerID:integer primary key',
                           'batchID:text',
                           'queueName:text',
                           'status:text',
                           'workParams:blob',
                           'computingSite:text',
                           'creationTime:timestamp',
                           'submitTime:timestamp',
                           'startTime:timestamp',
                           'endTime:timestamp',
                           'nCore:integer',
                           'walltime:timestamp',
                           'accessPoint:text',
                           'modificationTime:timestamp',
                           'stateChangeTime:timestamp',
                           )


    # constructor
    def __init__(self):
        SpecBase.__init__(self)



    # set file access point
    def setDiskAccessPoint(self,dirName):
        self.accessPoint = 'file:/{0}'.format(dirName)

