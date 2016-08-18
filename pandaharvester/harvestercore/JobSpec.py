"""
Job spec class

"""

import copy
import datetime

from SpecBase import SpecBase

class JobSpec(SpecBase):

    # has output file
    HO_noOutput    = 0
    HO_hasOutput   = 1
    HO_hasTransfer = 2

    # attributes
    attributesWithTypes = ('PandaID:integer primary key',
                           'taskID:integer',
                           'attemptNr:integer',
                           'status:text',
                           'subStatus:text',
                           'currentPriority:integer',
                           'computingSite:text',
                           'creationTime:timestamp',
                           'modificationTime:timestamp',
                           'stateChangeTime:timestamp',
                           'jobParams:blob',
                           'jobAttributes:blob',
                           'hasOutFile:integer',
                           'metaData:blob',
                           'outputFilesToReport:blob',
                           'lockedBy:text',
                           'propagatorLock:text',
                           'propagatorTime:timestamp',
                           'preparatorTime:timestamp',
                           'submitterTime:timestamp',
                           'stagerLock:text',
                           'stagerTime:timestamp',
                           )


    # constructor
    def __init__(self):
        SpecBase.__init__(self)
        object.__setattr__(self,'outFiles',set())
        object.__setattr__(self,'events',set())



    # add output file
    def addOutFile(self,fileSpec):
        self.outFiles.add(fileSpec)



    # add event
    def addEvent(self,eventSpec):
        self.events.add(eventSpec)



    # convert from Job JSON
    def convertJobJson(self,data):
        self.PandaID = data['PandaID']
        self.taskID = data['taskID']
        self.attemptNr = data['attemptNr']
        self.currentPriority = data['currentPriority']
        self.jobParams = data



    # trigger propagation
    def triggerPropagation(self):
        self.propagatorTime = datetime.datetime.utcnow() - datetime.timedelta(hours=1)




    # set attributes
    def setAttributes(self,attrs):
        if attrs == None:
            return
        attrs = copy.copy(attrs)
        # set metadata and outputs to dedicated attributes
        if 'metadata' in attrs:
            self.metaData = attrs['metadata']
            del attrs['metadata']
        if 'xml' in attrs:
            self.outputFilesToReport = attrs['xml']
            del attrs['xml']
        self.jobAttributes = attrs



    # check if final status
    def isFinalStatus(self):
        return self.status in ['finished','failed','cancelled']



    # get status
    def getStatus(self):
        # don't report the final status while staging-out
        if self.isFinalStatus() and (self.subStatus in ['totransfer','transferring'] or not self.allEventsDone()):
            return 'transferring'
        return self.status



    # check if all events are done
    def allEventsDone(self):
        retVal = True
        for eventSpec in self.events:
            if eventSpec.subStatus != 'done':
                retVal = False
                break
        return retVal



    # all files are triggered to stage-out
    def allFilesTriggeredToStageOut(self):
        for fileSpec in self.outFiles:
            fileSpec.status = 'transferring'
