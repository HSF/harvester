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
                           'jobAttributes:blob',
                           'outputFiles:blob',
                           'stagedFiles:blob',
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




    # set attributes
    def setAttributes(self,attrs):
        self.jobAttributes = attrs



    # set outputs
    def setOutputs(self,outputFiles):
        # append new output files
        if self.outputFiles == None:
            self.outputFiles = outputFiles
        else:
            for tmpLFN,fileVar in outputFiles:
                if not tmpLFN in self.outputFiles and not tmpLFN in self.stagedFiles:
                    self.outputFiles[tmpLFN] = fileVar
        # remove staged filesremove 
        if self.outputFiles != None and self.stagedFiles != None:
            for tmpLFN in self.outputFiles.keys():
                if tmpLFN in self.stagedFiles:
                    del self.outputFiles[tmpLFN]
        # flag the attribute
        self.forceUpdate('outputFiles')
