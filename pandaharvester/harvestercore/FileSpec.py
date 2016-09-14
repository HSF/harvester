"""
File spec class

"""

import copy
import datetime

from SpecBase import SpecBase

class FileSpec(SpecBase):
    # attributes
    attributesWithTypes = ('fileID:integer primary key',
                           'PandaID:integer',
                           'taskID:integer',
                           'lfn:text',
                           'status:text',
                           'fsize:integer',
                           'path:text',
                           'fileType:text',
                           'eventRangeID:text',
                           'modificationTime:timestamp',
                           'fileAttributes:blob',
                           'isZip:integer',
                           'zipFileID:integer',
                           'objstoreID:integer'
                           )


    # constructor
    def __init__(self):
        SpecBase.__init__(self)
        object.__setattr__(self,'associatedFiles',set())



    # add associated files
    def addAssociatedFile(self,fileSpec):
        self.associatedFiles.add(fileSpec)
