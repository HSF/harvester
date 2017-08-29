"""
File spec class

"""

from spec_base import SpecBase


class FileSpec(SpecBase):
    # attributes
    attributesWithTypes = ('fileID:integer primary key autoincrement',
                           'PandaID:integer',
                           'taskID:integer',
                           'lfn:text',
                           'status:text',
                           'fsize:integer',
                           'chksum:text',
                           'path:text',
                           'fileType:text',
                           'eventRangeID:text',
                           'modificationTime:timestamp',
                           'fileAttributes:blob',
                           'isZip:integer',
                           'zipFileID:integer',
                           'objstoreID:integer',
                           'endpoint:text',
                           'groupID:text',
                           'groupStatus:text',
                           'groupUpdateTime:timestamp',
                           'attemptNr:integer'
                           )

    # attributes initialized with 0
    zeroAttrs = ('attemptNr',
                 )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
        object.__setattr__(self, 'associatedFiles', set())

    # add associated files
    def add_associated_file(self, filespec):
        self.associatedFiles.add(filespec)
