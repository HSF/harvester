"""
File spec class

"""

from .spec_base import SpecBase


class FileSpec(SpecBase):
    # file type
    AUX_INPUT = "aux_input"

    # attributes
    attributesWithTypes = (
        "fileID:integer primary key autoincrement",
        "PandaID:integer / index",
        "taskID:integer",
        "lfn:text / index",
        "status:text / index",
        "fsize:integer",
        "chksum:text",
        "path:text",
        "fileType:text",
        "eventRangeID:text",
        "modificationTime:timestamp",
        "fileAttributes:blob",
        "isZip:integer",
        "zipFileID:integer / index",
        "objstoreID:integer",
        "endpoint:text",
        "groupID:text / index",
        "groupStatus:text / index",
        "groupUpdateTime:timestamp / index",
        "attemptNr:integer",
        "todelete:integer / index",
        "scope:text",
        "pathConvention:integer",
        "provenanceID:text / index",
        "workerID:integer / index",
        "url:text",
    )

    # attributes initialized with 0
    zeroAttrs = ("attemptNr", "todelete")

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
        object.__setattr__(self, "associatedFiles", set())

    # add associated files
    def add_associated_file(self, filespec):
        self.associatedFiles.add(filespec)
