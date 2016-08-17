"""
File spec class

"""

import copy
import datetime

from SpecBase import SpecBase

class FileSpec(SpecBase):
    # attributes
    attributesWithTypes = ('PandaID:integer',
                           'taskID:integer',
                           'lfn:text',
                           'status:text',
                           'eventRangeID:text',
                           'modificationTime:timestamp',
                           'fileAttributes:blob',
                           )


    # constructor
    def __init__(self):
        SpecBase.__init__(self)
