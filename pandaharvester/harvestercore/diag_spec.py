"""
dialog class

"""

import datetime
from .spec_base import SpecBase


class DiagSpec(SpecBase):
    # attributes
    attributesWithTypes = (
        "diagID:integer primary key autoincrement",
        "moduleName:text / index",
        "identifier:text",
        "creationTime:timestamp / index",
        "lockTime:timestamp / index",
        "messageLevel:text",
        "lockedBy:integer / index",
        "diagMessage:varchar(500)",
    )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)

    # convert to propagate
    def convert_to_propagate(self):
        data = dict()
        for attr in ["diagID", "moduleName", "identifier", "creationTime", "messageLevel", "diagMessage"]:
            val = getattr(self, attr)
            if isinstance(val, datetime.datetime):
                val = val.strftime("%Y-%m-%d %H:%M:%S.%f")
            data[attr] = val
        return data
