"""
Process lock class

"""

from .spec_base import SpecBase


class ProcessLockSpec(SpecBase):
    # attributes
    attributesWithTypes = ("processName:text primary key", "lockedBy:text", "lockTime:timestamp")

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
