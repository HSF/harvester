"""
sequential number class

"""

from .spec_base import SpecBase


class SeqNumberSpec(SpecBase):
    # attributes
    attributesWithTypes = (
        "numberName:text",
        "curVal:integer",
    )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
