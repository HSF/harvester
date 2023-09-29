"""
Cache class

"""
from .spec_base import SpecBase


class CacheSpec(SpecBase):
    # attributes
    attributesWithTypes = ("mainKey:text", "subKey:text", "data:blob", "lastUpdate:timestamp")

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
