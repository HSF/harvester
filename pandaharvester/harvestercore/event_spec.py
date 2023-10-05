"""
Event spec class

"""

from past.builtins import long
from future.utils import iteritems

from .spec_base import SpecBase


class EventSpec(SpecBase):
    # attributes
    attributesWithTypes = (
        "eventRangeID:text / index",
        "PandaID:integer / index",
        "eventStatus:text",
        "coreCount:integer",
        "cpuConsumptionTime:integer",
        "subStatus:text / index",
        "fileID:integer",
        "loss:text",
    )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)

    # convert to data
    def to_data(self):
        data = {}
        for attr in self.attributes:
            # ignore some attributes
            if attr not in ["eventRangeID", "eventStatus", "coreCount", "cpuConsumptionTime", "loss"]:
                continue
            val = getattr(self, attr)
            # don't propagate finished until subStatus is finished
            if attr == "eventStatus":
                if val == "finished" and not self.is_final_status():
                    val = "running"
            if val is not None:
                data[attr] = val
        return data

    # convert from data
    def from_data(self, data, panda_id):
        for attr, val in iteritems(data):
            # skip non attributes
            if attr not in self.attributes:
                continue
            setattr(self, attr, val)
        self.PandaID = long(panda_id)

    # final status
    def is_final_status(self):
        return self.subStatus in ["finished", "done", "failed"]
