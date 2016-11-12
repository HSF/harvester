"""
Event spec class

"""

from spec_base import SpecBase


class EventSpec(SpecBase):
    # attributes
    attributesWithTypes = ('eventRangeID:text',
                           'PandaID:integer',
                           'eventStatus:text',
                           'coreCount:integer',
                           'cpuConsumptionTime:integer',
                           'subStatus:text',
                           'fileID:integer'
                           )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)

    # convert to data
    def to_data(self):
        data = {}
        for attr in self.attributes:
            # ignore some attributes
            if attr not in ['eventRangeID', 'eventStatus', 'coreCount',
                            'cpuConsumptionTime']:
                continue
            val = getattr(self, attr)
            # don't propagate finished until subStatus is finished
            if attr == 'eventStatus':
                if val == 'finished' and self.subStatus not in ['finished', 'done']:
                    val = 'running'
            if val is not None:
                data[attr] = val
        return data

    # convert from data
    def from_data(self, data):
        for attr, val in data.iteritems():
            # skip non attributes
            if attr not in self.attributes:
                continue
            setattr(self, attr, val)
        if self.eventRangeID is not None:
            try:
                self.PandaID = long(self.eventRangeID.split('-')[1])
            except:
                pass
