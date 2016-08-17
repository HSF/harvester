"""
Event spec class

"""

import copy
import datetime

from SpecBase import SpecBase

class EventSpec(SpecBase):
    # attributes
    attributesWithTypes = ('eventRangeID:text',
                           'PandaID:integer',
                           'eventStatus:text',
                           'coreCount:integer',
                           'cpuConsumptionTime:integer',
                           'objstoreID:timestamp',
                           'subStatus:text'
                           )


    # constructor
    def __init__(self):
        SpecBase.__init__(self)



    # convert to data
    def toData(self):
        data = {}
        for attr in self.attributes:
            # ignore some attributes
            if not attr in ['eventRangeID','eventStatus','coreCount',
                            'cpuConsumptionTime','objstoreID']:
                continue
            val = getattr(self,attr)
            # don't propagate finished until subStatus is finished
            if attr == 'eventStatus':
                if val == 'finished' and not self.subStatus in ['finished','done']:
                    var == 'running'
            if val != None:
                data[attr] = val
            
        return data



    # convert from data
    def fromData(self,data):
        for attr,val in data.iteritems():
            # skip non attibutes
            if not attr in self.attributes:
                continue
            setattr(self,attr,val)
        if self.eventRangeID != None:
            try:
                self.PandaID = long(self.eventRangeID.split('-')[1])
            except:
                pass
