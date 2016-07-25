"""
Panda Queue class

"""

from SpecBase import SpecBase

class PandaQueueSpec(SpecBase):
    # attributes
    attributesWithTypes = ('queueName:text',
                           'nQueueLimit:int',
                           'jobFetchTime:timestamp',
                           )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
