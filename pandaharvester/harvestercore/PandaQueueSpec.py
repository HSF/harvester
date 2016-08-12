"""
Panda Queue class

"""

from SpecBase import SpecBase

class PandaQueueSpec(SpecBase):
    # attributes
    attributesWithTypes = ('queueName:text',
                           'nQueueLimitJob:integer',
                           'nQueueLimitWorker:integer',
                           'maxWorkers:integer',
                           'jobFetchTime:timestamp',
                           'submitTime:timestamp',
                           )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
