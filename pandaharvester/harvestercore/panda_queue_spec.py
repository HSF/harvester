"""
Panda Queue class

"""

from spec_base import SpecBase


class PandaQueueSpec(SpecBase):
    # attributes
    attributesWithTypes = ('queueName:text',
                           'nQueueLimitJob:integer',
                           'nQueueLimitWorker:integer',
                           'maxWorkers:integer',
                           'jobFetchTime:timestamp',
                           'submitTime:timestamp',
                           'siteName:text',
                           'nNewWorkers:integer'
                           )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
