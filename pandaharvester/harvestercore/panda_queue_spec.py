"""
Panda Queue class

"""

from .spec_base import SpecBase


class PandaQueueSpec(SpecBase):
    # attributes
    attributesWithTypes = ('queueName:text / index',
                           'nQueueLimitJob:integer',
                           'nQueueLimitWorker:integer',
                           'maxWorkers:integer',
                           'jobFetchTime:timestamp / index',
                           'submitTime:timestamp / index',
                           'siteName:text / index',
                           'resourceType:text',
                           'nNewWorkers:integer',
                           'uniqueName:text / unique'
                           )

    # catchall resource type
    RT_catchall = 'ANY'

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
