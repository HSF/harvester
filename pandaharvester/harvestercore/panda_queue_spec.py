"""
Panda Queue class

"""

from .spec_base import SpecBase


class PandaQueueSpec(SpecBase):
    # attributes
    attributesWithTypes = (
        "queueName:text / index",
        "nQueueLimitJob:integer",
        "nQueueLimitWorker:integer",
        "maxWorkers:integer",
        "jobFetchTime:timestamp / index",
        "submitTime:timestamp / index",
        "lockedBy:text",
        "siteName:text / index",
        "jobType:text",
        "resourceType:text",
        "nNewWorkers:integer",
        "uniqueName:text / unique",
        "nQueueLimitJobRatio:integer",
        "nQueueLimitJobMax:integer",
        "nQueueLimitJobMin:integer",
        "nQueueLimitWorkerRatio:integer",
        "nQueueLimitWorkerMax:integer",
        "nQueueLimitWorkerMin:integer",
    )

    # catchall resource type
    RT_catchall = "ANY"
    JT_catchall = "ANY"
    # constructor

    def __init__(self):
        SpecBase.__init__(self)
