"""
Relationship between job and worker

"""

from .spec_base import SpecBase


# relationship spec
class JobWorkerRelationSpec(SpecBase):
    # attributes
    attributesWithTypes = (
        "PandaID:integer / index",
        "workerID:integer / index",
        "relationType:text",
    )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
