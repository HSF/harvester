"""
Relationship between job and worker

"""

from SpecBase import SpecBase


# relationship spec
class JobWorkerRelationSpec(SpecBase):
    # attributes
    attributesWithTypes = ('PandaID:integer',
                           'workerID:integer',
                           'relationType:text',
                           )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
