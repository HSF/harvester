from pandaharvester.harvesterzipper.base_zipper import BaseZipper


# base class for stager plugin inheriting from BaseZipper since zip functions
# were moved from stager to zipper
class BaseStager(BaseZipper):
    # constructor
    def __init__(self, **kwarg):
        self.zipDir = "${SRCDIR}"
        self.zip_tmp_log = None
        self.zip_jobSpec = None
        BaseZipper.__init__(self, **kwarg)
