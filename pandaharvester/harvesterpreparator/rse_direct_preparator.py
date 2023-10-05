from future.utils import iteritems

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermover import mover_utils


# preparator plugin with RSE + no data motion
class RseDirectPreparator(PluginBase):
    """The workflow for RseDirectPreparator is as follows. First panda makes a rule to
    transfer files to an RSE which is associated to the resource. Once files are transferred
    to the RSE, job status is changed to activated from assigned. Then Harvester fetches
    the job and constructs input file paths that point to pfns in the storage. This means
    that the job directly read input files from the storage.
    """

    # constructor

    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check status
    def check_stage_in_status(self, jobspec):
        return True, ""

    # trigger preparation
    def trigger_preparation(self, jobspec):
        return True, ""

    # resolve input file paths
    def resolve_input_paths(self, jobspec):
        # get input files
        inFiles = jobspec.get_input_file_attributes()
        # set path to each file
        for inLFN, inFile in iteritems(inFiles):
            inFile["path"] = mover_utils.construct_file_path(self.basePath, inFile["scope"], inLFN)
        # set
        jobspec.set_input_file_paths(inFiles)
        return True, ""
