from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermover import mover_utils


# plugin for preparator with RSE + directIO
class RseDirectPreparator(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check status
    def check_status(self, jobspec):
        return True, ''

    # trigger preparation
    def trigger_preparation(self, jobspec):
        return True, ''

    # resolve input file paths
    def resolve_input_paths(self, jobspec):
        # get input files
        inFiles = jobspec.get_input_file_attributes()
        # set path to each file
        for inLFN, inFile in inFiles.iteritems():
            inFile['path'] = mover_utils.construct_file_path(self.basePath, inFile['dataset'], inFile['scope'], inLFN)
        # set
        jobspec.set_input_file_paths(inFiles)
        return True, ''
