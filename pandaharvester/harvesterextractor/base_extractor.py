from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.file_spec import FileSpec


# base extractor
class BaseExtractor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # make dictionary for aux inputs
    def make_aux_inputs(self, url_list):
        retVal = dict()
        for url in url_list:
            lfn = url.split("/")[-1]
            retVal[lfn] = {"scope": "aux_input", "INTERNAL_FileType": FileSpec.AUX_INPUT, "INTERNAL_URL": url}
        return retVal
