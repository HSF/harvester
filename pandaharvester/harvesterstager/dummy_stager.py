from pandaharvester.harvestercore.plugin_base import PluginBase


# dummy plugin for stager
class DummyStager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check status
    def check_status(self, jobspec):
        for fileSpec in jobspec.outFiles:
            fileSpec.status = 'finished'
        return True, ''

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        for fileSpec in jobspec.outFiles:
            # fileSpec.objstoreID = 123
            pass
        return True, ''

    # zip output files
    def zip_output(self, jobspec):
        for fileSpec in jobspec.outFiles:
            fileSpec.path = '/path/to/zip'
        return True, ''
