from pandaharvester.harvestercore.PluginBase import PluginBase


# dummy plugin for stager
class DummyStager (PluginBase):
    
    # constructor
    def __init__(self,**kwarg):
        PluginBase.__init__(self,**kwarg)



    # check status
    def checkStatus(self,jobSpec):
        for fileSpec in jobSpec.outFiles:
            fileSpec.status = 'finished'
        return True,''



    # trigger stage out
    def triggerStageOut(self,jobSpec):
        for fileSpec in jobSpec.outFiles:
            #fileSpec.objstoreID = 123
            pass
        return True,''



    # zip output files
    def zipOutput(self,jobSpec):
        for fileSpec in jobSpec.outFiles:
            fileSpec.path = '/path/to/zip'
        return True,''

