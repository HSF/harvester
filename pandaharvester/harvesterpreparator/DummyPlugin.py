# dummy plugin for preparator
class DummyPlugin:
    
    # constructor
    def __init__(self):
        pass


    # check status
    def checkStatus(self,jobSpec):
        return True,''



    # trigger preparation
    def triggerPreparation(self,jobSpec):
        return True,''

