import uuid

from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.PluginBase import PluginBase

# setup base logger
baseLogger = CoreUtils.setupLogger()


# dummy submitter
class DummySubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # submit workers
    def submitWorkers(self, workSpecs):
        tmpLog = CoreUtils.makeLogger(baseLogger)
        tmpLog.debug('start nWorkers={0}'.format(len(workSpecs)))
        retList = []
        retStrList = []
        for workSpec in workSpecs:
            workSpec.batchID = uuid.uuid4().hex
            retList.append((True, ''))
        tmpLog.debug('done')
        return retList
