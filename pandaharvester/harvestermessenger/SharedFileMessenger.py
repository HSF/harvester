import json
import os.path
from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.WorkSpec import WorkSpec
from pandaharvester.harvestercore.PluginBase import PluginBase

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('SharedFileMessenger')



# json for worker attributes
jsonAttrsFileName = 'worker_attributes.json'

# json for outputs
jsonOutputsFileName = 'worker_outputs.json'



# messenger with shared file system
class SharedFileMessenger (PluginBase):
    
    # constructor
    def __init__(self,**kwarg):
        PluginBase.__init__(self,**kwarg)



    # get attributes of a worker which should be propagated to job(s).
    # the worker needs to put worker_attributes.json under the accesspoint
    def getWorkAttributes(self,workSpec):
        # get logger
        tmpLog = CoreUtils.makeLogger(_logger,'workerID={0}'.format(workSpec.workerID))
        retDict = {}
        if workSpec.mapType == WorkSpec.MT_OneToOne:
            # look for the json just under the accesspoint
            jsonFilePath = os.path.join(workSpec.getAccessPoint(),jsonAttrsFileName)
            tmpLog.debug('looking for attributes file {0}'.format(jsonFilePath))
            if not os.path.exists(jsonFilePath):
                # not found
                tmpLog.debug('not found')
                return {}
            try:
                with open(jsonFilePath) as jsonFile:
                    retDict = json.load(jsonFile)
            except:
                tmpLog.debug('failed to load json')
        elif workSpec.mapType == WorkSpec.MT_MultiJobs:
            # look for json files under accesspoint/${PandaID}
            # TOBEFIXED
            pass
        return retDict
    



    # get output files in a dictionary which should be staged-out.
    # the worker needs to put worker_outputs.json under the accesspoint
    def getOutputFiles(self,workSpec):
        # get logger
        tmpLog = CoreUtils.makeLogger(_logger,'workerID={0}'.format(workSpec.workerID))
        retDict = {}
        if workSpec.mapType == WorkSpec.MT_OneToOne:
            # look for the json just under the accesspoint
            jsonFilePath = os.path.join(workSpec.getAccessPoint(),jsonOutputsFileName)
            tmpLog.debug('looking for attributes file {0}'.format(jsonFilePath))
            if not os.path.exists(jsonFilePath):
                # not found
                tmpLog.debug('not found')
                return {}
            try:
                with open(jsonFilePath) as jsonFile:
                    retDict = json.load(jsonFile)
            except:
                tmpLog.debug('failed to load json')
        elif workSpec.mapType == WorkSpec.MT_MultiJobs:
            # look for json files under accesspoint/${PandaID}
            # TOBEFIXED
            pass
        return retDict



    # update job attributes with workers
    def updateJobAttributesWithWorkers(self,mapType,jobSpecs,workSpecs,outputFiles):
        if mapType == WorkSpec.MT_OneToOne:
            jobSpec  = jobSpecs[0]
            workSpec = workSpecs[0]
            tmpLog = CoreUtils.makeLogger(_logger,'PandaID={0} workerID={1}'.format(jobSpec.PandaID,workSpec.workerID))
            jobSpec.setAttributes(workSpec.workAttributes)
            jobSpec.setOutputs(outputFiles)
        elif mapType == WorkSpec.MT_MultiJobs:
            # TOBEFIXED
            pass
        return True
