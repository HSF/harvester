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
jsonOutputsFileName = 'worker_filestostageout.json'

# json for job request
jsonJobRequestFileName = 'worker_requestjob.json'

# json for job spec
jsonJobSpecFileName = 'worker_jobspec.json'

# json for event request
jsonEventsRequestFileName = 'worker_requestevents.json'



# messenger with shared file system
class SharedFileMessenger (PluginBase):
    
    # constructor
    def __init__(self,**kwarg):
        PluginBase.__init__(self,**kwarg)



    # update job attributes with workers
    def updateJobAttributesWithWorkers(self,mapType,jobSpecs,workSpecs,filesToStageOut):
        if mapType == WorkSpec.MT_OneToOne:
            jobSpec  = jobSpecs[0]
            workSpec = workSpecs[0]
            tmpLog = CoreUtils.makeLogger(_logger,'PandaID={0} workerID={1}'.format(jobSpec.PandaID,workSpec.workerID))
            jobSpec.setAttributes(workSpec.workAttributes)
            jobSpec.setFilesToStageOut(filesToStageOut)
            jobSpec.status,jobSpec.subStatus = workSpec.convertToJobStatus()
            tmpLog.debug('new jobStatus={0} subStatus={1}'.format(jobSpec.status,jobSpec.subStatus))
        elif mapType == WorkSpec.MT_MultiJobs:
            # TOBEFIXED
            pass
        return True



    # get attributes of a worker which should be propagated to job(s).
    #  * the worker needs to put worker_attributes.json under the accesspoint
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
    



    # get files to stage-out in a dictionary.
    #  * the worker needs to put worker_filestostageout.json under the accesspoint
    def getFilesToStageOut(self,workSpec):
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



    # check if job is requested.
    # * the worker needs to put worker_requestjob.json under the accesspoint
    def jobRequested(self,workSpec):
        # get logger
        tmpLog = CoreUtils.makeLogger(_logger,'workerID={0}'.format(workSpec.workerID))
        # look for the json just under the accesspoint
        jsonFilePath = os.path.join(workSpec.getAccessPoint(),jsonJobRequestFileName)
        tmpLog.debug('looking for job request file {0}'.format(jsonFilePath))
        if not os.path.exists(jsonFilePath):
            # not found
            tmpLog.debug('not found')
            return False
        tmpLog.debug('found')
        return True



    # feed jobs
    # * worker_requestjob.json is put under the accesspoint
    def feedJobs(self,workSpec,jobList):
        # get logger
        tmpLog = CoreUtils.makeLogger(_logger,'workerID={0}'.format(workSpec.workerID))
        retVal = True
        if workSpec.mapType == WorkSpec.MT_OneToOne:
            jobSpec = jobList[0]
            # put the json just under the accesspoint
            jsonFilePath = os.path.join(workSpec.getAccessPoint(),jsonJobSpecFileName)
            tmpLog.debug('feeding jobs to {0}'.format(jsonFilePath))
            try:
                with open(jsonFilePath,'w') as jsonFile:
                    json.dump(jobSpec.jobParams,jsonFile)
            except:
                CoreUtils.dumpErrorMessage(tmpLog)
                retVal = False
        elif workSpec.mapType == WorkSpec.MT_MultiJobs:
            # TOBEFIXED
            pass
        tmpLog.debug('done')
        return retVal
        
