import os
import json

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config

# json for job report
jsonJobReport = harvester_config.payload_interaction.jobReportFile

# json for outputs
jsonOutputsFileName = harvester_config.payload_interaction.eventStatusDumpJsonFile

# xml for outputs
xmlOutputsBaseFileName = harvester_config.payload_interaction.eventStatusDumpXmlFile

# logger
baseLogger = core_utils.setup_logger('act_messenger')

class ACTMessenger(PluginBase):
    '''Mechanism for passing information about completed jobs back to harvester.'''

    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # get access point
    def get_access_point(self, workspec, panda_id):
        if workspec.mapType == WorkSpec.MT_MultiJobs:
            accessPoint = os.path.join(workspec.get_access_point(), str(panda_id))
        else:
            accessPoint = workspec.get_access_point()
        return accessPoint

    def post_processing(self, workspec, jobspec_list, map_type):
        '''
        Take the jobReport placed by aCT in the access point and fill metadata
        attributes of the workspec.
        '''

        # get logger
        tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
                                        method_name='post_processing')
        if not workspec.workAttributes:
            workspec.workAttributes = {}

        for pandaID in workspec.pandaid_list:
            workspec.workAttributes[pandaID] = {}
            # look for job report
            accessPoint = self.get_access_point(workspec, pandaID)
            jsonFilePath = os.path.join(accessPoint, jsonJobReport)
            tmpLog.debug('looking for job report file {0}'.format(jsonFilePath))
            if not os.path.exists(jsonFilePath):
                # not found
                tmpLog.debug('not found')
            else:
                try:
                    with open(jsonFilePath) as jsonFile:
                        workspec.workAttributes[pandaID] = json.load(jsonFile)
                    tmpLog.debug('got {0} kB of job report'.format(os.stat(jsonFilePath).st_size / 1024))
                except:
                    tmpLog.debug('failed to load {0}'.format(jsonFilePath))
            tmpLog.debug("pilot info for {0}: {1}".format(pandaID, workspec.workAttributes[pandaID]))
            

    def get_work_attributes(self, workspec):
        '''Get info from the job to pass back to harvester'''
        # Just return existing attributes. Attributes are added to workspec for
        # finished jobs in post_processing
        return workspec.workAttributes

    def events_requested(self, workspec):
        '''Used to tell harvester that the worker requests events'''
        
        # TODO for ARC + ES where getEventRanges is called before submitting job
        return {}

    def feed_events(self, workspec, events_dict):
        '''Havester has an event range to pass to job'''
        
        # TODO for ARC + ES pass event ranges in job desc
        return True

    def events_to_update(self, workspec):
        '''Report events processed for harvester to update'''
        
        # TODO implement for ARC + ES where job does not update event ranges itself
        return {}

    # The remaining methods do not apply to ARC
    def feed_jobs(self, workspec, jobspec_list):
        '''Pass job to worker. No-op for Grid'''
        return True

    def get_files_to_stage_out(self, workspec):
        '''Not required in Grid case'''
        return {}

    def job_requested(self, workspec):
        '''Used in pull model to say that worker is ready for a job'''
        return False

    def setup_access_points(self, workspec_list):
        '''Access is through CE so nothing to set up here'''
        pass

    def get_panda_ids(self, workspec):
        '''For pull model, get panda IDs assigned to jobs'''
        return []

    def acknowledge_events_files(self, workSpec):
        '''Tell workers that harvester received events/files. No-op here'''
        pass
    
    def kill_requested(self, workspec):
        '''Worker wants to kill itself (?)'''
        return False
    
    def is_alive(self, workspec, time_limit):
        '''Check if worker is alive, not for Grid'''
        return True
    

def test():
    pass

if __name__ == '__main__':
    test()
