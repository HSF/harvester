import os
import json

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config

from act.common.aCTConfig import aCTConfigARC
from act.atlas.aCTDBPanda import aCTDBPanda

# json for job report
jsonJobReport = harvester_config.payload_interaction.jobReportFile

# logger
baseLogger = core_utils.setup_logger('act_monitor')


# monitor for aCT plugin
class ACTMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        # Set up aCT DB connection
        self.log = core_utils.make_logger(baseLogger, 'aCT submitter', method_name='__init__')
        self.actDB = aCTDBPanda(self.log)

    # get access point
    def get_access_point(self, workspec, panda_id):
        if workspec.mapType == WorkSpec.MT_MultiJobs:
            accessPoint = os.path.join(workspec.get_access_point(), str(panda_id))
        else:
            accessPoint = workspec.get_access_point()
        return accessPoint

    # Check for pilot errors
    def check_pilot_status(self, workspec, tmpLog):
        for pandaID in workspec.pandaid_list:
            # look for job report
            accessPoint = self.get_access_point(workspec, pandaID)
            jsonFilePath = os.path.join(accessPoint, jsonJobReport)
            tmpLog.debug('looking for job report file {0}'.format(jsonFilePath))
            try:
                with open(jsonFilePath) as jsonFile:
                    jobreport = json.load(jsonFile)
            except:
                # Assume no job report available means true pilot or push mode
                # If job report is not available in full push mode aCT would have failed the job
                tmpLog.debug('no job report at {0}'.format(jsonFilePath))
                return WorkSpec.ST_finished
            tmpLog.debug("pilot info for {0}: {1}".format(pandaID, jobreport))
            # Check for pilot errors
            if jobreport.get('pilotErrorCode', 0):
                workspec.set_pilot_error(jobreport.get('pilotErrorCode'), jobreport.get('pilotErrorDiag', ''))
                return WorkSpec.ST_failed
            if jobreport.get('exeErrorCode', 0):
                workspec.set_pilot_error(jobreport.get('exeErrorCode'), jobreport.get('exeErrorDiag', ''))
                return WorkSpec.ST_failed
        return WorkSpec.ST_finished

    # check workers
    def check_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workSpec.workerID),
                                            method_name='check_workers')
            try:
                tmpLog.debug('Querying aCT for id {0}'.format(workSpec.batchID))
                columns = ['actpandastatus', 'pandastatus', 'computingElement', 'node']
                actjobs = self.actDB.getJobs("id={0}".format(workSpec.batchID), columns)
            except Exception as e:
                tmpLog.error("Failed to query aCT DB: {0}".format(str(e)))
                # send back current status
                retList.append((workSpec.status, ''))
                continue

            if not actjobs:
                tmpLog.error("Job with id {0} not found in aCT".format(workSpec.batchID))
                # send back current status
                retList.append((WorkSpec.ST_failed, "Job not found in aCT"))
                continue

            actstatus = actjobs[0]['actpandastatus']
            workSpec.nativeStatus = actstatus
            newStatus = WorkSpec.ST_running
            if actstatus in ['sent', 'starting']:
                newStatus = WorkSpec.ST_submitted
            elif actstatus == 'done':
                newStatus = self.check_pilot_status(workSpec, tmpLog)
            elif actstatus == 'donefailed':
                newStatus = WorkSpec.ST_failed
            elif actstatus == 'donecancelled':
                newStatus = WorkSpec.ST_cancelled

            if newStatus != workSpec.status:
                tmpLog.info('ID {0} updated status {1} -> {2} ({3})'.format(workSpec.batchID, workSpec.status, newStatus, actstatus))
            else:
                tmpLog.debug('batchStatus {0} -> workerStatus {1}'.format(actstatus, newStatus))

            if actjobs[0]['computingElement']:
                workSpec.computingElement = actjobs[0]['computingElement']
            if actjobs[0]['node']:
                try:
                    pandaid = workSpec.get_jobspec_list()[0].PandaID
                    workSpec.set_work_attributes({pandaid: {'node': actjobs[0]['node']}})
                except:
                    tmpLog.warning('Could not extract panda ID for worker {0}'.format(workSpec.batchID))

            retList.append((newStatus, ''))

        return True, retList
