from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config

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
        try:
            self.actDB = aCTDBPanda(self.log)
        except Exception as e:
            self.log.error('Could not connect to aCT database: {0}'.format(str(e)))
            self.actDB = None

    # check workers
    def check_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workSpec.workerID),
                                            method_name='check_workers')
            try:
                tmpLog.debug('Querying aCT for id {0}'.format(workSpec.batchID))
                columns = ['actpandastatus', 'pandastatus', 'computingElement', 'node', 'error']
                actjobs = self.actDB.getJobs("id={0}".format(workSpec.batchID), columns)
            except Exception as e:
                if self.actDB:
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
            errorMsg = ''
            if actstatus in ['waiting', 'sent', 'starting']:
                newStatus = WorkSpec.ST_submitted
            elif actstatus in ['done', 'donefailed', 'donecancelled', 'transferring', 'tovalidate']:
                # All post processing is now done in the stager
                newStatus = WorkSpec.ST_finished

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

            retList.append((newStatus, errorMsg))

        return True, retList
