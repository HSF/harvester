import re
import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase

from act.common.aCTConfig import aCTConfigARC
from act.atlas.aCTDBPanda import aCTDBPanda

# logger
baseLogger = core_utils.setup_logger()


# monitor for aCT plugin
class ACTMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        # Set up aCT DB connection
        self.log = core_utils.make_logger(baseLogger, 'aCT submitter')
        self.conf = aCTConfigARC()
        self.actDB = aCTDBPanda(self.log, self.conf.get(["db", "file"]))


    # check workers
    def check_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workSpec.workerID))
            try:
                tmpLog.debug('Querying aCT for id {0}'.format(workSpec.batchID))
                columns = ['actpandastatus', 'pandastatus']
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
            newStatus = WorkSpec.ST_running
            if actstatus in ['sent', 'starting']:
                newStatus = WorkSpec.ST_submitted
            elif actstatus == 'done':
                newStatus = WorkSpec.ST_finished
            elif actstatus == 'donefailed':
                newStatus = WorkSpec.ST_failed
            elif actstatus == 'donecancelled':
                newStatus = WorkSpec.ST_cancelled

            tmpLog.debug('batchStatus {0} -> workerStatus {1}'.format(actstatus, newStatus))
            retList.append((newStatus, ''))

        return True, retList
