try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger('slurm_monitor')


# monitor for SLURM batch system
class SlurmBulkMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check workers
    def check_workers(self, workspec_list, bulk_size=200):
        retList = []
        batch_id_status_map = {}
        workspec_list_chunks = [workspec_list[i:i + bulk_size] for i in range(0, len(workspec_list), bulk_size)]
        for workspec_list_chunk in workspec_list_chunks:
            # make logger
            # worker_ids = [workSpec.workerID for workSpec in workspec_list_chunk]
            tmpLog = self.make_logger(baseLogger, 'bulkWorkers',
                                      method_name='check_workers')

            batch_id_list = []
            for workSpec in workspec_list_chunk:
                batch_id_list.append(str(workSpec.batchID))
            batch_id_list_str = ",".join(batch_id_list)
            # command
            comStr = "sacct -X --jobs={0}".format(batch_id_list_str)
            # check
            tmpLog.debug('check with {0}'.format(comStr))
            p = subprocess.Popen(comStr.split(),
                                 shell=False,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            newStatus = workSpec.status
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug('retCode={0}'.format(retCode))
            errStr = ''
            stdOut_str = stdOut if (isinstance(stdOut, str) or stdOut is None) else stdOut.decode()
            stdErr_str = stdErr if (isinstance(stdErr, str) or stdErr is None) else stdErr.decode()
            tmpLog.debug("stdout={0}".format(stdOut_str))
            tmpLog.debug("stderr={0}".format(stdErr_str))
            if retCode == 0:
                for tmpLine in stdOut_str.split('\n'):
                    if len(tmpLine) == 0 or tmpLine.startswith("JobID") or tmpLine.startswith("--"):
                        continue
                    batchID = tmpLine.split()[0].strip()
                    if len(tmpLine.split()) < 6:
                        batchStatus = tmpLine.split()[3].strip()
                    else:
                        batchStatus = tmpLine.split()[5].strip()

                    if batchStatus in ['RUNNING', 'COMPLETING', 'STOPPED', 'SUSPENDED']:
                        newStatus = WorkSpec.ST_running
                    elif batchStatus in ['COMPLETED', 'PREEMPTED', 'TIMEOUT']:
                        newStatus = WorkSpec.ST_finished
                    elif batchStatus in ['CANCELLED']:
                        newStatus = WorkSpec.ST_cancelled
                    elif batchStatus in ['CONFIGURING', 'PENDING']:
                        newStatus = WorkSpec.ST_submitted
                    else:
                        newStatus = WorkSpec.ST_failed
                    tmpLog.debug('batchStatus {0} -> workerStatus {1}'.format(batchStatus,
                                                                              newStatus))
                    batch_id_status_map[batchID] = (newStatus, stdErr_str)
            else:
                # failed
                errStr = '{0} {1}'.format(stdOut_str, stdErr_str)
                tmpLog.error(errStr)
                if 'slurm_load_jobs error: Invalid job id specified' in errStr:
                    newStatus = WorkSpec.ST_failed
                for batchID in batch_id_list:
                    batch_id_status_map[batchID] = (newStatus, errStr)

        for workSpec in workspec_list:
            batchID = str(workSpec.batchID)
            newStatus, errStr = None, None
            if batchID in batch_id_status_map:
                newStatus, errStr = batch_id_status_map[batchID]
            else:
                newStatus = WorkSpec.ST_failed
                errStr = "Unknown batchID"
            retList.append((newStatus, errStr))
            tmpLog.debug("Worker {0} -> workerStatus {1} errStr {2}".format(workSpec.workerID,
                                                                            newStatus,
                                                                            errStr))
        return True, retList
