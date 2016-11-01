import re
import subprocess

from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.WorkSpec import WorkSpec
from pandaharvester.harvestercore.PluginBase import PluginBase

# logger
baseLogger = CoreUtils.setupLogger()


# monitor for SLURM batch system
class SlurmMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check workers
    def checkWorkers(self, workSpecs):
        retList = []
        for workSpec in workSpecs:
            # make logger
            tmpLog = CoreUtils.makeLogger(baseLogger, 'workerID={0}'.format(workSpec.workerID))
            # command
            comStr = "scontrol show job {0}".format(workSpec.batchID)
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
            if retCode == 0:
                # parse
                for tmpLine in stdOut.split('\n'):
                    tmpMatch = re.search('JobState=([^ ]+)', tmpLine)
                    if tmpMatch != None:
                        errStr = tmpLine
                        batchStatus = tmpMatch.group(1)
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
                        break
                retList.append((newStatus, errStr))
            else:
                # failed
                errStr = stdOut + ' ' + stdErr
                tmpLog.error(errStr)
                if 'slurm_load_jobs error: Invalid job id specified' in errStr:
                    newStatus = WorkSpec.ST_failed
                retList.append((newStatus, errStr))
        return True, retList
