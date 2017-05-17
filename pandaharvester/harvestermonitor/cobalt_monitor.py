import re
import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger()


# qstat output
# JobID  User     WallTime  Nodes  State   Location
# ===================================================
# 77734  fcurtis  06:00:00  64     queued  None

# monitor for HTCONDOR batch system
class CobaltMonitor (PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check workers
    def check_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workSpec.workerID))
            # first command
            comStr = "qstat {0}".format(workSpec.batchID)
            # first check
            tmpLog.debug('check with {0}'.format(comStr))
            p = subprocess.Popen(comStr.split(),
                                 shell=False,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            newStatus = workSpec.status
            # first check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug('retCode= {0}'.format(retCode))
            tmpLog.debug('stdOut = {0}'.format(stdOut))
            errStr = ''
            if retCode == 0:
                # parse
                if len(stdOut.split('\n')) == 2:
                    newStatus = WorkSpec.ST_finished 
                else:
                    tmpMatch = None
                    for tmpLine in stdOut.split('\n'):
                        #DPBtmpLog.debug('tmpLine = {0}'.format(tmpLine))
                        tmpMatch = re.search('{0} '.format(workSpec.batchID), tmpLine)
                        if tmpMatch is not None:
                            errStr = tmpLine
                            batchStatus = tmpLine.split()[4]
                            tmpLog.debug('batchStatus = {0}'.format(batchStatus))
                            if batchStatus == 'running':
                                newStatus = WorkSpec.ST_running
                            elif batchStatus == 'queued':
                                newStatus = WorkSpec.ST_submitted
                            elif batchStatus == 'user_hold':
                                newStatus = WorkSpec.ST_submitted
                            else:
                                # failed
                                errStr = stdOut + ' ' + stdErr
                                tmpLog.error(errStr)
                                raise Exception('failed to parse job state: ' + batchStatus + ' from qstat output: \n' + stdOut)
                            break
                    if tmpMatch is None:
                        # failed
                        errStr = stdOut + ' ' + stdErr
                        tmpLog.error(errStr)
                        raise Exception('could not parse qstat output: \n' + stdOut)

                tmpLog.debug('batchStatus {0} -> workerStatus {1}'.format(batchStatus,newStatus))
                retList.append((newStatus, errStr))
            else:
                # failed
                errStr = 'qstat stdout: \n' + stdOut + '\nqstat stderr:\n' + stdErr + '\n'
                tmpLog.error(errStr)
                retList.append((newStatus, errStr))
        return True, retList
