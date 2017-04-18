import re
import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger()


# monitor for HTCONDOR batch system
class HTCondorMonitor (PluginBase):
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
            comStr = 'condor_q -l {0}'.format(workSpec.batchID)
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
            tmpLog.debug('retCode={0}'.format(retCode))
            errStr = ''
            if retCode == 0:
                # first parse
                for tmpLine in stdOut.split('\n'):
                    tmpMatch = re.search('^JobStatus = ([^ ]+)', tmpLine)
                    if tmpMatch is not None:
                        batchStatus = tmpMatch.group(1)
                        if batchStatus in ['2', '5', '6', '7', '3', '4']:
                            # 2 running, 5 held, 6 transferring output, 7 suspended
                            newStatus = WorkSpec.ST_running
                        elif batchStatus in ['1']:
                            # 1 idle
                            newStatus = WorkSpec.ST_submitted
                        else:
                            newStatus = WorkSpec.ST_failed
                        tmpLog.debug('batchStatus {0} -> workerStatus {1}'.format(batchStatus, newStatus))
                        break
                    else:
                        # nothing in condor_q. Try condor_history
                        # second command
                        comStr = 'condor_history -l {0}'.format(workSpec.batchID)
                        # second check
                        tmpLog.debug('check with {0}'.format(comStr))
                        p2 = subprocess.Popen(comStr.split(),
                                                shell=False,
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE)
                        # second check return code
                        stdOut, stdErr = p2.communicate()
                        retCode = p2.returncode
                        tmpLog.debug('retCode={0}'.format(retCode))
                        if retCode == 0:
                            # second parse
                            for tmpLine in stdOut.split('\n'):
                                tmpMatch = re.search('^JobStatus = ([^ ]+)', tmpLine)
                                if tmpMatch is not None:
                                    batchStatus = tmpMatch.group(1)
                                    if batchStatus in ['4']:
                                        # 4 completed
                                        newStatus = WorkSpec.ST_finished
                                    elif batchStatus in ['3']:
                                        # 3 removed
                                        newStatus = WorkSpec.ST_cancelled
                                    else:
                                        newStatus = WorkSpec.ST_failed
                                    tmpLog.debug('batchStatus {0} -> workerStatus {1}'.format(batchStatus, newStatus))
                                    break
                            retList.append((newStatus, errStr))

                retList.append((newStatus, errStr))
            else:
                # failed
                errStr = stdOut + ' ' + stdErr
                tmpLog.error(errStr)
                retList.append((newStatus, errStr))
        return True, retList
