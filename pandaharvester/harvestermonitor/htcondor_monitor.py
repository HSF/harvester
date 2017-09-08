import re
import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger('htcondor_monitor')


## Run shell function
def _runShell(cmd):
    cmd = str(cmd)
    p = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdOut, stdErr = p.communicate()
    retCode = p.returncode
    return (retCode, stdOut, stdErr)


# monitor for HTCONDOR batch system
class HTCondorMonitor (PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check workers
    def check_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            ## Make logger
            tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workSpec.workerID),
                                            method_name='check_workers')
            ## Initialize newStatus
            newStatus = workSpec.status

            ## Query commands
            comStr_list = [
                'condor_q -l {0}'.format(workSpec.batchID),
                'condor_history -l {0}'.format(workSpec.batchID),
                ]

            got_job_ads = False
            job_ads_str = None

            for comStr in comStr_list:
                tmpLog.debug('check with {0}'.format(comStr))
                (retCode, stdOut, stdErr) = _runShell(comStr)
                if retCode == 0:
                    if stdOut:
                        ## Make job ads dictionary
                        job_ads_str = str(stdOut)
                        job_ads_dict = dict()
                        for tmp_line_str in job_ads_str.split('\n'):
                            tmp_match = re.search('^(\w+) = (.+)$', tmp_line_str)
                            if not tmp_match:
                                continue
                            _key = tmp_match.group(1)
                            _value = tmp_match.group(2)
                            job_ads_dict[_key] = _value

                        if 'ClusterId' in job_ads_dict:
                            ## Make sure job found via ClusterId
                            got_job_ads = True
                            break

                    ## Job not found
                    tmpLog.debug('job not found with {0}'.format(comStr))
                    continue

                else:
                    ## Command failed
                    errStr = 'command "{0}" failed, retCode={1}, error: {2} {3}'.format(comStr, retCode, stdOut, stdErr)
                    tmpLog.error(errStr)
                    retList.append((newStatus, errStr))
                    return True, retList

            ## Parse job ads
            errStr = ''
            if got_job_ads:
                ## Check JobStatus
                try:
                    batchStatus = job_ads_dict['JobStatus']
                except KeyError:
                    errStr = 'cannot get JobStatus of job batchID={0}'.format(workSpec.batchID)
                    tmpLog.error(errStr)
                    newStatus = WorkSpec.ST_failed
                else:
                    if batchStatus in ['2', '6']:
                        # 2 running, 6 transferring output
                        newStatus = WorkSpec.ST_running
                    elif batchStatus in ['1', '5', '7']:
                        # 1 idle, 5 held, 7 suspended
                        newStatus = WorkSpec.ST_submitted
                    elif batchStatus in ['3']:
                        # 3 removed
                        newStatus = WorkSpec.ST_cancelled
                    elif batchStatus in ['4']:
                        # 4 completed
                        try:
                            payloadExitCode = job_ads_dict['ExitCode']
                        except KeyError:
                            errStr = 'cannot get ExitCode of job batchID={0}'.format(workSpec.batchID)
                            tmpLog.error(errStr)
                            newStatus = WorkSpec.ST_failed
                        else:
                            if payloadExitCode in ['0']:
                                # Payload should return 0 after successful run
                                newStatus = WorkSpec.ST_finished
                            else:
                                # Other return codes are considered failed
                                newStatus = WorkSpec.ST_failed
                                tmpLog.debug('Payload execution error: returned non-zero')
                            tmpLog.info('Payload return code = {0}'.format(payloadExitCode))
                    else:
                        errStr = 'cannot get JobStatus of job batchID={0}'.format(workSpec.batchID)
                        tmpLog.error(errStr)
                        newStatus = WorkSpec.ST_failed

                    tmpLog.info('batchStatus {0} -> workerStatus {1}'.format(batchStatus, newStatus))

            else:
                tmpLog.info('condor job batchID={0} not found'.format(workSpec.batchID))

            ## Append to return list
            retList.append((newStatus, errStr))

        return True, retList
