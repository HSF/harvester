import re
import subprocess
import xml.etree.ElementTree as ET

# from concurrent.futures import ProcessPoolExecutor as Pool
from concurrent.futures import ThreadPoolExecutor as Pool

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


## Check one worker
def _check_one_worker(workspec, job_ads_all_dict):
    # Make logger for one single worker
    tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
    method_name='check_workers')

    ## Initialize newStatus
    newStatus = workspec.status

    try:
        job_ads_dict = job_ads_all_dict[str(workspec.batchID)]
    except KeyError:
        got_job_ads = False
    except Exception, e:
        got_job_ads = False
        tmpLog.error('With error {0}'.format(e))
    else:
        got_job_ads = True

        ## Parse job ads
        errStr = ''
        if got_job_ads:
            ## Check JobStatus
            try:
                batchStatus = job_ads_dict['JobStatus']
            except KeyError:
                errStr = 'cannot get JobStatus of job batchID={0}'.format(workspec.batchID)
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
                        errStr = 'cannot get ExitCode of job batchID={0}'.format(workspec.batchID)
                        tmpLog.error(errStr)
                        newStatus = WorkSpec.ST_failed
                    else:
                        if payloadExitCode in ['0']:
                            # Payload should return 0 after successful run
                            newStatus = WorkSpec.ST_finished
                        else:
                            # Other return codes are considered failed
                            newStatus = WorkSpec.ST_failed
                            errStr = 'Payload execution error: returned non-zero'
                            tmpLog.debug(errStr)

                        tmpLog.info('Payload return code = {0}'.format(payloadExitCode))
                else:
                    errStr = 'cannot get JobStatus of job batchID={0}'.format(workspec.batchID)
                    tmpLog.error(errStr)
                    newStatus = WorkSpec.ST_failed

                tmpLog.info('batchStatus {0} -> workerStatus {1}'.format(batchStatus, newStatus))

        else:
            tmpLog.info('condor job batchID={0} not found'.format(workspec.batchID))

    ## Return
    return (newStatus, errStr)


# monitor for HTCONDOR batch system
class HTCondorMonitor (PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.nProcesses = 4

    # check workers
    def check_workers(self, workspec_list):
        ## Make logger for batch job query
        tmpLog = core_utils.make_logger(baseLogger, '{0}'.format('batch job query'),
                                        method_name='check_workers')

        ## Initial a list all batchIDs of workspec_list
        batchIDs_list = map( lambda _x: str(_x.batchID) , workspec_list )

        ## Query commands
        orig_comStr_list = [
            'condor_q -xml',
            'condor_history -xml',
        ]

        ## Record batch job query result to this dict, with key = batchID
        job_ads_all_dict = dict()

        ## Start query
        for orig_comStr in orig_comStr_list:
            ## String of batchIDs
            batchIDs_str = ' '.join(batchIDs_list)

            ## Command
            if batchIDs_str:
                comStr = '{0} {1}'.format(orig_comStr, batchIDs_str)
            else:
                tmpLog.debug('No batch job left to query in this cycle by this thread')
                continue

            tmpLog.debug('check with {0}'.format(comStr))
            (retCode, stdOut, stdErr) = _runShell(comStr)

            if retCode == 0:
                ## Command succeeded

                ## Kill out redundant xml roots
                badtext = """
<?xml version="1.0"?>
<!DOCTYPE classads SYSTEM "classads.dtd">
<classads>

</classads>
"""
                # badtext_re_str = '<?xml version="1.0"?>\W+<!DOCTYPE classads SYSTEM "classads.dtd">\W+<classads>\W+</classads>'

                job_ads_xml_str = '\n'.join(str(stdOut).split(badtext))
                # job_ads_xml_str = re.sub(badtext_re_str, '\n', str(stdOut))
                # tmpLog.debug(job_ads_xml_str)
                import time
                with open('/tmp/jobads-{0}.xml'.format(time.time()), 'wb') as _f: _f.write(job_ads_xml_str)

                if '<c>' in job_ads_xml_str:
                    ## Found at least one job

                    ## XML parsing
                    xml_root = ET.fromstring(job_ads_xml_str)

                    def _getAttribute_tuple(attribute_xml_element):
                        ## Attribute name
                        _n = str(attribute_xml_element.get('n'))
                        ## Attribute value text
                        _t = ' '.join(attribute_xml_element.itertext())
                        return (_n, _t)


                    ## Every batch job
                    for _c in xml_root.findall('c'):
                        job_ads_dict = dict()
                        ## Every attribute
                        attribute_iter = map(_getAttribute_tuple, _c.findall('a'))
                        job_ads_dict.update(attribute_iter)
                        batchid = str(job_ads_dict['ClusterId'])
                        job_ads_all_dict[batchid] = job_ads_dict
                        ## Remove batch jobs already gotten from the list
                        batchIDs_list.remove(batchid)

                else:
                    ## Job not found
                    tmpLog.debug('job not found with {0}'.format(comStr))
                    continue

            else:
                ## Command failed
                errStr = 'command "{0}" failed, retCode={1}, error: {2} {3}'.format(comStr, retCode, stdOut, stdErr)
                tmpLog.error(errStr)
                return False, errStr

        if batchIDs_list:
            tmpLog.info( 'Unfound batch jobs: '.format( ' '.join(batchIDs_list) ) )

        ## Check for all workers
        with Pool(self.nProcesses) as _pool:
            retList = _pool.map(lambda _x: _check_one_worker(_x, job_ads_all_dict), workspec_list)


        return True, retList
