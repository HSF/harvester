import re
try:
    import subprocess32 as subprocess
except:
    import subprocess
import xml.etree.ElementTree as ET

import time
import threading

import six

from concurrent.futures import ThreadPoolExecutor as Pool

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger('htcondor_monitor')


## Run shell function
def _runShell(cmd):
    cmd = str(cmd)
    p = subprocess.Popen(cmd.split(), shell=False, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdOut, stdErr = p.communicate()
    retCode = p.returncode
    return (retCode, stdOut, stdErr)


## Native HTCondor status map
CONDOR_JOB_STATUS_MAP = {
    '1': 'idle',
    '2': 'running',
    '3': 'removed',
    '4': 'completed',
    '5': 'held',
    '6': 'transferring output',
    '7': 'suspended',
    }


## Singleton distinguishable with ID
class SingletonWithID(type):
    def __init__(cls, *args,**kwargs):
        cls.__instance = {}
        super(SingletonWithID, cls).__init__(*args, **kwargs)
    def __call__(cls, *args, **kwargs):
        obj_id = str(kwargs.get('id', ''))
        if obj_id not in cls.__instance:
            cls.__instance[obj_id] = super(SingletonWithID, cls).__call__(*args, **kwargs)
        return cls.__instance.get(obj_id)


## Condor job ads query
class CondorJobQuery(six.with_metaclass(SingletonWithID, object)):
    ## Query commands
    orig_comStr_list = [
        'condor_q -xml',
        'condor_history -xml',
    ]
    # Bad text of redundant xml roots to eleminate from condor XML
    badtext = """
</classads>

<?xml version="1.0"?>
<!DOCTYPE classads SYSTEM "classads.dtd">
<classads>
"""

    def __init__(self, *args, **kwargs):
        self.submissionHost = kwargs.get('id')
        self.lock = threading.Lock()
        self.job_ads_all_dict = {}
        self.job_last_update_dict = {}
        self.updateTimestamp = 0

    def get_all(self, batchIDs_list=[], update_interval=300, lifetime=432000):
        if time.time() > self.updateTimestamp + update_interval:
            if self.lock.acquire(False):
                self.cleanup(lifetime=lifetime)
                self.update(batchIDs_list)
                self.lock.release()
        for batchid in batchIDs_list:
            if batchid not in self.job_ads_all_dict:
                if self.lock.acquire(False):
                    self.update(batchIDs_list)
                    self.lock.release()
                break
        return self.job_ads_all_dict

    def update(self, batchIDs_list=[]):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, method_name='CondorJobQuery.update')
        ## Start query
        tmpLog.debug('Start update query')
        batchIDs_list = list(batchIDs_list)
        for orig_comStr in self.orig_comStr_list:
            ## String of batchIDs
            batchIDs_str = ' '.join(batchIDs_list)
            ## Command
            if 'condor_q' in orig_comStr or ('condor_history' in orig_comStr and batchIDs_list):
                name_opt, pool_opt = '', ''
                if self.submissionHost:
                    try:
                        condor_schedd, condor_pool = self.submissionHost.split(',')[0:2]
                    except ValueError:
                        tmpLog.error('Invalid submissionHost: {0} . Skipped'.format(self.submissionHost))
                        continue
                    name_opt = '-name {0}'.format(condor_schedd) if condor_schedd else ''
                    pool_opt = '-pool {0}'.format(condor_pool) if condor_pool else ''
                ids = ''
                if 'condor_history' in orig_comStr:
                    ids = batchIDs_str
                comStr = '{cmd} {name_opt} {pool_opt} {ids}'.format(cmd=orig_comStr,
                                                                    name_opt=name_opt,
                                                                    pool_opt=pool_opt,
                                                                    ids=ids)
            else:
                # tmpLog.debug('No batch job left to query in this cycle by this thread')
                continue

            tmpLog.debug('check with {0}'.format(comStr))
            (retCode, stdOut, stdErr) = _runShell(comStr)
            if retCode == 0:
                ## Command succeeded
                job_ads_xml_str = '\n'.join(str(stdOut).split(self.badtext))
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
                        self.job_ads_all_dict[batchid] = job_ads_dict
                        ## Remove batch jobs already gotten from the list
                        if batchid in batchIDs_list:
                            batchIDs_list.remove(batchid)
                else:
                    ## Job not found
                    tmpLog.debug('job not found with {0}'.format(comStr))
                    continue
            else:
                ## Command failed
                errStr = 'command "{0}" failed, retCode={1}, error: {2} {3}'.format(comStr, retCode, stdOut, stdErr)
                tmpLog.error(errStr)
        if len(batchIDs_list) > 0:
            ## Job unfound via both condor_q or condor_history, marked as failed worker in harvester
            for batchid in batchIDs_list:
                self.job_ads_all_dict[batchid] = dict()
            tmpLog.info( 'Force batchStatus to be failed for unfound batch jobs of submissionHost={0}: {1}'.format(
                            self.submissionHost, ' '.join(batchIDs_list) ) )
        ## Size in cache
        tmpLog.debug('Job query cache id={0} , size={1}'.format(id(self.job_ads_all_dict), len(self.job_ads_all_dict)))
        ## Update timestamp and for all jobs
        self.updateTimestamp = time.time()
        self.job_last_update_dict.update(dict.fromkeys(six.iterkeys(self.job_ads_all_dict), self.updateTimestamp))
        ## Return
        return

    def cleanup(self, lifetime):
        # Make logger
        tmpLog = core_utils.make_logger(baseLogger, method_name='CondorJobQuery.cleanup')
        # Start cleanup
        now_timestamp = time.time()
        job_last_update_dict_copy = self.job_last_update_dict.copy()
        for batchid, last_update in six.iteritems(job_last_update_dict_copy):
            if now_timestamp > last_update + lifetime:
                self.job_ads_all_dict.pop(batchid, None)
                self.job_last_update_dict.pop(batchid, None)
        # Size in cache
        tmpLog.debug('Job query cache id={0} , size={1}'.format(id(self.job_ads_all_dict), len(self.job_ads_all_dict)))
        # Return
        return


## Check one worker
def _check_one_worker(workspec, job_ads_all_dict):
    # Make logger for one single worker
    tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID), method_name='_check_one_worker')

    ## Initialize newStatus
    newStatus = workspec.status
    errStr = ''

    name_opt, pool_opt = '', ''
    if workspec.submissionHost:
        try:
            condor_schedd, condor_pool = workspec.submissionHost.split(',')[0:2]
        except ValueError:
            pass
        name_opt = '-name {0}'.format(condor_schedd) if condor_schedd else ''
        pool_opt = '-pool {0}'.format(condor_pool) if condor_pool else ''

    try:
        job_ads_dict = job_ads_all_dict[str(workspec.batchID)]
    except KeyError:
        got_job_ads = False
    except Exception as e:
        got_job_ads = False
        tmpLog.error('With error {0}'.format(e))
    else:
        got_job_ads = True

        ## Parse job ads
        if got_job_ads:
            ## Check JobStatus
            try:
                batchStatus = job_ads_dict['JobStatus']
            except KeyError:
                errStr = 'cannot get JobStatus of job submissionHost={0} batchID={1}. Regard the worker as canceled by default'.format(workspec.submissionHost, workspec.batchID)
                tmpLog.error(errStr)
                newStatus = WorkSpec.ST_cancelled
            else:
                # Propagate native condor job status
                workspec.nativeStatus = CONDOR_JOB_STATUS_MAP.get(batchStatus, 'unexpected')
                if batchStatus in ['2', '6']:
                    # 2 running, 6 transferring output
                    newStatus = WorkSpec.ST_running
                elif batchStatus in ['1', '7']:
                    # 1 idle, 7 suspended
                    newStatus = WorkSpec.ST_submitted
                elif batchStatus in ['3']:
                    # 3 removed
                    errStr = 'Condor HoldReason: {0} ; Condor RemoveReason: {1} '.format(
                                job_ads_dict.get('LastHoldReason'), job_ads_dict.get('RemoveReason'))
                    newStatus = WorkSpec.ST_cancelled
                elif batchStatus in ['5']:
                    # 5 held
                    if (
                        job_ads_dict.get('HoldReason') == 'Job not found'
                        or int(time.time()) - int(job_ads_dict.get('EnteredCurrentStatus', 0)) > 7200
                        ):
                        # Kill the job if held too long or other reasons
                        (retCode, stdOut, stdErr) = _runShell('condor_rm {name_opt} {pool_opt} {batchID}'.format(
                                                                                        batchID=workspec.batchID,
                                                                                        name_opt=name_opt,
                                                                                        pool_opt=pool_opt,
                                                                                        ))
                        if retCode == 0:
                            tmpLog.info('killed held job submissionHost={0} batchID={1}'.format(workspec.submissionHost, workspec.batchID))
                        else:
                            newStatus = WorkSpec.ST_cancelled
                            tmpLog.error('cannot kill held job submissionHost={0} batchID={1}. Force worker to be in cancelled status'.format(workspec.submissionHost, workspec.batchID))
                        # Mark the PanDA job as closed instead of failed
                        workspec.set_pilot_closed()
                        tmpLog.debug('Called workspec set_pilot_closed')
                    else:
                        newStatus = WorkSpec.ST_submitted
                elif batchStatus in ['4']:
                    # 4 completed
                    try:
                        payloadExitCode = job_ads_dict['ExitCode']
                    except KeyError:
                        errStr = 'cannot get ExitCode of job submissionHost={0} batchID={1}'.format(workspec.submissionHost, workspec.batchID)
                        tmpLog.error(errStr)
                        newStatus = WorkSpec.ST_failed
                    else:
                        # Propagate condor return code
                        workspec.nativeExitCode = payloadExitCode
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
                    errStr = 'cannot get reasonable JobStatus of job submissionHost={0} batchID={1}. Regard the worker as failed by default'.format(
                                workspec.submissionHost, workspec.batchID)
                    tmpLog.error(errStr)
                    newStatus = WorkSpec.ST_failed

                tmpLog.info('submissionHost={0} batchID={1} : batchStatus {2} -> workerStatus {3}'.format(
                                workspec.submissionHost, workspec.batchID, batchStatus, newStatus))

        else:
            tmpLog.error('condor job submissionHost={0} batchID={1} not found. Regard the worker as canceled by default'.format(
                            workspec.submissionHost, workspec.batchID))
            newStatus = WorkSpec.ST_cancelled
            tmpLog.info('submissionHost={0} batchID={1} : batchStatus {2} -> workerStatus {3}'.format(
                            workspec.submissionHost, workspec.batchID, batchStatus, newStatus))

    ## Return
    return (newStatus, errStr)


# monitor for HTCONDOR batch system
class HTCondorMonitor (PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        try:
            self.nProcesses
        except AttributeError:
            self.nProcesses = 4
        try:
            self.cacheUpdateInterval
        except AttributeError:
            self.cacheUpdateInterval = 300
        try:
            self.cacheLifetime
        except AttributeError:
            self.cacheLifetime = 432000

    # check workers
    def check_workers(self, workspec_list):
        ## Make logger for batch job query
        tmpLog = self.make_logger(baseLogger, '{0}'.format('batch job query'),
                                  method_name='check_workers')

        ## Initial a dictionary of submissionHost: list of batchIDs among workspec_list
        s_b_dict = {}
        for _w in workspec_list:
            try:
                s_b_dict[_w.submissionHost].append(str(_w.batchID))
            except KeyError:
                s_b_dict[_w.submissionHost] = [str(_w.batchID)]

        ## Loop over submissionHost
        for submissionHost, batchIDs_list in six.iteritems(s_b_dict):
            ## Record batch job query result to this dict, with key = batchID
            job_query = CondorJobQuery(id=submissionHost)
            job_ads_all_dict = job_query.get_all(batchIDs_list=batchIDs_list,
                                                    update_interval=self.cacheUpdateInterval,
                                                    lifetime=self.cacheLifetime)

        ## Check for all workers
        with Pool(self.nProcesses) as _pool:
            retIterator = _pool.map(lambda _x: _check_one_worker(_x, job_ads_all_dict), workspec_list)

        retList = list(retIterator)

        return True, retList
