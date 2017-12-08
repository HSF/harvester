import os
import tempfile
import subprocess

from concurrent.futures import ProcessPoolExecutor as Pool

import re

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger('htcondor_submitter')


# submit a worker
def submit_a_worker(data):
    workspec = data['workspec']
    template = data['template']
    log_dir = data['log_dir']
    n_core_per_node = data['n_core_per_node']
    workspec.reset_changed_list()
    # make logger
    tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
                                    method_name='submit_a_worker')
    # make batch script
    batchFile = make_batch_script(workspec, template, n_core_per_node, log_dir)
    # command
    comStr = 'condor_submit {0}'.format(batchFile)
    # submit
    tmpLog.debug('submit with {0}'.format(batchFile))
    try:
        p = subprocess.Popen(comStr.split(),
                             shell=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        # check return code
        stdOut, stdErr = p.communicate()
        retCode = p.returncode
    except:
        stdOut = ''
        stdErr = core_utils.dump_error_message(tmpLog, no_message=True)
        retCode = 1
    tmpLog.debug('retCode={0}'.format(retCode))
    if retCode == 0:
        # extract batchID
        job_id_match = None
        for tmp_line_str in stdOut.split('\n'):
            job_id_match = re.search('^(\d+) job[(]s[)] submitted to cluster (\d+)\.$', tmp_line_str)
            if job_id_match:
                break
        if job_id_match is not None:
            workspec.batchID = job_id_match.group(2)
            tmpLog.debug('batchID={0}'.format(workspec.batchID))
            tmpRetVal = (True, '')
        else:
            errStr = 'batchID cannot be found'
            tmpLog.error(errStr)
            tmpRetVal = (False, errStr)
    else:
        # failed
        errStr = stdOut + ' ' + stdErr
        tmpLog.error(errStr)
        tmpRetVal = (False, errStr)
    return tmpRetVal, workspec.get_changed_attributes()


# make batch script
def make_batch_script(workspec, template, n_core_per_node, log_dir):
    tmpFile = tempfile.NamedTemporaryFile(delete=False, suffix='_submit.sdf', dir=workspec.get_access_point())
    # Note: In workspec, unit of minRamCount and of maxDiskCount are both MB.
    #       In HTCondor SDF, unit of request_memory is MB, and request_disk is KB.
    tmpFile.write(template.format(
        nCorePerNode=n_core_per_node,
        nCoreTotal=workspec.nCore,
        nNode=(workspec.nCore // n_core_per_node + min(workspec.nCore % n_core_per_node, 1)),
        requestRam=workspec.minRamCount,
        requestDisk=(workspec.maxDiskCount * 1024),
        requestWalltime=workspec.maxWalltime,
        accessPoint=workspec.accessPoint,
        harvesterID=harvester_config.master.harvester_id,
        workerID=workspec.workerID,
        computingSite=workspec.computingSite,
        logDir=log_dir)
    )
    tmpFile.close()
    return tmpFile.name


# parse log, stdout, stderr filename
def parse_batch_job_filename(value_str, file_dir, batchID):
    _filename = os.path.basename(value_str)
    _sanitized_list = re.sub('\{(\w+)\}|\[(\w+)\]|\((\w+)\)|#(\w+)#|\$', '',  _filename).split('.')
    _prefix = _sanitized_list[0]
    _suffix = _sanitized_list[-1] if len(_sanitized_list) > 1 else ''

    for _f in os.listdir(file_dir):
        if re.match('{prefix}(.*)\.{batchID}\.(.*)\.{suffix}'.format(prefix=_prefix, suffix=_suffix, batchID=batchID), _f):
            return _f
    return None


# submitter for HTCONDOR batch system
class HTCondorSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.nProcesses = 1
        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)
        # template for batch script
        tmpFile = open(self.templateFile)
        self.template = tmpFile.read()
        tmpFile.close()
        # number of processes
        if self.nProcesses < 1:
            self.nProcesses = None

    # submit workers
    def submit_workers(self, workspec_list):
        tmpLog = core_utils.make_logger(baseLogger, method_name='submit_workers')
        tmpLog.debug('start nWorkers={0}'.format(len(workspec_list)))
        dataList = []
        for workSpec in workspec_list:
            data = {'workspec': workSpec,
                    'template': self.template,
                    'log_dir': self.logDir,
                    'n_core_per_node': self.nCorePerNode}
            dataList.append(data)
        # exec with mcore
        with Pool(self.nProcesses) as pool:
            retValList = pool.map(submit_a_worker, dataList)

        # get batch_log, stdout, stderr filename
        for _line in self.template.split('\n'):
            if _line.startswith('#'):
                continue
            _match_batch_log = re.match('log = (.+)', _line)
            _match_stdout = re.match('output = (.+)', _line)
            _match_stderr = re.match('error = (.+)', _line)
            if _match_batch_log:
                batch_log_value = _match_batch_log.group(1)
                continue
            if _match_stdout:
                stdout_value = _match_stdout.group(1)
                continue
            if _match_stderr:
                stderr_value = _match_stderr.group(1)
                continue


        # propagate changed attributes
        retList = []
        for workSpec, tmpVal in zip(workspec_list, retValList):
            retVal, tmpDict = tmpVal
            workSpec.set_attributes_with_dict(tmpDict)
            # URLs for log files
            if self.logBaseURL is not None and workSpec.batchID is not None:
                batch_log_filename = parse_batch_job_filename(value_str=batch_log_value, file_dir=self.logDir, batchID=workSpec.batchID)
                stdout_path_file_name = parse_batch_job_filename(value_str=stdout_value, file_dir=self.logDir, batchID=workSpec.batchID)
                stderr_path_filename = parse_batch_job_filename(value_str=stderr_value, file_dir=self.logDir, batchID=workSpec.batchID)
                workSpec.set_log_file('batch_log', '{0}/{1}'.format(self.logBaseURL, batch_log_filename))
                workSpec.set_log_file('stdout', '{0}/{1}'.format(self.logBaseURL, stdout_path_file_name))
                workSpec.set_log_file('stderr', '{0}/{1}'.format(self.logBaseURL, stderr_path_filename))
                tmpLog.debug('Done set_log_file')
            for jobSpec in workSpec.get_jobspec_list():
                # using batchLog and stdOut URL as pilotID and pilotLog
                jobSpec.set_one_attribute('pilotID', workSpec.workAttributes['stdOut'])
                jobSpec.set_one_attribute('pilotLog', workSpec.workAttributes['batchLog'])
                tmpLog.debug('Done jobspec attribute setting')
            retList.append(retVal)
        tmpLog.debug('done')
        return retList
