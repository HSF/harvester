import os
import errno
import datetime
import tempfile
import threading
import random

from concurrent.futures import ThreadPoolExecutor
import re
from math import log1p

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestermisc.htcondor_utils import get_job_id_tuple_from_batchid
from pandaharvester.harvestermisc.htcondor_utils import CondorJobSubmit


# logger
baseLogger = core_utils.setup_logger('htcondor_submitter')


# Integer division round up
def _div_round_up(a, b):
    return a // b + int(a % b > 0)


# Compute weight of each CE according to worker stat, return tuple(dict, total weight score)
def _get_ce_weighting(ce_endpoint_list=[], worker_ce_all_tuple=None):
    multiplier = 1000.
    n_ce = len(ce_endpoint_list)
    worker_limits_dict, worker_ce_stats_dict, worker_ce_backend_throughput_dict, time_window, n_new_workers = worker_ce_all_tuple
    N = float(n_ce)
    Q = float(worker_limits_dict['nQueueLimitWorker'])
    W = float(worker_limits_dict['maxWorkers'])
    Q_good_init = float(sum(worker_ce_backend_throughput_dict[_ce][_st]
                            for _st in ('submitted', 'running', 'finished')
                            for _ce in worker_ce_backend_throughput_dict))
    Q_good_fin = float(sum(worker_ce_backend_throughput_dict[_ce][_st]
                            for _st in ('submitted',)
                            for _ce in worker_ce_backend_throughput_dict))
    thruput_avg = (log1p(Q_good_init) - log1p(Q_good_fin))
    n_new_workers = float(n_new_workers)

    def _get_thruput(_ce_endpoint):  # inner function
        if _ce_endpoint not in worker_ce_backend_throughput_dict:
            q_good_init = 0.
            q_good_fin = 0.
        else:
            q_good_init = float(sum(worker_ce_backend_throughput_dict[_ce_endpoint][_st]
                                    for _st in ('submitted', 'running', 'finished')))
            q_good_fin = float(sum(worker_ce_backend_throughput_dict[_ce_endpoint][_st]
                                    for _st in ('submitted',)))
        thruput = (log1p(q_good_init) - log1p(q_good_fin))
        return thruput

    def _get_thruput_adj_ratio(thruput):  # inner function
        try:
            thruput_adj_ratio = thruput/thruput_avg + 1/N
        except ZeroDivisionError:
            if thruput == 0.:
                thruput_adj_ratio = 1/N
            else:
                raise
        return thruput_adj_ratio
    ce_base_weight_sum = sum((_get_thruput_adj_ratio(_get_thruput(_ce))
                                for _ce in ce_endpoint_list))

    def _get_init_weight(_ce_endpoint):  # inner function
        if _ce_endpoint not in worker_ce_stats_dict:
            q = 0.
            r = 0.
        else:
            q = float(worker_ce_stats_dict[_ce_endpoint]['submitted'])
            r = float(worker_ce_stats_dict[_ce_endpoint]['running'])
            # q_avg = sum(( float(worker_ce_stats_dict[_k]['submitted']) for _k in worker_ce_stats_dict )) / N
            # r_avg = sum(( float(worker_ce_stats_dict[_k]['running']) for _k in worker_ce_stats_dict )) / N
        if ( _ce_endpoint in worker_ce_stats_dict and q > Q ):
            return float(0)
        ce_base_weight_normalized = _get_thruput_adj_ratio(_get_thruput(_ce_endpoint))/ce_base_weight_sum
        q_expected = (Q + n_new_workers) * ce_base_weight_normalized
        # weight by difference
        ret = max((q_expected - q), 2**-10)
        # # Weight by running ratio
        # _weight_r = 1 + N*r/R
        if r == 0:
            # Penalty for dead CE (no running worker)
            ret = ret / (1 + log1p(q)**2)
        return ret
    init_weight_iterator = map(_get_init_weight, ce_endpoint_list)
    sum_of_weights = sum(init_weight_iterator)
    total_score = multiplier * N
    try:
        regulator = total_score / sum_of_weights
    except ZeroDivisionError:
        regulator = 1.
    ce_weight_dict = {_ce: _get_init_weight(_ce) * regulator for _ce in ce_endpoint_list}
    ce_thruput_dict = {_ce: _get_thruput(_ce) * 86400. / time_window for _ce in ce_endpoint_list}
    return total_score, ce_weight_dict, ce_thruput_dict


# Choose a CE accroding to weighting
def _choose_ce(weighting):
    total_score, ce_weight_dict, ce_thruput_dict = weighting
    lucky_number = random.random() * total_score
    cur = 0.
    ce_now = None
    for _ce, _w in ce_weight_dict.items():
        if _w == 0.:
            continue
        ce_now = _ce
        cur += _w
        if cur >= lucky_number:
            return _ce
    if ce_weight_dict.get(ce_now, -1) > 0.:
        return ce_now
    else:
        return None


# Get better string to display the statistics and weightng of CEs
def _get_ce_stats_weighting_display(ce_list, worker_ce_all_tuple, ce_weighting):
    worker_limits_dict, worker_ce_stats_dict, worker_ce_backend_throughput_dict, time_window, n_new_workers = worker_ce_all_tuple
    total_score, ce_weight_dict, ce_thruput_dict = ce_weighting
    worker_ce_stats_dict_sub_default = {'submitted': 0, 'running': 0}
    worker_ce_backend_throughput_dict_sub_default = {'submitted': 0, 'running': 0, 'finished': 0}
    general_dict = {
            'maxWorkers': int(worker_limits_dict.get('maxWorkers')),
            'nQueueLimitWorker': int(worker_limits_dict.get('nQueueLimitWorker')),
            'nNewWorkers': int(n_new_workers),
            'history_time_window': int(time_window),
        }
    general_str = (
            'maxWorkers={maxWorkers} '
            'nQueueLimitWorker={nQueueLimitWorker} '
            'nNewWorkers={nNewWorkers} '
            'hist_timeWindow={history_time_window} '
        ).format(**general_dict)
    ce_str_list = []
    for _ce in ce_list:
        schema_sub_dict = {
                'submitted_now': int(worker_ce_stats_dict.get(_ce, worker_ce_stats_dict_sub_default).get('submitted')),
                'running_now': int(worker_ce_stats_dict.get(_ce, worker_ce_stats_dict_sub_default).get('running')),
                'submitted_history': int(worker_ce_backend_throughput_dict.get(_ce, worker_ce_backend_throughput_dict_sub_default).get('submitted')),
                'running_history': int(worker_ce_backend_throughput_dict.get(_ce, worker_ce_backend_throughput_dict_sub_default).get('running')),
                'finished_history': int(worker_ce_backend_throughput_dict.get(_ce, worker_ce_backend_throughput_dict_sub_default).get('finished')),
                'thruput_score': ce_thruput_dict.get(_ce),
                'weight_score': ce_weight_dict.get(_ce),
            }
        ce_str = (
                '"{_ce}": '
                'now_S={submitted_now} '
                'now_R={running_now} '
                'hist_S={submitted_history} '
                'hist_R={running_history} '
                'hist_F={finished_history} '
                'T={thruput_score:.02f} '
                'W={weight_score:.03f} '
            ).format(_ce=_ce, **schema_sub_dict)
        ce_str_list.append(ce_str)
    stats_weighting_display_str = general_str + ' ; ' + ' , '.join(ce_str_list)
    return stats_weighting_display_str


# Replace condor Marco from SDF file, return string
def _condor_macro_replace(string, **kwarg):
    new_string = string
    macro_map = {
                '\$\(Cluster\)': str(kwarg['ClusterId']),
                '\$\(Process\)': str(kwarg['ProcId']),
                }
    for k, v in macro_map.items():
        new_string = re.sub(k, v, new_string)
    return new_string


# Parse resource type from string for Unified PanDA Queue
def _get_resource_type(string, is_unified_queue, is_pilot_option=False, pilot_version='1'):
    string = str(string)
    if not is_unified_queue:
        ret = ''
    elif string in set(['SCORE', 'MCORE', 'SCORE_HIMEM', 'MCORE_HIMEM']):
        if is_pilot_option:
            if pilot_version == '2':
                ret = '--resource-type {0}'.format(string)
            else:
                ret = '-R {0}'.format(string)
        else:
            ret = string
    else:
        ret = ''
    return ret


# Map "pilotType" (defined in harvester) to prodSourceLabel and pilotType option (defined in pilot, -i option)
# and piloturl (pilot option --piloturl)
# Depending on pilot version 1 or 2
def _get_prodsourcelabel_pilotypeopt_piloturlstr(pilot_type, pilot_version='1'):
    if pilot_version == '2':
        # pilot 2
        pt_psl_map = {
            'RC': ('rc_test2', 'RC', '--piloturl http://project-atlas-gmsb.web.cern.ch/project-atlas-gmsb/pilot2-dev.tar.gz'),
            'ALRB': ('rc_alrb', 'ALRB', ''),
            'PT': ('ptest', 'PR', ''),
        }
    else:
        # pilot 1, need not piloturl since wrapper covers it
        pt_psl_map = {
            'RC': ('rc_test', 'RC', ''),
            'ALRB': ('rc_alrb', 'ALRB', ''),
            'PT': ('ptest', 'PR', ''),
        }
    pilot_opt_tuple = pt_psl_map.get(pilot_type, None)
    return pilot_opt_tuple


# submit a bag of workers
def submit_bag_of_workers(data_list):
    # make logger
    tmpLog = core_utils.make_logger(baseLogger, method_name='submit_bag_of_workers')
    # keep order of workers in data_list
    workerIDs_list = [ data['workspec'].workerID for data in data_list ]
    # initialization
    worker_retval_map = {}
    worker_data_map = {}
    host_jdl_list_workerid_map = {}
    # go
    for data in data_list:
        workspec = data['workspec']
        workerID = workspec.workerID
        worker_data_map[workerID] = data
        to_submit = data['to_submit']
        # no need to submit bad worker
        if not to_submit:
            errStr = '{0} not submitted due to incomplete data of the worker'.format(workerID)
            tmpLog.warning(errStr)
            tmpRetVal = (None, errStr)
            # return tmpRetVal, workspec.get_changed_attributes()
            worker_retval_map[workerID] = (tmpRetVal, workspec.get_changed_attributes())
        # attributes
        try:
            use_spool = data['use_spool']
        except KeyError:
            errStr = '{0} not submitted due to incomplete data of the worker'.format(workerID)
            tmpLog.warning(errStr)
            tmpRetVal = (None, errStr)
            # return tmpRetVal, workspec.get_changed_attributes()
            worker_retval_map[workerID] = (tmpRetVal, workspec.get_changed_attributes())
        else:
            workspec.reset_changed_list()
            # fill in host_jdl_list_workerid_map
            a_jdl = make_a_jdl(**data)
            val = (workspec, a_jdl)
            try:
                host_jdl_list_workerid_map[workspec.submissionHost].append(val)
            except KeyError:
                host_jdl_list_workerid_map[workspec.submissionHost] = [val]
    # loop over submissionHost
    for host, val_list in host_jdl_list_workerid_map.items():
        # make jdl string of workers
        jdl_list = [ val[1] for val in val_list ]
        # condor job submit object
        tmpLog.debug('submitting to submissionHost={0}'.format(host))
        condor_job_submit = CondorJobSubmit(id=host)
        # submit
        try:
            batchIDs_list, ret_err_str = condor_job_submit.submit(jdl_list, use_spool=use_spool)
        except Exception as e:
            batchIDs_list = None
            ret_err_str = 'Exception {0}: {1}'.format(e.__class__.__name__, e)
        # result
        if batchIDs_list:
            # submitted
            n_workers = len(val_list)
            tmpLog.debug('submitted {0} workers to submissionHost={1}'.format(n_workers, host))
            for val_i in range(n_workers):
                val = val_list[val_i]
                workspec = val[0]
                # got batchID
                workspec.batchID = batchIDs_list[val_i]
                tmpLog.debug('workerID={0} submissionHost={1} batchID={2}'.format(
                                workspec.workerID, workspec.submissionHost, workspec.batchID))
                # get worker data
                data = worker_data_map[workspec.workerID]
                # set computingElement
                ce_info_dict = data['ce_info_dict']
                workspec.computingElement = ce_info_dict.get('ce_endpoint', '')
                # set log
                batch_log_dict = data['batch_log_dict']
                (clusterid, procid) = get_job_id_tuple_from_batchid(workspec.batchID)
                batch_log = _condor_macro_replace(batch_log_dict['batch_log'], ClusterId=clusterid, ProcId=procid)
                batch_stdout = _condor_macro_replace(batch_log_dict['batch_stdout'], ClusterId=clusterid, ProcId=procid)
                batch_stderr = _condor_macro_replace(batch_log_dict['batch_stderr'], ClusterId=clusterid, ProcId=procid)
                try:
                    batch_jdl = '{0}.jdl'.format(batch_stderr[:-4])
                except Exception:
                    batch_jdl = None
                workspec.set_log_file('batch_log', batch_log)
                workspec.set_log_file('stdout', batch_stdout)
                workspec.set_log_file('stderr', batch_stderr)
                workspec.set_log_file('jdl', batch_jdl)
                if not workspec.get_jobspec_list():
                    tmpLog.debug('No jobspec associated in the worker of workerID={0}'.format(workspec.workerID))
                else:
                    for jobSpec in workspec.get_jobspec_list():
                        # using batchLog and stdOut URL as pilotID and pilotLog
                        jobSpec.set_one_attribute('pilotID', workspec.workAttributes['stdOut'])
                        jobSpec.set_one_attribute('pilotLog', workspec.workAttributes['batchLog'])
                tmpLog.debug('Done set_log_file after submission of workerID={0}'.format(workspec.workerID))
                tmpRetVal = (True, '')
                worker_retval_map[workspec.workerID] = (tmpRetVal, workspec.get_changed_attributes())
        else:
            # failed
            tmpLog.debug('failed to submit workers to submissionHost={0} ; {1}'.format(host, ret_err_str))
            for val in val_list:
                workspec = val[0]
                errStr = 'submission failed: {0}'.format(ret_err_str)
                tmpLog.error(errStr)
                tmpRetVal = (None, errStr)
                worker_retval_map[workspec.workerID] = (tmpRetVal, workspec.get_changed_attributes())
    # make return list
    retValList = [ worker_retval_map[w_id] for w_id in workerIDs_list ]
    return retValList


# make a condor jdl for a worker
def make_a_jdl(workspec, template, n_core_per_node, log_dir, panda_queue_name, executable_file,
                x509_user_proxy, log_subdir=None, ce_info_dict=dict(), batch_log_dict=dict(),
                special_par='', harvester_queue_config=None, is_unified_queue=False, pilot_version='1', **kwarg):
    # make logger
    tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
                                    method_name='make_a_jdl')
    # Note: In workspec, unit of minRamCount and of maxDiskCount are both MB.
    #       In HTCondor SDF, unit of request_memory is MB, and request_disk is KB.
    n_core_total = workspec.nCore if workspec.nCore else n_core_per_node
    request_ram = max(workspec.minRamCount, 1 * n_core_total) if workspec.minRamCount else 1 * n_core_total
    request_disk = workspec.maxDiskCount * 1024 if workspec.maxDiskCount else 1
    request_walltime = workspec.maxWalltime if workspec.maxWalltime else 0
    io_intensity = workspec.ioIntensity if workspec.ioIntensity else 0
    ce_info_dict = ce_info_dict.copy()
    batch_log_dict = batch_log_dict.copy()
    # possible override by AGIS special_par
    if special_par:
        special_par_attr_list = ['queue', 'maxWallTime', 'xcount', ]
        _match_special_par_dict = { attr: re.search('\({attr}=([^)]+)\)'.format(attr=attr), special_par) \
                                        for attr in special_par_attr_list }
        for attr, _match in _match_special_par_dict.items():
            if not _match:
                continue
            elif attr == 'queue':
                ce_info_dict['ce_queue_name'] = str(_match.group(1))
            elif attr == 'maxWallTime':
                request_walltime = int(_match.group(1))
            elif attr == 'xcount':
                n_core_total = int(_match.group(1))
            tmpLog.debug('job attributes override by AGIS special_par: {0}={1}'.format(attr, str(_match.group(1))))
    # derived job attributes
    n_node = _div_round_up(n_core_total, n_core_per_node)
    request_ram_per_core = _div_round_up(request_ram * n_node, n_core_total)
    request_cputime = request_walltime * n_core_total
    request_walltime_minute = _div_round_up(request_walltime, 60)
    request_cputime_minute = _div_round_up(request_cputime, 60)
    # decide prodSourceLabel
    pilot_opt_tuple = _get_prodsourcelabel_pilotypeopt_piloturlstr(workspec.pilotType, pilot_version)
    if pilot_opt_tuple is None:
        prod_source_label = harvester_queue_config.get_source_label()
        pilot_type_opt = workspec.pilotType
        pilot_url_str = ''
    else:
        prod_source_label, pilot_type_opt, pilot_url_str = pilot_opt_tuple
    # open tmpfile as submit description file
    tmpFile = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_submit.sdf', dir=workspec.get_access_point())
    # fill in template string
    jdl_str = template.format(
        sdfPath=tmpFile.name,
        executableFile=executable_file,
        nCorePerNode=n_core_per_node,
        nCoreTotal=n_core_total,
        nNode=n_node,
        requestRam=request_ram,
        requestRamPerCore=request_ram_per_core,
        requestDisk=request_disk,
        requestWalltime=request_walltime,
        requestWalltimeMinute=request_walltime_minute,
        requestCputime=request_cputime,
        requestCputimeMinute=request_cputime_minute,
        accessPoint=workspec.accessPoint,
        harvesterID=harvester_config.master.harvester_id,
        workerID=workspec.workerID,
        computingSite=workspec.computingSite,
        pandaQueueName=panda_queue_name,
        x509UserProxy=x509_user_proxy,
        ceEndpoint=ce_info_dict.get('ce_endpoint', ''),
        ceHostname=ce_info_dict.get('ce_hostname', ''),
        ceFlavour=ce_info_dict.get('ce_flavour', ''),
        ceJobmanager=ce_info_dict.get('ce_jobmanager', ''),
        ceQueueName=ce_info_dict.get('ce_queue_name', ''),
        ceVersion=ce_info_dict.get('ce_version', ''),
        logDir=log_dir,
        logSubdir=log_subdir,
        gtag=batch_log_dict.get('gtag', 'fake_GTAG_string'),
        prodSourceLabel=prod_source_label,
        resourceType=_get_resource_type(workspec.resourceType, is_unified_queue),
        pilotResourceTypeOption=_get_resource_type(workspec.resourceType, is_unified_queue, True, pilot_version),
        ioIntensity=io_intensity,
        pilotType=pilot_type_opt,
        pilotUrlOption=pilot_url_str,
        )
    # save jdl to submit description file
    tmpFile.write(jdl_str)
    tmpFile.close()
    tmpLog.debug('saved sdf at {0}'.format(tmpFile.name))
    tmpLog.debug('done')
    return jdl_str


# parse log, stdout, stderr filename
def parse_batch_job_filename(value_str, file_dir, batchID, guess=False):
    _filename = os.path.basename(value_str)
    if guess:
        # guess file name before files really created; possibly containing condor macros
        return _filename
    else:
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
        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)
        # number of processes
        try:
            self.nProcesses
        except AttributeError:
            self.nProcesses = 1
        else:
            if (not self.nProcesses) or (self.nProcesses < 1):
                self.nProcesses = 1
        # executable file
        try:
            self.executableFile
        except AttributeError:
            self.executableFile = None
        # condor log directory
        try:
            self.logDir
        except AttributeError:
            self.logDir = os.getenv('TMPDIR') or '/tmp'
        # Default x509 proxy for a queue
        try:
            self.x509UserProxy
        except AttributeError:
            self.x509UserProxy = os.getenv('X509_USER_PROXY')
        # x509 proxy for analysis jobs in grandly unified queues
        try:
            self.x509UserProxyAnalysis
        except AttributeError:
            self.x509UserProxyAnalysis = os.getenv('X509_USER_PROXY_ANAL')
        # ATLAS AGIS
        try:
            self.useAtlasAGIS = bool(self.useAtlasAGIS)
        except AttributeError:
            self.useAtlasAGIS = False
        # ATLAS Grid CE, requiring AGIS
        try:
            self.useAtlasGridCE = bool(self.useAtlasGridCE)
        except AttributeError:
            self.useAtlasGridCE = False
        finally:
            self.useAtlasAGIS = self.useAtlasAGIS or self.useAtlasGridCE
        # sdf template directories of CEs
        try:
            self.CEtemplateDir
        except AttributeError:
            self.CEtemplateDir = ''
        # remote condor schedd and pool name (collector), and spool option
        try:
            self.condorSchedd
        except AttributeError:
            self.condorSchedd = None
        try:
            self.condorPool
        except AttributeError:
            self.condorPool = None
        # condor spool mechanism. If False, need shared FS across remote schedd
        try:
            self.useSpool
        except AttributeError:
            self.useSpool = False
        # number of workers less than this number will be bulkily submitted in only one schedd
        try:
            self.minBulkToRamdomizedSchedd
        except AttributeError:
            self.minBulkToRamdomizedSchedd = 20
        # record of information of CE statistics
        self.ceStatsLock = threading.Lock()
        self.ceStats = dict()

    # get CE statistics of a site
    def get_ce_statistics(self, site_name, n_new_workers, time_window=21600):
        if site_name in self.ceStats:
            return self.ceStats[site_name]
        with self.ceStatsLock:
            if site_name in self.ceStats:
                return self.ceStats[site_name]
            else:
                worker_limits_dict = self.dbInterface.get_worker_limits(self.queueName)
                worker_ce_stats_dict = self.dbInterface.get_worker_ce_stats(self.queueName)
                worker_ce_backend_throughput_dict = self.dbInterface.get_worker_ce_backend_throughput(self.queueName, time_window=time_window)
                return (worker_limits_dict, worker_ce_stats_dict, worker_ce_backend_throughput_dict, time_window, n_new_workers)

    # submit workers
    def submit_workers(self, workspec_list):
        tmpLog = self.make_logger(baseLogger, method_name='submit_workers')

        nWorkers = len(workspec_list)
        tmpLog.debug('start nWorkers={0}'.format(nWorkers))

        # whether to submit any worker
        to_submit_any = True

        # get log subdirectory name from timestamp
        timeNow = datetime.datetime.utcnow()
        log_subdir = timeNow.strftime('%y-%m-%d_%H')
        log_subdir_path = os.path.join(self.logDir, log_subdir)
        if self.condorSchedd is None or not self.useSpool:
            try:
                os.mkdir(log_subdir_path)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
                else:
                    pass

        # get info from harvester queue config
        _queueConfigMapper = QueueConfigMapper()
        harvester_queue_config = _queueConfigMapper.get_queue(self.queueName)

        is_grandly_unified_queue = False
        # get queue info from AGIS by cacher in db
        if self.useAtlasAGIS:
            panda_queues_dict = PandaQueuesDict()
            panda_queue_name = panda_queues_dict.get_panda_queue_name(self.queueName)
            this_panda_queue_dict = panda_queues_dict.get(self.queueName, dict())
            is_grandly_unified_queue = panda_queues_dict.is_grandly_unified_queue(self.queueName)
            # tmpLog.debug('panda_queues_name and queue_info: {0}, {1}'.format(self.queueName, panda_queues_dict[self.queueName]))
        else:
            panda_queues_dict = dict()
            panda_queue_name = self.queueName
            this_panda_queue_dict = dict()

        # get default information from queue info
        n_core_per_node_from_queue = this_panda_queue_dict.get('corecount', 1) if this_panda_queue_dict.get('corecount', 1) else 1
        is_unified_queue = this_panda_queue_dict.get('capability', '') == 'ucore'
        pilot_version_orig = str(this_panda_queue_dict.get('pilot_version', ''))
        pilot_version_suffix_str = '_pilot2' if pilot_version_orig == '2' else ''

        # get override requirements from queue configured
        try:
            n_core_per_node = self.nCorePerNode if self.nCorePerNode else n_core_per_node_from_queue
        except AttributeError:
            n_core_per_node = n_core_per_node_from_queue

        # deal with Condor schedd and central managers; make a random list the choose
        n_bulks = _div_round_up(nWorkers, self.minBulkToRamdomizedSchedd)
        if isinstance(self.condorSchedd, list) and len(self.condorSchedd) > 0:
            if isinstance(self.condorPool, list) and len(self.condorPool) > 0:
                orig_list = list(zip(self.condorSchedd, self.condorPool))
            else:
                orig_list = [ (_schedd, self.condorPool) for _schedd in self.condorSchedd ]
            if n_bulks < len(orig_list):
                schedd_pool_choice_list = random.sample(orig_list, n_bulks)
            else:
                schedd_pool_choice_list = orig_list
        else:
            schedd_pool_choice_list = [(self.condorSchedd, self.condorPool)]

        # deal with CE
        special_par = ''
        ce_weighting = None
        if self.useAtlasGridCE:
            # If ATLAS Grid CE mode used
            tmpLog.debug('Using ATLAS Grid CE mode...')
            queues_from_queue_list = this_panda_queue_dict.get('queues', [])
            special_par = this_panda_queue_dict.get('special_par', '')
            ce_auxilary_dict = {}
            for _queue_dict in queues_from_queue_list:
                if not ( _queue_dict.get('ce_endpoint')
                        and str(_queue_dict.get('ce_state', '')).upper() == 'ACTIVE'
                        and str(_queue_dict.get('ce_flavour', '')).lower() in set(['arc-ce', 'cream-ce', 'htcondor-ce']) ):
                    continue
                ce_endpoint = _queue_dict.get('ce_endpoint')
                if ( ce_endpoint in ce_auxilary_dict
                    and str(_queue_dict.get('ce_queue_name', '')).lower() == 'default' ):
                    pass
                else:
                    ce_auxilary_dict[ce_endpoint] = _queue_dict
            # qualified CEs from AGIS info
            n_qualified_ce = len(ce_auxilary_dict)
            if n_qualified_ce > 0:
                # Get CE weighting
                tmpLog.debug('Get CE weighting')
                worker_ce_all_tuple = self.get_ce_statistics(self.queueName, nWorkers)
                ce_weighting = _get_ce_weighting(ce_endpoint_list=list(ce_auxilary_dict.keys()),
                                                        worker_ce_all_tuple=worker_ce_all_tuple)
                stats_weighting_display_str = _get_ce_stats_weighting_display(
                                                ce_auxilary_dict.keys(), worker_ce_all_tuple, ce_weighting)
                tmpLog.debug('CE stats and weighting: {0}'.format(stats_weighting_display_str))
            else:
                tmpLog.error('No valid CE endpoint found')
                to_submit_any = False

        def _handle_one_worker(workspec, to_submit=to_submit_any):
            # make logger
            tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
                                            method_name='_handle_one_worker')
            ce_info_dict = dict()
            batch_log_dict = dict()
            data = {'workspec': workspec,
                    'to_submit': to_submit,}
            if to_submit:
                if self.useAtlasGridCE:
                    # choose a CE
                    tmpLog.info('choose a CE...')
                    ce_chosen = _choose_ce(ce_weighting)
                    try:
                        ce_info_dict = ce_auxilary_dict[ce_chosen].copy()
                    except KeyError:
                        tmpLog.info('Problem choosing CE with weighting. Choose an arbitrary CE endpoint')
                        ce_info_dict = random.choice(list(ce_auxilary_dict.values())).copy()
                    # go on info of the CE
                    ce_endpoint_from_queue = ce_info_dict.get('ce_endpoint', '')
                    ce_flavour_str = str(ce_info_dict.get('ce_flavour', '')).lower()
                    ce_version_str = str(ce_info_dict.get('ce_version', '')).lower()
                    ce_info_dict['ce_hostname'] = re.sub(':\w*', '',  ce_endpoint_from_queue)
                    if ce_info_dict['ce_hostname'] == ce_endpoint_from_queue:
                        # add default port to ce_endpoint if missing
                        default_port_map = {
                                'cream-ce': 8443,
                                'arc-ce': 2811,
                                'htcondor-ce': 9619,
                            }
                        if ce_flavour_str in default_port_map:
                            default_port = default_port_map[ce_flavour_str]
                            ce_info_dict['ce_endpoint'] = '{0}:{1}'.format(ce_endpoint_from_queue, default_port)
                    tmpLog.debug('For site {0} got pilot version: "{1}"; CE endpoint: "{2}", flavour: "{3}"'.format(
                                    self.queueName, pilot_version_orig, ce_endpoint_from_queue, ce_flavour_str))
                    if os.path.isdir(self.CEtemplateDir) and ce_flavour_str:
                        sdf_template_filename = '{ce_flavour_str}{pilot_version_suffix_str}.sdf'.format(
                                                    ce_flavour_str=ce_flavour_str, pilot_version_suffix_str=pilot_version_suffix_str)
                        self.templateFile = os.path.join(self.CEtemplateDir, sdf_template_filename)
                else:
                    try:
                        # Manually define site condor schedd as ceHostname and central manager as ceEndpoint
                        if self.ceHostname and isinstance(self.ceHostname, list) and len(self.ceHostname) > 0:
                            if isinstance(self.ceEndpoint, list) and len(self.ceEndpoint) > 0:
                                ce_info_dict['ce_hostname'], ce_info_dict['ce_endpoint'] = random.choice(list(zip(self.ceHostname, self.ceEndpoint)))
                            else:
                                ce_info_dict['ce_hostname'] = random.choice(self.ceHostname)
                                ce_info_dict['ce_endpoint'] = self.ceEndpoint
                        else:
                            ce_info_dict['ce_hostname'] = self.ceHostname
                            ce_info_dict['ce_endpoint'] = self.ceEndpoint
                    except AttributeError:
                        pass
                # template for batch script
                try:
                    tmpFile = open(self.templateFile)
                    sdf_template_raw = tmpFile.read()
                    tmpFile.close()
                except AttributeError:
                    tmpLog.error('No valid templateFile found. Maybe templateFile, CEtemplateDir invalid, or no valid CE found')
                    to_submit = False
                    return data
                else:
                    # get batch_log, stdout, stderr filename, and remobe commented liness
                    sdf_template_str_list = []
                    for _line in sdf_template_raw.split('\n'):
                        if _line.startswith('#'):
                            continue
                        sdf_template_str_list.append(_line)
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
                    sdf_template = '\n'.join(sdf_template_str_list)
                    # Choose from Condor schedd and central managers
                    condor_schedd, condor_pool = random.choice(schedd_pool_choice_list)
                    # set submissionHost
                    if not condor_schedd and not condor_pool:
                        workspec.submissionHost = 'LOCAL'
                    else:
                        workspec.submissionHost = '{0},{1}'.format(condor_schedd, condor_pool)
                    tmpLog.debug('set submissionHost={0}'.format(workspec.submissionHost))
                    # Log Base URL
                    if self.logBaseURL and '[ScheddHostname]' in self.logBaseURL:
                        schedd_hostname = re.sub(r'(?:[a-zA-Z0-9_.\-]*@)?([a-zA-Z0-9.\-]+)(?::[0-9]+)?',
                                                    lambda matchobj: matchobj.group(1) if matchobj.group(1) else '',
                                                    condor_schedd)
                        log_base_url = re.sub(r'\[ScheddHostname\]', schedd_hostname, self.logBaseURL)
                    else:
                        log_base_url = self.logBaseURL
                    # URLs for log files
                    if not (log_base_url is None):
                        if workspec.batchID:
                            batchID = workspec.batchID
                            guess = False
                        else:
                            batchID = ''
                            guess = True
                        batch_log_filename = parse_batch_job_filename(value_str=batch_log_value, file_dir=log_subdir_path, batchID=batchID, guess=guess)
                        stdout_path_file_name = parse_batch_job_filename(value_str=stdout_value, file_dir=log_subdir_path, batchID=batchID, guess=guess)
                        stderr_path_filename = parse_batch_job_filename(value_str=stderr_value, file_dir=log_subdir_path, batchID=batchID, guess=guess)
                        batch_log = '{0}/{1}/{2}'.format(log_base_url, log_subdir, batch_log_filename)
                        batch_stdout = '{0}/{1}/{2}'.format(log_base_url, log_subdir, stdout_path_file_name)
                        batch_stderr = '{0}/{1}/{2}'.format(log_base_url, log_subdir, stderr_path_filename)
                        workspec.set_log_file('batch_log', batch_log)
                        workspec.set_log_file('stdout', batch_stdout)
                        workspec.set_log_file('stderr', batch_stderr)
                        batch_log_dict['batch_log'] = batch_log
                        batch_log_dict['batch_stdout'] = batch_stdout
                        batch_log_dict['batch_stderr'] = batch_stderr
                        batch_log_dict['gtag'] = workspec.workAttributes['stdOut']
                        tmpLog.debug('Done set_log_file before submission')
                    tmpLog.debug('Done jobspec attribute setting')

                # choose the x509 certificate based on the type of job (analysis or production)
                proxy = _choose_proxy(workspec)

                # set data dict
                data.update({
                        'workspec': workspec,
                        'to_submit': to_submit,
                        'template': sdf_template,
                        'executable_file': self.executableFile,
                        'log_dir': self.logDir,
                        'log_subdir': log_subdir,
                        'n_core_per_node': n_core_per_node,
                        'panda_queue_name': panda_queue_name,
                        'x509_user_proxy': proxy,
                        'ce_info_dict': ce_info_dict,
                        'batch_log_dict': batch_log_dict,
                        'special_par': special_par,
                        'harvester_queue_config': harvester_queue_config,
                        'is_unified_queue': is_unified_queue,
                        'condor_schedd': condor_schedd,
                        'condor_pool': condor_pool,
                        'use_spool': self.useSpool,
                        'pilot_version': pilot_version_orig,
                        })
            return data

        def _choose_proxy(workspec):
            """
            Choose the proxy based on the job type
            """
            job_type = workspec.job_type
            proxy = self.x509UserProxy
            if is_grandly_unified_queue and (job_type == 'user' or job_type == 'analysis') and self.x509UserProxyAnalysis:
                tmpLog.debug('Taking analysis proxy')
                proxy = self.x509UserProxyAnalysis
            else:
                tmpLog.debug('Taking default proxy')

            return proxy

        def _propagate_attributes(workspec, tmpVal):
            # make logger
            tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
                                            method_name='_propagate_attributes')
            (retVal, tmpDict) = tmpVal
            workspec.set_attributes_with_dict(tmpDict)
            tmpLog.debug('Done workspec attributes propagation')
            return retVal

        tmpLog.debug('finished preparing worker attributes')

        # map(_handle_one_worker, workspec_list)
        with ThreadPoolExecutor(self.nProcesses * 4) as thread_pool:
            dataIterator = thread_pool.map(_handle_one_worker, workspec_list)
        tmpLog.debug('{0} workers handled'.format(nWorkers))

        # submit
        retValList = submit_bag_of_workers(list(dataIterator))
        tmpLog.debug('{0} workers submitted'.format(nWorkers))

        # propagate changed attributes
        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            retIterator = thread_pool.map(lambda _wv_tuple: _propagate_attributes(*_wv_tuple), zip(workspec_list, retValList))

        retList = list(retIterator)
        tmpLog.debug('done')

        return retList
