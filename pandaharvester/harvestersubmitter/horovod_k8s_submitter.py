import os
import argparse
import traceback
try:
    from urllib import unquote  # Python 2.X
except ImportError:
    from urllib.parse import unquote  # Python 3+

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.k8s_utils import k8s_Client
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper

# logger
base_logger = core_utils.setup_logger('horovod_submitter')

# image defaults
DEF_EVA_IMAGE = 'fbarreir/rui-hrvd'
# PILOT_IMAGE = 'gitlab-registry.cern.ch/panda/harvester-k8s-images/adc-centos7-singularity:work'
PILOT_IMAGE = 'palnilsson/my-panda-pilot'

# command defaults
DEF_EVALUATION_COMMAND = ['sh', '-c', 'cp $SSH_DIR/* ~/.ssh/;',
                          'while [ ! -f __payload_in_sync_file__ ]; do sleep 5; done; ',
                          'echo "=== cat exec script ==="; ', 'cat __run_main_exec.sh; ', 'echo; ',
                          'echo "=== exec script ==="; ', '/bin/sh __run_main_exec.sh; ',
                          'REAL_MAIN_RET_CODE=$?; ', 'touch __payload_out_sync_file__; ',
                          'exit $REAL_MAIN_RET_CODE ']

# DEF_PILOT_COMMAND = ["sh", "-c", "cd; wget https://raw.githubusercontent.com/HSF/harvester/master/pandaharvester/harvestercloud/pilots_starter.py; chmod 755 pilots_starter.py; ./pilots_starter.py || true"]
DEF_PILOT_COMMAND = ["python3", "/user/share/panda-pilot/pilot.py", "-d", "-w", "generic", "-j", "ptest", "-q", "CERN-PROD_UCORE_2", "--pilot-user=ATLAS"]
DEF_WORKER_COMMAND = ["sh", "-c", "mkdir -p ~/.ssh && cat $SSH_DIR/public_key >> ~/.ssh/authorized_keys && /usr/sbin/sshd -p 22 && sleep infinity"]


class HorovodSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)

        self.k8s_client = k8s_Client(namespace=self.k8s_namespace, config_file=self.k8s_config_file)

        # required for parsing jobParams
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('-p', dest='executable', type=unquote)
        self.parser.add_argument('--containerImage', dest='container_image')

        # number of processes
        try:
            self.nProcesses
        except AttributeError:
            self.nProcesses = 1
        else:
            if (not self.nProcesses) or (self.nProcesses < 1):
                self.nProcesses = 1

        # CPU adjust ratio
        try:
            self.cpuAdjustRatio
        except AttributeError:
            self.cpuAdjustRatio = 100

        # Memory adjust ratio
        try:
            self.memoryAdjustRatio
        except AttributeError:
            self.memoryAdjustRatio = 100

    def parse_params(self, job_params):
        tmp_log = self.make_logger(base_logger, method_name='parse_params')

        job_params_list = job_params.split(' ')
        args, unknown = self.parser.parse_known_args(job_params_list)

        tmp_log.info('Parsed params: {0}'.format(args))
        return args

    def read_job_configuration(self, work_spec):
        try:
            job_spec_list = work_spec.get_jobspec_list()
            if job_spec_list:
                job_spec = job_spec_list[0]
                job_fields = job_spec.jobParams
                job_pars_parsed = self.parse_params(job_fields['jobPars'])
                return job_fields, job_pars_parsed
        except (KeyError, AttributeError):
            return None, None

        return None, None

    def get_container_image(self, job_fields):
        return job_fields.get('container_name', DEF_EVA_IMAGE)

    def _choose_proxy(self, workspec, is_grandly_unified_queue):
        """
        Choose the proxy based on the job type and whether k8s secrets are enabled
        """
        cert = None
        job_type = workspec.jobType

        if is_grandly_unified_queue and job_type in ('user', 'panda', 'analysis'):
            if self.proxySecretPathAnalysis:
                cert = self.proxySecretPathAnalysis
            elif self.proxySecretPath:
                cert = self.proxySecretPath
        else:
            if self.proxySecretPath:
                cert = self.proxySecretPath

        return cert

    def submit_horovod_k8s_formation(self, work_spec):
        tmp_log = self.make_logger(base_logger, method_name='submit_horovod_k8s_formation')

        # get info from harvester queue config
        _queueConfigMapper = QueueConfigMapper()
        harvester_queue_config = _queueConfigMapper.get_queue(self.queueName)
        prod_source_label = harvester_queue_config.get_source_label(work_spec.jobType)

        # set the stdout log file
        log_file_name = '{0}_{1}.out'.format(harvester_config.master.harvester_id, work_spec.workerID)
        work_spec.set_log_file('stdout', '{0}/{1}'.format(self.logBaseURL, log_file_name))

        try:
            # read the job configuration
            job_fields, job_pars_parsed = self.read_job_configuration(work_spec)

            # choose the appropriate proxy
            panda_queues_dict = PandaQueuesDict()
            is_grandly_unified_queue = panda_queues_dict.is_grandly_unified_queue(self.queueName)
            cert = self._choose_proxy(work_spec, is_grandly_unified_queue)
            if not cert:
                err_str = 'No proxy specified in proxySecretPath. Not submitted'
                tmp_return_value = (False, err_str)
                return tmp_return_value

            # get the walltime limit
            try:
                max_time = panda_queues_dict.get(self.queueName)['maxtime']
            except Exception as e:
                tmp_log.warning('Could not retrieve maxtime field for queue {0}'.format(self.queueName))
                max_time = None

            # figure out image and command details
            evaluation_image = self.get_container_image(job_fields)
            pilot_image = PILOT_IMAGE
            evaluation_command = DEF_EVALUATION_COMMAND
            pilot_command = DEF_PILOT_COMMAND
            worker_command = DEF_WORKER_COMMAND

            # submit the worker
            rsp = self.k8s_client.create_horovod_formation(work_spec, prod_source_label, self.queueName,
                                                           evaluation_image, evaluation_command,
                                                           pilot_image, pilot_command, worker_command,
                                                           cert, cpu_adjust_ratio=self.cpuAdjustRatio,
                                                           memory_adjust_ratio=self.memoryAdjustRatio,
                                                           max_time=max_time)
        except Exception as _e:
            tmp_log.error(traceback.format_exc())
            err_str = 'Failed to create a formation; {0}'.format(_e)
            tmp_return_value = (False, err_str)
        else:
            work_spec.batchID = str(work_spec.workerID)
            tmp_log.debug('Created formation {0}'.format(work_spec.workerID, work_spec.batchID))
            tmp_return_value = (True, '')

        return tmp_return_value

    # submit workers
    def submit_workers(self, workspec_list):
        tmp_log = self.make_logger(base_logger, method_name='submit_workers')

        n_workers = len(workspec_list)
        tmp_log.debug('start, n_workers={0}'.format(n_workers))

        ret_list = list()
        if not workspec_list:
            tmp_log.debug('empty workspec_list')
            return ret_list

        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            ret_val_list = thread_pool.map(self.submit_horovod_k8s_formation, workspec_list)
            tmp_log.debug('{0} workers submitted'.format(n_workers))

        ret_list = list(ret_val_list)

        tmp_log.debug('done')

        return ret_list
