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
from pandaharvester.harvestersubmitter import submitter_common

# logger
base_logger = core_utils.setup_logger('k8s_submitter')

# image defaults
DEF_SLC6_IMAGE = 'atlasadc/atlas-grid-slc6'
DEF_CENTOS7_IMAGE = 'atlasadc/atlas-grid-centos7'
DEF_IMAGE = DEF_CENTOS7_IMAGE

# command defaults
DEF_COMMAND = ["/usr/bin/bash"]
DEF_ARGS = ["-c", "cd; python $EXEC_DIR/pilots_starter.py || true"]


# submitter for K8S
class K8sSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)

        self.panda_queues_dict = PandaQueuesDict()

        # retrieve the k8s namespace from CRIC
        namespace = self.panda_queues_dict.get_k8s_namespace(self.queueName)

        self.k8s_client = k8s_Client(namespace=namespace, queue_name=self.queueName, config_file=self.k8s_config_file)

        # update or create the pilot starter executable
        self.k8s_client.create_or_patch_configmap_starter()

        # required for parsing jobParams
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('-p', dest='executable', type=unquote)
        self.parser.add_argument('--containerImage', dest='container_image')

        # allowed associated parameters from AGIS
        self._allowed_agis_attrs = (
                'pilot_url',
            )

        # number of processes
        try:
            self.nProcesses
        except AttributeError:
            self.nProcesses = 1
        else:
            if (not self.nProcesses) or (self.nProcesses < 1):
                self.nProcesses = 1

        # x509 proxy through k8s secrets: preferred way
        try:
            self.proxySecretPath
        except AttributeError:
            if os.getenv('PROXY_SECRET_PATH'):
                self.proxySecretPath = os.getenv('PROXY_SECRET_PATH')

        # analysis x509 proxy through k8s secrets: on GU queues
        try:
            self.proxySecretPathAnalysis
        except AttributeError:
            if os.getenv('PROXY_SECRET_PATH_ANAL'):
                self.proxySecretPath = os.getenv('PROXY_SECRET_PATH_ANAL')

    def parse_params(self, job_params):
        tmp_log = self.make_logger(base_logger, 'queueName={0}'.format(self.queueName), method_name='parse_params')

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

    def decide_container_image(self, job_fields, job_pars_parsed):
        """
        Decide container image:
        - job defined image: if we are running in push mode and the job specified an image, use it
        - production images: take SLC6 or CentOS7
        - otherwise take default image specified for the queue
        """
        tmp_log = self.make_logger(base_logger, 'queueName={0}'.format(self.queueName), method_name='decide_container_image')
        try:
            container_image = job_pars_parsed.container_image
            if container_image:
                tmp_log.debug('Taking container image from job params: {0}'.format(container_image))
                return container_image
        except AttributeError:
            pass

        try:
            cmt_config = job_fields['cmtconfig']
            requested_os = cmt_config.split('@')[1]
            if 'slc6' in requested_os.lower():
                container_image = DEF_SLC6_IMAGE
            else:
                container_image = DEF_CENTOS7_IMAGE
            tmp_log.debug('Taking container image from cmtconfig: {0}'.format(container_image))
            return container_image
        except (KeyError, TypeError):
            pass

        container_image = DEF_IMAGE
        tmp_log.debug('Taking default container image: {0}'.format(container_image))
        return container_image

    def build_executable(self, job_fields, job_pars_parsed):
        executable = DEF_COMMAND
        args = DEF_ARGS
        try:
            if 'runcontainer' in job_fields['transformation']:
                # remove any quotes
                exec_list = job_pars_parsed.executable.strip('"\'').split(' ')
                # take first word as executable
                executable = [exec_list[0]]
                # take rest as arguments
                if len(exec_list) > 1:
                    args = [' '.join(exec_list[1:])]
        except (AttributeError, TypeError):
            pass

        return executable, args

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

    def submit_k8s_worker(self, work_spec):
        tmp_log = self.make_logger(base_logger, 'queueName={0}'.format(self.queueName), method_name='submit_k8s_worker')

        # get info from harvester queue config
        _queueConfigMapper = QueueConfigMapper()
        harvester_queue_config = _queueConfigMapper.get_queue(self.queueName)

        # set the stdout log file
        log_file_name = '{0}_{1}.out'.format(harvester_config.master.harvester_id, work_spec.workerID)
        work_spec.set_log_file('stdout', '{0}/{1}'.format(self.logBaseURL, log_file_name))
        # TODO: consider if we want to upload the yaml file to PanDA cache

        yaml_content = self.k8s_client.read_yaml_file(self.k8s_yaml_file)
        try:

            # read the job configuration (if available, only push model)
            job_fields, job_pars_parsed = self.read_job_configuration(work_spec)

            # decide container image and executable to run. In pull mode, defaults are provided
            container_image = self.decide_container_image(job_fields, job_pars_parsed)
            executable, args = self.build_executable(job_fields, job_pars_parsed)
            tmp_log.debug('container_image: "{0}"; executable: "{1}"; args: "{2}"'.format(container_image, executable,
                                                                                          args))

            # choose the appropriate proxy
            this_panda_queue_dict = self.panda_queues_dict.get(self.queueName, dict())

            is_grandly_unified_queue = self.panda_queues_dict.is_grandly_unified_queue(self.queueName)
            cert = self._choose_proxy(work_spec, is_grandly_unified_queue)
            if not cert:
                err_str = 'No proxy specified in proxySecretPath. Not submitted'
                tmp_return_value = (False, err_str)
                return tmp_return_value

            # get the walltime limit
            try:
                max_time = this_panda_queue_dict['maxtime']
            except Exception as e:
                tmp_log.warning('Could not retrieve maxtime field for queue {0}'.format(self.queueName))
                max_time = None

            associated_params_dict = {}
            for key, val in self.panda_queues_dict.get_harvester_params(self.queueName).items():
                if key in self._allowed_agis_attrs:
                    associated_params_dict[key] = val

            pilot_url = associated_params_dict.get('pilot_url')
            pilot_version = str(this_panda_queue_dict.get('pilot_version', 'current'))
            python_version = str(this_panda_queue_dict.get('python_version', '2'))

            # prod_source_label = harvester_queue_config.get_source_label(work_spec.jobType)
            pilot_opt_dict = submitter_common.get_complicated_pilot_options(work_spec.pilotType)
            if pilot_opt_dict is None:
                prod_source_label = harvester_queue_config.get_source_label(work_spec.jobType)
                pilot_type = work_spec.pilotType
                pilot_url_str = '--piloturl {0}'.format(pilot_url) if pilot_url else ''
            else:
                prod_source_label = pilot_opt_dict['prod_source_label']
                pilot_type = pilot_opt_dict['pilot_type_opt']
                pilot_url_str = pilot_opt_dict['pilot_url_str']

            pilot_python_option = submitter_common.get_python_version_option(python_version, prod_source_label)

            # submit the worker
            rsp, yaml_content_final = self.k8s_client.create_job_from_yaml(yaml_content, work_spec, prod_source_label,
                                                                           pilot_type, pilot_url_str,
                                                                           pilot_python_option,
                                                                           container_image, executable, args, cert,
                                                                           max_time=max_time)
        except Exception as _e:
            tmp_log.error(traceback.format_exc())
            err_str = 'Failed to create a JOB; {0}'.format(_e)
            tmp_return_value = (False, err_str)
        else:
            work_spec.batchID = yaml_content['metadata']['name']
            tmp_log.debug('Created worker {0} with batchID={1}'.format(work_spec.workerID, work_spec.batchID))
            tmp_return_value = (True, '')

        return tmp_return_value

    # submit workers
    def submit_workers(self, workspec_list):
        tmp_log = self.make_logger(base_logger, 'queueName={0}'.format(self.queueName), method_name='submit_workers')

        n_workers = len(workspec_list)
        tmp_log.debug('start, n_workers={0}'.format(n_workers))

        ret_list = list()
        if not workspec_list:
            tmp_log.debug('empty workspec_list')
            return ret_list

        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            ret_val_list = thread_pool.map(self.submit_k8s_worker, workspec_list)
            tmp_log.debug('{0} workers submitted'.format(n_workers))

        ret_list = list(ret_val_list)

        tmp_log.debug('done')

        return ret_list
