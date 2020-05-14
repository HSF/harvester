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

# logger
base_logger = core_utils.setup_logger('k8s_submitter')

# image defaults
DEF_SLC6_IMAGE = 'atlasadc/atlas-grid-slc6'
DEF_CENTOS7_IMAGE = 'atlasadc/atlas-grid-centos7'
DEF_IMAGE = DEF_CENTOS7_IMAGE

# command defaults
DEF_COMMAND = ["/usr/bin/bash"]
DEF_ARGS = ["-c", "cd; wget https://raw.githubusercontent.com/HSF/harvester/master/pandaharvester/harvestercloud/pilots_starter.py; chmod 755 pilots_starter.py; ./pilots_starter.py || true"]


# submitter for K8S
class K8sSubmitter(PluginBase):
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
        # x509 proxy
        try:
            self.x509UserProxy
        except AttributeError:
            if os.getenv('X509_USER_PROXY'):
                self.x509UserProxy = os.getenv('X509_USER_PROXY')

        # CPU adjust ratio
        try:
            self.cpu_adjust_ratio
        except AttributeError:
            self.cpu_adjust_ratio = 100

        # Memory adjust ratio
        try:
            self.memory_adjust_ratio
        except AttributeError:
            self.memory_adjust_ratio = 100

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

    def decide_container_image(self, job_fields, job_pars_parsed):
        """
        Decide container image:
        - job defined image: if we are running in push mode and the job specified an image, use it
        - production images: take SLC6 or CentOS7
        - otherwise take default image specified for the queue
        """
        tmp_log = self.make_logger(base_logger, method_name='decide_container_image')
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

    def submit_k8s_worker(self, work_spec):
        tmp_log = self.make_logger(base_logger, method_name='submit_k8s_worker')

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

            if hasattr(self, 'proxySecretPath'):
                cert = self.proxySecretPath
                use_secret = True
            elif hasattr(self, 'x509UserProxy'):
                cert = self.x509UserProxy
                use_secret = False
            else:
                err_str = 'No proxy specified in proxySecretPath or x509UserProxy; not submitted'
                tmp_return_value = (False, err_str)
                return tmp_return_value

            rsp, yaml_content_final = self.k8s_client.create_job_from_yaml(yaml_content, work_spec, container_image,
                                                                           executable, args,
                                                                           cert, cert_in_secret=use_secret,
                                                                           cpu_adjust_ratio=self.cpu_adjust_ratio,
                                                                           memory_adjust_ratio=self.memory_adjust_ratio)
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
        tmp_log = self.make_logger(base_logger, method_name='submit_workers')

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
