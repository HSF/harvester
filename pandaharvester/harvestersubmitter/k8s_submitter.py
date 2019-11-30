import os

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.k8s_utils import k8s_Client
from pandaharvester.harvesterconfig import harvester_config

# logger
base_logger = core_utils.setup_logger('k8s_submitter')


# submitter for K8S
class K8sSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)

        self.k8s_client = k8s_Client(self.k8s_namespace, config_file=self.k8s_config_file)

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
            self.cpuAdjustRatio
        except AttributeError:
            self.cpuAdjustRatio = 100

        # Memory adjust ratio
        try:
            self.memoryAdjustRatio
        except AttributeError:
            self.memoryAdjustRatio = 100

    def decide_container_image(self, work_spec):
        """
        Decide container image:
        - job defined image: if we are running in push mode and the job specified an image, use it
        - production images: take SLC6 or CentOS7
        - otherwise take default image specified for the queue
        """

        # job defined image
        job_spec_list = work_spec.get_jobspec_list()
        if job_spec_list:
            job_spec = job_spec_list[0]
            container_image = 'ABC' # TODO: parse the container image from the job parameters
            return container_image

        # production images
        # TODO: figure out how this is decided in the pilot
        container_image = 'DEF'
        return container_image

        # take default image for the queue
        container_image = 'GHI'
        return container_image

        # take the overal default image
        container_image = 'JKL'
        return container_image

    def submit_k8s_worker(self, work_spec):
        tmp_log = self.make_logger(base_logger, method_name='submit_k8s_worker')

        yaml_content = self.k8s_client.read_yaml_file(self.k8s_yaml_file)

        try:

            # decide container image to run
            container_image = self.decide_container_image(work_spec)

            if hasattr(self, 'proxySecretPath'):
                cert = self.proxySecretPath
                use_secret = True
            elif hasattr(self, 'x509UserProxy'):
                cert = self.x509UserProxy
                use_secret = False
            else:
                errStr = 'No proxy specified in proxySecretPath or x509UserProxy; not submitted'
                tmp_return_value = (False, errStr)
                return tmp_return_value

            rsp, yaml_content_final = self.k8s_client.create_job_from_yaml(yaml_content, work_spec, container_image,
                                                                           cert, use_secret, self.cpuAdjustRatio,
                                                                           self.memoryAdjustRatio)
        except Exception as _e:
            err_str = 'Failed to create a JOB; {0}'.format(_e)
            tmp_return_value = (False, err_str)
        else:
            work_spec.batchID = yaml_content['metadata']['name']

            # set the log file
            work_spec.set_log_file('stdout', '{0}/{1}.out'.format(self.logBaseURL, work_spec.workerID))
            # TODO: consider if we want to upload the yaml file to PanDA cache

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
            ret_val_list = thread_pool.map(self.submit_a_k8s_job, workspec_list)
            tmp_log.debug('{0} workers submitted'.format(n_workers))

        ret_list = list(ret_val_list)

        tmp_log.debug('done')

        return ret_list
