import os

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.k8s_utils import k8s_Client


# logger
baseLogger = core_utils.setup_logger('k8s_submitter')


# submitter for K8S
class K8sSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)

        self.k8s_client = k8s_Client(namespace=self.k8s_namespace, config_file=self.k8s_config_file)

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

    def submit_a_job(self, work_spec):
        tmp_log = self.make_logger(baseLogger, method_name='submit_a_job')
        tmpRetVal = (None, 'Nothing done')

        yaml_content = self.k8s_client.read_yaml_file(self.k8s_yaml_file)

        try:
            if hasattr(self, 'proxySecretPath'):
                rsp = self.k8s_client.create_job_from_yaml(yaml_content, work_spec, self.proxySecretPath, True, self.cpuAdjustRatio, self.memoryAdjustRatio)
            elif hasattr(self, 'x509UserProxy'):
                rsp = self.k8s_client.create_job_from_yaml(yaml_content, work_spec, self.x509UserProxy, False, self.cpuAdjustRatio, self.memoryAdjustRatio)
            else:
                errStr = 'No proxy specified in proxySecretPath or x509UserProxy; not submitted'
                tmpRetVal = (False, errStr)
        except Exception as _e:
            errStr = 'Failed to create a JOB; {0}'.format(_e)
            tmpRetVal = (False, errStr)
        else:
            work_spec.batchID = yaml_content['metadata']['name']
            tmp_log.debug('Created worker {0} with batchID={1}'.format(work_spec.workerID, work_spec.batchID))
            tmpRetVal = (True, '')

        return tmpRetVal


    # submit workers
    def submit_workers(self, workspec_list):
        tmp_log = self.make_logger(baseLogger, method_name='submit_workers')

        nWorkers = len(workspec_list)
        tmp_log.debug('start, nWorkers={0}'.format(nWorkers))

        retList = list()
        if not workspec_list:
            tmp_log.debug('empty workspec_list')
            return retList

        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            retValList = thread_pool.map(self.submit_a_job, workspec_list)
            tmp_log.debug('{0} workers submitted'.format(nWorkers))

        retList = list(retValList)

        tmp_log.debug('done')

        return retList
