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
            self.x509UserProxy = os.getenv('X509_USER_PROXY')

    def submit_a_job(self, work_spec):
        tmpRetVal = (None, 'Nothing done')

        try:
            job_id = self.k8s_client.create_job_from_yaml(self.k8s_yaml_file, str(work_spec.workerID), work_spec.computingSite, self.x509UserProxy)
        except Exception as _e:
            errStr = 'Failed to create a JOB; {0}'.format(_e)
            tmpRetVal = (False, errStr)
        else:
            work_spec.batchID = job_id
            tmpRetVal = (True, '')

        return tmpRetVal


    # submit workers
    def submit_workers(self, workspec_list):
        tmpLog = self.make_logger(baseLogger, method_name='submit_workers')

        nWorkers = len(workspec_list)
        tmpLog.debug('start, nWorkers={0}'.format(nWorkers))

        retList = list()
        if not workspec_list:
            tmpLog.debug('empty workspec_list')
            return retList

        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            retValList = thread_pool.map(self.submit_a_job, workspec_list)  
        tmpLog.debug('{0} workers submitted'.format(nWorkers))

        retList = list(retValList)

        tmpLog.debug('done')

        return retList
