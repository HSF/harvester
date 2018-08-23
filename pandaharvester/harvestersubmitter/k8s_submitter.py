import os

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvesterconfig import harvester_config
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

        self.k8s_client = k8s_Client()

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

    # submit workers
    def submit_workers(self, workspec_list):
        tmpLog = self.make_logger(baseLogger, method_name='submit_workers')

        nWorkers = len(workspec_list)
        tmpLog.debug('start, nWorkers={0}'.format(nWorkers))

        retList = list()
        if not workspec_list:
            tmp_log.debug('empty workspec_list')
            return retList

        for work_spec in workspec_list:
            try:
                job_id = self.k8s_client.create_job_from_yaml(harvester_config.k8s.YAMLFile, work_spec, self.x509UserProxy)
            except Exception as _e:
                errStr = 'Failed to create a JOB; {0}'.format(_e)
                retList.append((False, errStr))
            else:
                work_spec.batchID = job_id
                pods_list = self.k8s_client.get_pods_info(job_id)
                work_spec.set_work_params("pod_name") = [ pods_info['name'] for pods_info in pods_list ]
                retList.append((True, ''))
        tmpLog.debug('{0} workers submitted'.format(nWorkers))

        tmpLog.debug('done')

        return retList
