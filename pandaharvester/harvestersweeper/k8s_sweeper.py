import os

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestersweeper.base_sweeper import BaseSweeper
from pandaharvester.harvestermisc.k8s_utils import k8s_Client


# logger
baseLogger = core_utils.setup_logger('k8s_sweeper')


# sweeper for K8S
class K8sSweeper(BaseSweeper):
    # constructor
    def __init__(self, **kwarg):
        BaseSweeper.__init__(self, **kwarg)

        self.k8s_client = k8s_Client(namespace=self.k8s_namespace, config_file=self.k8s_config_file)

        self._all_pods_list = []

    # # kill a worker
    # def kill_worker(self, workspec):
    #     tmpLog = self.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
    #                               method_name='kill_worker')
    #
    #     tmpRetVal = (None, 'Nothing done')
    #
    #     job_id = workspec.batchID
    #     try:
    #         self.k8s_client.delete_job(job_id)
    #     except Exception as _e:
    #         errStr = 'Failed to delete a JOB with id={0} ; {1}'.format(job_id, _e)
    #         tmpLog.error(errStr)
    #         tmpRetVal = (False, errStr)
    #
    #     self._all_pods_list = self.k8s_client.get_pods_info()
    #     pods_list = self.k8s_client.filter_pods_info(self._all_pods_list, job_name=job_id)
    #     pods_name = [ pods_info['name'] for pods_info in pods_list ]
    #     job_info = self.k8s_client.get_jobs_info(job_id)
    #
    #     if not job_info:
    #         retList = self.k8s_client.delete_pods(pods_name)
    #         if all(item['errMsg'] == '' for item in retList):
    #             tmpLog.info('Deleted a JOB & POD with id={0}'.format(job_id))
    #             tmpRetVal = (True, '')
    #         else:
    #             errStrList = list()
    #             for item in retList:
    #                 if item['errMsg']:
    #                     errStr = 'Failed to delete a POD with id={0} ; {1}'.format(item['name'], item['errMsg'])
    #                     tmpLog.error(errStr)
    #                     errStrList.append(errStr)
    #             tmpRetVal = (False, ','.join(errStrList))
    #
    #     return tmpRetVal

    # kill workers
    def kill_workers(self, workspec_list):
        tmpLog = self.make_logger(baseLogger, method_name='kill_workers')

        self._all_pods_list = self.k8s_client.get_pods_info()

        retList = []
        for workspec in workspec_list:
            tmpRetVal = (None, 'Nothing done')

            job_id = workspec.batchID
            try:
                self.k8s_client.delete_job(job_id)
            except Exception as _e:
                errStr = 'Failed to delete a JOB with id={0} ; {1}'.format(job_id, _e)
                tmpLog.error(errStr)
                tmpRetVal = (False, errStr)

            pods_list = self.k8s_client.filter_pods_info(self._all_pods_list, job_name=job_id)
            pods_name = [ pods_info['name'] for pods_info in pods_list ]
            job_info = self.k8s_client.get_jobs_info(job_id)

            if not job_info:
                retList = self.k8s_client.delete_pods(pods_name)
                if all(item['errMsg'] == '' for item in retList):
                    tmpLog.info('Deleted a JOB & POD with id={0}'.format(job_id))
                    tmpRetVal = (True, '')
                else:
                    errStrList = list()
                    for item in retList:
                        if item['errMsg']:
                            errStr = 'Failed to delete a POD with id={0} ; {1}'.format(item['name'], item['errMsg'])
                            tmpLog.error(errStr)
                            errStrList.append(errStr)
                    tmpRetVal = (False, ','.join(errStrList))

            retList.append(tmpRetVal)

        return retList


    # cleanup for a worker
    def sweep_worker(self, workspec):
        ## Make logger
        tmpLog = self.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
                                  method_name='sweep_worker')

        # nothing to do
        return True, ''
