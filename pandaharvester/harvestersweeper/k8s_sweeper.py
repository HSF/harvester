from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestersweeper.base_sweeper import BaseSweeper
from pandaharvester.harvestermisc.k8s_utils import k8s_Client

# logger
base_logger = core_utils.setup_logger('k8s_sweeper')


# sweeper for K8S
class K8sSweeper(BaseSweeper):
    # constructor
    def __init__(self, **kwarg):
        BaseSweeper.__init__(self, **kwarg)

        self.k8s_client = k8s_Client(self.k8s_namespace, config_file=self.k8s_config_file)

        self._all_pods_list = []

    # # kill a worker
    # def kill_worker(self, work_spec):
    #     tmp_log = self.make_logger(base_logger, 'workerID={0}'.format(work_spec.workerID),
    #                               method_name='kill_worker')
    #
    #     tmp_ret_val = (None, 'Nothing done')
    #
    #     job_id = work_spec.batchID
    #     try:
    #         self.k8s_client.delete_job(job_id)
    #     except Exception as _e:
    #         err_str = 'Failed to delete a JOB with id={0} ; {1}'.format(job_id, _e)
    #         tmp_log.error(err_str)
    #         tmp_ret_val = (False, err_str)
    #
    #     self._all_pods_list = self.k8s_client.get_pods_info()
    #     pods_list = self.k8s_client.filter_pods_info(self._all_pods_list, job_name=job_id)
    #     pods_name = [ pods_info['name'] for pods_info in pods_list ]
    #     job_info = self.k8s_client.get_jobs_info(job_id)
    #
    #     if not job_info:
    #         ret_list = self.k8s_client.delete_pods(pods_name)
    #         if all(item['errMsg'] == '' for item in ret_list):
    #             tmp_log.info('Deleted a JOB & POD with id={0}'.format(job_id))
    #             tmp_ret_val = (True, '')
    #         else:
    #             err_str_list = list()
    #             for item in ret_list:
    #                 if item['errMsg']:
    #                     err_str = 'Failed to delete a POD with id={0} ; {1}'.format(item['name'], item['errMsg'])
    #                     tmp_log.error(err_str)
    #                     err_str_list.append(err_str)
    #             tmp_ret_val = (False, ','.join(err_str_list))
    #
    #     return tmp_ret_val

    # kill workers
    def kill_workers(self, work_spec_list):
        tmp_log = self.make_logger(base_logger, method_name='kill_workers')

        self._all_pods_list = self.k8s_client.get_pods_info()

        ret_list = []
        for work_spec in work_spec_list:
            tmp_ret_val = (None, 'Nothing done')
            job_spec_list = work_spec.get_jobspec_list()

            # if push mode, delete the configmap
            if job_spec_list:
                job_spec = job_spec_list[0]
                panda_id = str(job_spec.PandaID)
                try:
                    self.k8s_client.delete_config_map(panda_id)
                    tmp_log.debug('Deleted configmap {0}'.format(panda_id))
                except Exception as _e:
                    err_str = 'Failed to delete a CONFIGMAP with id={0} ; {1}'.format(panda_id, _e)
                    tmp_log.error(err_str)
                    tmp_ret_val = (False, err_str)

            job_id = work_spec.batchID
            if job_id:  # sometimes there are missed workers that were not submitted

                # delete the job
                try:
                    self.k8s_client.delete_job(job_id)
                    tmp_log.debug('Deleted job {0}'.format(job_id))
                except Exception as _e:
                    err_str = 'Failed to delete a JOB with id={0} ; {1}'.format(job_id, _e)
                    tmp_log.error(err_str)
                    tmp_ret_val = (False, err_str)

                # retrieve the associated pods
                pods_list = self.k8s_client.filter_pods_info(self._all_pods_list, job_name=job_id)
                pods_name = [pods_info['name'] for pods_info in pods_list]
                job_info = self.k8s_client.get_jobs_info(job_id)
                # retrieve the associated pods
                if not job_info:
                    ret_list = self.k8s_client.delete_pods(pods_name)
                    if all(item['errMsg'] == '' for item in ret_list):
                        tmp_log.info('Deleted a JOB & POD with id={0}'.format(job_id))
                        tmp_ret_val = (True, '')
                    else:
                        err_str_list = list()
                        for item in ret_list:
                            if item['errMsg']:
                                err_str = 'Failed to delete a POD with id={0} ; {1}'.format(item['name'], item['errMsg'])
                                tmp_log.error(err_str)
                                err_str_list.append(err_str)
                        tmp_ret_val = (False, ','.join(err_str_list))
            else:  # the worker cannot be cleaned
                tmp_ret_val = (True, '')

            ret_list.append(tmp_ret_val)

        return ret_list

    def sweep_worker(self, work_spec):
        # cleanup for a worker
        tmp_log = self.make_logger(base_logger, 'workerID={0}'.format(work_spec.workerID), method_name='sweep_worker')

        # retrieve and upload the logs to panda cache
        # job_id = work_spec.batchID
        # log_content = self.k8s_client.retrieve_pod_log(job_id)

        # nothing to do
        return True, ''
