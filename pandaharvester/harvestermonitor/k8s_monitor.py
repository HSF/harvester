import datetime

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.k8s_utils import k8s_Client


# logger
base_logger = core_utils.setup_logger('k8s_monitor')


# monitor for K8S
class K8sMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        self.k8s_client = k8s_Client(self.k8s_namespace, config_file=self.k8s_config_file)

        try:
            self.nProcesses
        except AttributeError:
            self.nProcesses = 4
        try:
            self.cancelUnknown
        except AttributeError:
            self.cancelUnknown = False
        else:
            self.cancelUnknown = bool(self.cancelUnknown)
        try:
            self.podQueueTimeLimit
        except AttributeError:
            self.podQueueTimeLimit = 172800

        self._all_pods_list = []

    def check_pods_status(self, pods_status_list):
        if 'Unknown' in pods_status_list:
            if all(item == 'Unknown' for item in pods_status_list):
                new_status = None
            elif 'Running' in pods_status_list:
                new_status = WorkSpec.ST_running
            else:
                new_status = WorkSpec.ST_idle
        else:
            if all(item == 'Pending' for item in pods_status_list):
                new_status = WorkSpec.ST_submitted
            # elif all(item == 'Succeeded' for item in pods_status_list):
            #    new_status = WorkSpec.ST_finished
            elif 'Succeeded' in pods_status_list:
                new_status = WorkSpec.ST_finished
            elif 'Running' in pods_status_list:
                new_status = WorkSpec.ST_running
            elif 'Failed' in pods_status_list:
                new_status = WorkSpec.ST_failed
            else:
                new_status = WorkSpec.ST_idle

        return new_status

    def check_a_job(self, workspec):
        # set logger
        tmp_log = self.make_logger(base_logger, 'workerID={0} batchID={1}'.format(workspec.workerID, workspec.batchID),
                                  method_name='check_a_job')

        ## initialization
        job_id = workspec.batchID
        err_str = ''

        try:
            pods_list = self.k8s_client.filter_pods_info(self._all_pods_list, job_name=job_id)
            time_now = datetime.datetime.utcnow()
            pods_status_list = []
            pods_name_to_delete_list = []
            for pods_info in pods_list:
                if pods_info['status'] in ['Pending', 'Unknown'] and pods_info['start_time'] \
                 and time_now - pods_info['start_time'] > datetime.timedelta(seconds=self.podQueueTimeLimit):
                    # fetch queuing too long pods
                    pods_name_to_delete_list.append(pods_info['name'])
                pods_status_list.append(pods_info['status'])
        except Exception as _e:
            err_str = 'Failed to get POD status of JOB id={0} ; {1}'.format(job_id, _e)
            tmp_log.error(err_str)
            new_status = None
        else:
            if not pods_status_list:
                err_str = 'JOB id={0} not found'.format(job_id)
                tmp_log.error(err_str)
                tmp_log.info('Force to cancel the worker due to JOB not found')
                new_status = WorkSpec.ST_cancelled
            else:
                tmp_log.debug('pods_status_list={0}'.format(pods_status_list))
                new_status = self.check_pods_status(pods_status_list)
                tmp_log.debug('new_status={0}'.format(new_status))
            # delete queuing too long pods
            if pods_name_to_delete_list:
                tmp_log.debug('Deleting pods queuing too long')
                ret_list = self.k8s_client.delete_pods(pods_name_to_delete_list)
                deleted_pods_list = []
                for item in ret_list:
                    if item['errMsg'] == '':
                        deleted_pods_list.append(item['name'])
                tmp_log.debug('Deleted pods queuing too long: {0}'.format(
                                ','.join(deleted_pods_list)))

        return new_status, err_str

    # check workers
    def check_workers(self, workspec_list):
        tmp_log = self.make_logger(base_logger, 'k8s query', method_name='check_workers')
        tmp_log.debug('start')

        ret_list = list()
        if not workspec_list:
            err_str = 'empty workspec_list'
            tmp_log.debug(err_str)
            ret_list.append(('', err_str))
            return False, ret_list

        self._all_pods_list = self.k8s_client.get_pods_info()

        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            ret_iterator = thread_pool.map(self.check_a_job, workspec_list)

        ret_list = list(ret_iterator)

        tmp_log.debug('done')

        return True, ret_list
