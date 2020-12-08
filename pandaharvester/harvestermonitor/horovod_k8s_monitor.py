import datetime

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.worker_errors import WorkerErrors
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.k8s_utils import k8s_Client


# logger
base_logger = core_utils.setup_logger('horovod_monitor')


# monitor for K8S
class HorovodMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        self.k8s_client = k8s_Client(namespace=self.k8s_namespace, config_file=self.k8s_config_file)

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

        # for traditional batch queues
        self._all_pods_list = []

        # for horovod queues
        self._deployments_info_dict = {}

    def check_a_worker(self, work_spec):

        tmp_log = self.make_logger(base_logger, 'workerID={0} batchID={1}'.format(work_spec.workerID, work_spec.batchID),
                                   method_name='check_a_worker')

        worker_id = work_spec.workerID
        head_pod = self._deployments_info_dict[worker_id]['head_pod']
        worker_deployment = self._deployments_info_dict[worker_id]['worker_deployment']
        err_str = ''
        pod_names_to_delete_list = []
        head_status = None

        # CHECK THE HEAD
        try:
            # check if head pod has been queued too long
            if self.k8s_client.pod_queued_too_long(head_pod, self.podQueueTimeLimit):
                pod_names_to_delete_list.append(head_pod['name'])

            # make list of status of the pods belonging to our job
            head_status = head_pod['status']
            # containers_state_list.extend(head_pod['containers_state'])
        except Exception as _e:
            err_str = 'Failed to get HEAD POD status for worker_id={0} ; {1}'.format(worker_id, _e)
            tmp_log.error(err_str)
            head_status = None
        else:
            if not head_status:
                # there was no head pod found belonging to our job
                err_str = 'HEAD POD for worker_id={0} not found'.format(worker_id)
                tmp_log.error(err_str)
                tmp_log.info('Force to cancel the worker due to JOB not found')
                head_status = WorkSpec.ST_cancelled
            else:
                # we found the head pod belonging to our job. Obtain the final status
                tmp_log.debug('head_status={0}'.format(head_status))
                new_status, sub_msg = self.check_horovod_head_status(head_status, containers_state_list)
                if sub_msg:
                    err_str += sub_msg
                tmp_log.debug('new_status={0}'.format(new_status))

        # TODO: CHECK THE WORKERS

        # delete pods that have been queueing too long
        if pod_names_to_delete_list:
            tmp_log.debug('Deleting pods queuing too long')
            ret_list = self.k8s_client.delete_pods(pod_names_to_delete_list)
            deleted_pods_list = []
            for item in ret_list:
                if item['errMsg'] == '':
                    deleted_pods_list.append(item['name'])
            tmp_log.debug('Deleted pods queuing too long: {0}'.format(
                            ','.join(deleted_pods_list)))

        # supplemental diag messages
        sup_error_code = WorkerErrors.error_codes.get('GENERAL_ERROR') if err_str else WorkerErrors.error_codes.get('SUCCEEDED')
        sup_error_diag = 'PODs=' + ','.join(pods_sup_diag_list) + ' ; ' + err_str
        work_spec.set_supplemental_error(error_code=sup_error_code, error_diag=sup_error_diag)

        return new_status, err_str

    def check_workers(self, workspec_list):
        tmp_log = self.make_logger(base_logger, 'k8s query', method_name='check_workers')
        tmp_log.debug('start')

        ret_list = list()
        if not workspec_list:
            err_str = 'empty workspec_list'
            tmp_log.debug(err_str)
            ret_list.append(('', err_str))
            return False, ret_list

        self._deployments_info_dict = self.k8s_client.get_horovod_deployments_info(workspec_list=workspec_list)

        # resolve status requested workers
        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            ret_iterator = thread_pool.map(self.check_a_worker, workspec_list)

        ret_list = list(ret_iterator)

        tmp_log.debug('done')
        return True, ret_list
