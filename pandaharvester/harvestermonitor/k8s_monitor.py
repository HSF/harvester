import datetime

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.worker_errors import WorkerErrors
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.k8s_utils import k8s_Client
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict

# logger
base_logger = core_utils.setup_logger('k8s_monitor')


# monitor for K8S
class K8sMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        self.panda_queues_dict = PandaQueuesDict()

        # retrieve the k8s namespace from CRIC
        namespace = self.panda_queues_dict.get_k8s_namespace(self.queueName)

        self.k8s_client = k8s_Client(namespace=namespace, config_file=self.k8s_config_file)

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

    def check_pods_status(self, pods_status_list, containers_state_list):
        sub_msg = ''

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
                if all((item.terminated is not None and item.terminated.reason == 'Completed') for item in containers_state_list):
                    new_status = WorkSpec.ST_finished
                else:
                    sub_mesg_list = []
                    for item in containers_state_list:
                        msg_str = ''
                        if item.terminated is None:
                            state = 'UNKNOWN'
                            if item.running is not None:
                                state = 'running'
                            elif item.waiting is not None:
                                state = 'waiting'
                            msg_str = 'container not terminated yet ({0}) while pod Succeeded'.format(state)
                        elif item.terminated.reason != 'Completed':
                            msg_str = 'container termiated by k8s for reason {0}'.format(item.terminated.reason)
                        sub_mesg_list.append(msg_str)
                    sub_msg = ';'.join(sub_mesg_list)
                    new_status = WorkSpec.ST_cancelled
            elif 'Running' in pods_status_list:
                new_status = WorkSpec.ST_running
            elif 'Failed' in pods_status_list:
                new_status = WorkSpec.ST_failed
            else:
                new_status = WorkSpec.ST_idle

        return new_status, sub_msg

    def check_a_worker(self, workspec):
        # set logger
        tmp_log = self.make_logger(base_logger, 'workerID={0} batchID={1}'.format(workspec.workerID, workspec.batchID),
                                   method_name='check_a_worker')

        # initialization
        job_id = workspec.batchID
        err_str = ''
        time_now = datetime.datetime.utcnow()
        pods_status_list = []
        pods_name_to_delete_list = []

        try:
            pods_list = self.k8s_client.filter_pods_info(self._all_pods_list, job_name=job_id)
            containers_state_list = []
            pods_sup_diag_list = []
            for pods_info in pods_list:
                # make a list of pods that have been queued too long
                if pods_info['status'] in ['Pending', 'Unknown'] and pods_info['start_time'] \
                        and time_now - pods_info['start_time'] > datetime.timedelta(seconds=self.podQueueTimeLimit):
                    # fetch queuing too long pods
                    pods_name_to_delete_list.append(pods_info['name'])
                # make list of status of the pods belonging to our job
                pods_status_list.append(pods_info['status'])
                containers_state_list.extend(pods_info['containers_state'])
                pods_sup_diag_list.append(pods_info['name'])
        except Exception as _e:
            err_str = 'Failed to get POD status of JOB id={0} ; {1}'.format(job_id, _e)
            tmp_log.error(err_str)
            new_status = None
        else:
            if not pods_status_list:
                # there were no pods found belonging to our job
                err_str = 'JOB id={0} not found'.format(job_id)
                tmp_log.error(err_str)
                tmp_log.info('Force to cancel the worker due to JOB not found')
                new_status = WorkSpec.ST_cancelled
            else:
                # we found pods belonging to our job. Obtain the final status
                tmp_log.debug('pods_status_list={0}'.format(pods_status_list))
                new_status, sub_msg = self.check_pods_status(pods_status_list, containers_state_list)
                if sub_msg:
                    err_str += sub_msg
                tmp_log.debug('new_status={0}'.format(new_status))

            # delete pods that have been queueing too long
            if pods_name_to_delete_list:
                tmp_log.debug('Deleting pods queuing too long')
                ret_list = self.k8s_client.delete_pods(pods_name_to_delete_list)
                deleted_pods_list = []
                for item in ret_list:
                    if item['errMsg'] == '':
                        deleted_pods_list.append(item['name'])
                tmp_log.debug('Deleted pods queuing too long: {0}'.format(
                                ','.join(deleted_pods_list)))
            # supplemental diag messages
            sup_error_code = WorkerErrors.error_codes.get('GENERAL_ERROR') if err_str else WorkerErrors.error_codes.get('SUCCEEDED')
            sup_error_diag = 'PODs=' + ','.join(pods_sup_diag_list) + ' ; ' + err_str
            workspec.set_supplemental_error(error_code=sup_error_code, error_diag=sup_error_diag)

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

        self._all_pods_list = self.k8s_client.get_pods_info(workspec_list=workspec_list)

        # resolve status requested workers
        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            ret_iterator = thread_pool.map(self.check_a_worker, workspec_list)

        ret_list = list(ret_iterator)

        tmp_log.debug('done')
        return True, ret_list
