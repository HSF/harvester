import os
import time
import datetime
import re

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.k8s_utils import k8s_Client


# logger
baseLogger = core_utils.setup_logger('k8s_monitor')


# monitor for K8S
class K8sMonitor(PluginBase):
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

        self._all_pods_list = []

    def check_pods_status(self, pods_status_list):
        newStatus = ''

        if 'Unknown' in pods_status_list:
            if all(item == 'Unknown' for item in pods_status_list):
                newStatus = None
            elif 'Running' in pods_status_list:
                newStatus = WorkSpec.ST_running
            else:
                newStatus = WorkSpec.ST_idle
        else:
            if all(item == 'Pending' for item in pods_status_list):
                newStatus = WorkSpec.ST_submitted
            elif all(item == 'Succeeded' for item in pods_status_list):
                newStatus = WorkSpec.ST_finished
            elif 'Running' in pods_status_list:
                newStatus = WorkSpec.ST_running
            elif 'Failed' in pods_status_list:
                newStatus = WorkSpec.ST_failed
            else:
                newStatus = WorkSpec.ST_idle

        return newStatus

    def check_a_job(self, workspec):
        # set logger
        tmpLog = self.make_logger(baseLogger, 'workerID={0} batchID={1}'.format(workspec.workerID, workspec.batchID),
                                  method_name='check_a_job')

        ## initialization
        job_id = workspec.batchID
        newStatus = workspec.status
        errStr = ''

        try:
            pods_list = self.k8s_client.filter_pods_info(self._all_pods_list, job_name=job_id)
            timeNow = datetime.datetime.utcnow()
            pods_status_list = []
            pods_name_to_delete_list = []
            for pods_info in pods_list:
                if pods_info['status'] in ['Pending', 'Unknown'] and pods_info['start_time'] \
                    and timeNow - pods_info['start_time'] > datetime.timedelta(seconds=self.podQueueTimeLimit):
                    # fetch queuing too long pods
                    pods_name_to_delete_list.append(pods_info['name'])
                pods_status_list.append(pods_info['status'])
        except Exception as _e:
            errStr = 'Failed to get POD status of JOB id={0} ; {1}'.format(job_id, _e)
            tmpLog.error(errStr)
            newStatus = None
        else:
            if not pods_status_list:
                errStr = 'JOB id={0} not found'.format(job_id)
                tmpLog.error(errStr)
                tmpLog.info('Force to cancel the worker due to JOB not found')
                newStatus = WorkSpec.ST_cancelled
            else:
                newStatus = self.check_pods_status(pods_status_list)
                tmpLog.debug('new_status={0}'.format(newStatus))
            # delete queuing too long pods
            if pods_name_to_delete_list:
                tmpLog.debug('Deleting pods queuing too long')
                retList = self.k8s_client.delete_pods(pods_name_to_delete_list)
                deleted_pods_list = []
                for item in retList:
                    if item['errMsg'] == '':
                        deleted_pods_list.append(item['name'])
                tmpLog.debug('Deleted pods queuing too long: {0}'.format(
                                ','.join(deleted_pods_list)))

        return (newStatus, errStr)


    # check workers
    def check_workers(self, workspec_list):
        tmpLog = self.make_logger(baseLogger, 'k8s query', method_name='check_workers')
        tmpLog.debug('start')

        retList = list()
        if not workspec_list:
            errStr = 'empty workspec_list'
            tmpLog.debug(errStr)
            retList.append(('', errStr))
            return False, retList

        self._all_pods_list = self.k8s_client.get_pods_info()

        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            retIterator = thread_pool.map(self.check_a_job, workspec_list)

        retList = list(retIterator)

        tmpLog.debug('done')

        return True, retList
