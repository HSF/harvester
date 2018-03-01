"""
Based on: https://cloud.google.com/compute/docs/tutorials/python-guide#before-you-begin
"""

import time
import os
import googleapiclient.discovery

from concurrent.futures import ProcessPoolExecutor as Pool

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercloud.googlecloud import GoogleVM, ZONE, PROJECT

# setup base logger
base_logger = core_utils.setup_logger('google_submitter')

SERVICE_ACCOUNT_FILE = harvester_config.googlecloud.service_account_file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_FILE
compute = googleapiclient.discovery.build('compute', 'v1')

def wait_for_operation(project, zone, operation_name):
    """
    Waits for an operation to complete.
    TODO: decide whether we want to block or just move on and list the instance status later
    :param project:
    :param zone:
    :param operation_name:
    :return:
    """
    tmp_log = core_utils.make_logger(base_logger, method_name='wait_for_operation')
    tmp_log.debug('Waiting for operation to finish...')

    while True:
        result = compute.zoneOperations().get(project=project, zone=zone, operation=operation_name).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)

    tmp_log.debug('Operation finished...')


def create_vm(work_spec):
    """
    Boots up a VM in GCE based on the worker specifications

    :param work_spec: worker specifications
    :return:
    """
    work_spec.reset_changed_list()

    tmp_log = core_utils.make_logger(base_logger, 'workerID={0}'.format(work_spec.workerID),
                                     method_name='submit_a_worker')

    tmp_log.debug('nCore={0} minRamCount={1} maxDiskCount={2} maxWalltime={0}'.format(work_spec.nCore,
                                                                                      work_spec.minRamCount,
                                                                                      work_spec.maxDiskCount,
                                                                                      work_spec.maxWalltime))

    vm = GoogleVM(work_spec)
    # tmp_log.debug('vm.config: {0}'.format(vm.config))

    tmp_log.debug('Going to submit VM {0}'.format(vm.name))
    operation = compute.instances().insert(project=PROJECT, zone=ZONE, body=vm.config).execute()
    tmp_log.debug('Submitting VM {0}'.format(vm.name))
    wait_for_operation(PROJECT, ZONE, operation['name'])
    tmp_log.debug('Submitted VM {0}'.format(vm.name))

    #work_spec.batchID = operation['targetId']
    work_spec.batchID = vm.name
    tmp_log.debug('booted VM {0}'.format(vm.name))

    return (True, 'OK'), work_spec.get_changed_attributes()


class GoogleSubmitter(PluginBase):
    """
    Plug-in for Google Cloud Engine VM submission. In this case the worker will abstract a VM running a job
    """

    def __init__(self, **kwarg):
        self.logBaseURL = 'http://localhost/test'
        PluginBase.__init__(self, **kwarg)

    def submit_workers(self, work_spec_list):
        """
        :param work_spec_list: list of workers to submit
        :return:
        """
        tmp_log = core_utils.make_logger(base_logger, method_name='submit_workers')
        tmp_log.debug('start nWorkers={0}'.format(len(work_spec_list)))

        # Create VMs in parallel
        pool_size = min(len(work_spec_list), 10) # TODO: think about the optimal pool size
        with Pool(pool_size) as pool:
            ret_val_list = pool.map(create_vm, work_spec_list)

        # Propagate changed attributes
        ret_list = []
        for work_spec, tmp_val in zip(work_spec_list, ret_val_list):
            ret_val, tmp_dict = tmp_val

            work_spec.set_attributes_with_dict(tmp_dict)
            work_spec.set_log_file('batch_log', '{0}/{1}.log'.format(self.logBaseURL, work_spec.batchID))
            work_spec.set_log_file('stdout', '{0}/{1}.out'.format(self.logBaseURL, work_spec.batchID))
            work_spec.set_log_file('stderr', '{0}/{1}.err'.format(self.logBaseURL, work_spec.batchID))
            ret_list.append(ret_val)

        tmp_log.debug('done')

        return ret_list


