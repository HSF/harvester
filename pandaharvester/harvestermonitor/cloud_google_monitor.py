from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercloud.googlecloud import compute, ZONE, PROJECT

baseLogger = core_utils.setup_logger('google_monitor')

class GoogleMonitor(PluginBase):
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        # States taken from: https://cloud.google.com/compute/docs/instances/checking-instance-status
        self.vm_to_worker_status = {
                                     'RUNNING': WorkSpec.ST_running,
                                     'TERMINATED': WorkSpec.ST_finished,
                                     'STOPPING': WorkSpec.ST_finished,
                                     'PROVISIONING': WorkSpec.ST_submitted,
                                     'STAGING': WorkSpec.ST_submitted
                                     }

    def list_vms(self):
        """
        List the status of the running VMs
        :return:
        """

        try:
            result = compute.instances().list(project=PROJECT, zone=ZONE).execute()
            vm_instances = result['items']

            # make a list with the VM names
            vm_names = map(lambda vm_instance: vm_instance['name'], vm_instances)

            # make a dictionary so we can retrieve a VM by its name
            vm_name_to_status = {}
            for vm_instance in vm_instances:
                vm_name_to_status[vm_instance['name']] = vm_instance['status']

            return vm_names, vm_name_to_status

        except:
            return None, None

    def check_workers(self, workers):
        """
        This method takes a list of WorkSpecs as input argument and returns a list of worker's statuses.
        Nth element in the return list corresponds to the status of Nth WorkSpec in the given list.

        :param worker_list: a list of work specs instances
        :return: A tuple containing the return code (True for success, False otherwise) and a list of worker's statuses
        :rtype: (bool, [string,])
        """

        # running instances
        vm_names, vm_name_to_status = self.list_vms()
        if vm_names is None and vm_name_to_status is None:
            error_string = 'Could not list the VMs'
            baseLogger.error(error_string)
            return False, error_string

        # extract the list of batch IDs
        batch_IDs = map(lambda x: str(x.batchID), workers)
        baseLogger.debug('Batch IDs: {0}'.format(batch_IDs))

        ret_list = []
        for batch_ID in batch_IDs:
            tmp_log = core_utils.make_logger(baseLogger, 'batch ID={0}'.format(batch_ID), method_name='check_workers')

            if batch_ID not in vm_names:
                new_status = WorkSpec.ST_finished
                message = 'VM not found'
            else:
                try:
                    new_status = self.vm_to_worker_status[vm_name_to_status[batch_ID]]
                    message = 'VM status returned by GCE API'
                except KeyError:
                    new_status = WorkSpec.ST_missed
                    message = 'Unknown status to Harvester: {0}'.format(vm_name_to_status[batch_ID])

            tmp_log.debug('new_status={0}'.format(new_status))
            ret_list.append((new_status, message))

        baseLogger.debug('ret_list: {0}'.format(ret_list))
        return True, ret_list
