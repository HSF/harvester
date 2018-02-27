import os.path
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils

baseLogger = core_utils.setup_logger('google_monitor')

class GoogleMonitor(PluginBase):
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.compute = googleapiclient.discovery.build('compute', 'v1')

        # TODO: Figure out the correct states for Google VMs
        self.vm_to_worker_status = {
                                     'RUNNING': WorkSpec.ST_running,
                                     'FINISHED': WorkSpec.ST_finished,
                                     'FAILED': WorkSpec.ST_failed,
                                     'CANCELLED': WorkSpec.ST_cancelled,
                                     'STARTING': WorkSpec.ST_submitted
                                     }

    def list_vms(self):
        """
        List the status of the running VMs
        :return:
        """

        try:
            result = self.compute.instances().list(project=project, zone=zone).execute()
            baseLogger.debug('VM instances: {0}'.format(result))

            vm_instances = result['items']

            # make a list with the VM names
            vm_names = map(lambda vm_instance: vm_instance['name'], vm_instances)
            baseLogger.debug('VM names: {0}'.format(vm_names))

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
        Worker status needs to be one of

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
                new_status = WorkSpec.ST_missed # TODO: check with Tadashi if this state can be used
            else:
                try:
                    new_status = self.vm_to_worker_status[vm_name_to_status[batch_ID]]
                except KeyError:
                    # TODO: check with Tadashi if this state can be used. The VM should be killed
                    new_status = WorkSpec.ST_idle

            tmp_log.debug('new_status={0}'.format(new_status))
            ret_list.append((new_status, ''))

        return True, ret_list
