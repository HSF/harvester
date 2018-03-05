from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercloud.googlecloud import compute, ZONE, PROJECT

base_logger = core_utils.setup_logger('google_monitor')

class GoogleSweeper(PluginBase):
    """
    Sweeper with kill/clean-up functions for Google Compute Engine
    """
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    def kill_worker(self, work_spec):
        """
        Sends the command to Google to destroy a VM

        :param work_spec: worker specification
        :type work_spec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """

        try:
            vm_name = work_spec.batchID
            base_logger.debug('Going to kill VM {0}'.format(vm_name))
            compute.instances().delete(project=PROJECT, zone=ZONE, instance=vm_name).execute()
            base_logger.debug('Killed VM {0}'.format(vm_name))
            return True, ''
        except:
            return False, 'Problem occurred deleting the instance'

    def sweep_worker(self, work_spec):
        """
        In the cloud, cleaning means destroying a VM

        :param work_spec: worker specification
        :type work_spec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        return self.kill_worker(work_spec)
