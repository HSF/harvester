from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercloud.googlecloud import compute, ZONE, PROJECT
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
import googleapiclient

base_logger = core_utils.setup_logger("google_sweeper")


class GoogleSweeper(PluginBase):
    """
    Sweeper with kill/clean-up functions for Google Compute Engine
    """

    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.queue_config_mapper = QueueConfigMapper()

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

            queue_config = self.queue_config_mapper.get_queue(work_spec.computingSite)
            try:
                zone = queue_config.zone
            except AttributeError:
                zone = ZONE

            base_logger.debug("Going to kill VM {0}".format(vm_name))
            compute.instances().delete(project=PROJECT, zone=zone, instance=vm_name).execute()
            base_logger.debug("Killed VM {0}".format(vm_name))
            return True, ""
        except googleapiclient.errors.HttpError as e:
            if "was not found" in e.content:
                # the VM was already killed or does not exist for any other reason
                message = "VM does not exist".format(vm_name)
                base_logger.debug(message)
                return True, message
            else:
                # there was an issue killing the VM and it should be retried at another time
                return False, "Problems killing the VM: {0}".format(e)
        except Exception as e:
            return False, "Problems killing the VM: {0}".format(e)

    def sweep_worker(self, work_spec):
        """
        In the cloud, cleaning means destroying a VM

        :param work_spec: worker specification
        :type work_spec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        return self.kill_worker(work_spec)
