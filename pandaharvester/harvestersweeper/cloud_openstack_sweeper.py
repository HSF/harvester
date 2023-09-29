import os

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.cloud_openstack_utils import OS_SimpleClient


# setup base logger
baseLogger = core_utils.setup_logger("cloud_openstack_sweeper")


# Cloud Openstack submitter
class CloudOpenstackSweeper(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.vm_client = OS_SimpleClient(auth_config_json_file=self.authConfigFile)

    # kill a worker

    def kill_worker(self, workspec):
        # set logger
        tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="kill_worker")

        # initial return values
        tmpRetVal = (None, "Nothing done")

        # kill vm
        vm_id = workspec.batchID
        try:
            self.vm_client.nova.servers.delete(vm_id)
        except Exception as _e:
            errStr = "Failed to delete a VM with id={0} ; {1}".format(vm_id, _e)
            tmpLog.error(errStr)
            tmpRetVal = (False, errStr)
        else:
            tmpLog.info("Deleted a VM with id={0}".format(vm_id))
            tmpRetVal = (True, "")

        return tmpRetVal

    # cleanup for a worker

    def sweep_worker(self, workspec):
        # set logger
        tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="sweep_worker")

        return True, ""
