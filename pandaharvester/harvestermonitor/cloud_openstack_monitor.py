import os

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestermisc.cloud_openstack_utils import OS_SimpleClient


# setup base logger
baseLogger = core_utils.setup_logger("cloud_openstack_monitor")


# status map
# FIXME
vm_worker_status_map_dict = {
    "ACTIVE": WorkSpec.ST_running,
    "BUILD": WorkSpec.ST_submitted,
    "DELETED": WorkSpec.ST_finished,
    "ERROR": WorkSpec.ST_failed,
    "HARD_REBOOT": WorkSpec.ST_pending,
    "MIGRATING": WorkSpec.ST_pending,
    "PASSWORD": WorkSpec.ST_pending,
    "PAUSED": WorkSpec.ST_pending,
    "REBOOT": WorkSpec.ST_pending,
    "REBUILD": WorkSpec.ST_pending,
    "RESCUE": WorkSpec.ST_pending,
    "RESIZE": WorkSpec.ST_pending,
    "REVERT_RESIZE": WorkSpec.ST_pending,
    "SHELVED": WorkSpec.ST_pending,
    "SHELVED_OFFLOADED": WorkSpec.ST_pending,
    "SHUTOFF": WorkSpec.ST_cancelled,
    "SOFT_DELETED": WorkSpec.ST_pending,
    "SUSPENDED": WorkSpec.ST_pending,
    "UNKNOWN": WorkSpec.ST_failed,
    "VERIFY_RESIZE": WorkSpec.ST_pending,
}


# whether to kill the vm
def _toKillVM(*some_info):
    retVal = False
    # FIXME
    # information should come from harvester messenger or else
    return retVal


# Cloud Openstack monitor
class CloudOpenstackMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.nProcesses = 4
        self.vm_client = OS_SimpleClient(auth_config_json_file=self.authConfigFile)

    # kill a vm

    def _kill_a_vm(self, vm_id):
        # set logger
        tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="_kill_a_vm")
        try:
            self.vm_client.nova.delete(vm_id)
        except Exception as _e:
            errStr = "Failed to delete a VM with id={0} ; {1}".format(vm_id, _e)
            tmpLog.error(errStr)
            tmpRetVal = (False, errStr)
        else:
            tmpLog.info("Deleted a VM with id={0}".format(vm_id))
            tmpRetVal = (True, "")
        return tmpRetVal

    # check a vm

    def _check_a_vm(self, workspec):
        # set logger
        tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="_check_a_vm")

        # initialization
        vm_id = workspec.batchID
        newStatus = workspec.status
        errStr = ""

        try:
            vm_server = self.vm_client.nova.servers.get(vm_id)
            vm_status = vm_server.status
        except Exception as _e:
            errStr = "Failed to get VM status of id={0} ; {1}".format(vm_id, _e)
            tmpLog.error(errStr)
            tmpLog.info("Force to cancel the worker due to failure to get VM status")
            newStatus = WorkSpec.ST_cancelled
        else:
            newStatus = vm_worker_status_map_dict.get(vm_status)
            tmpLog.info("batchID={0}: vm_status {1} -> worker_status {2}".format(workspec.batchID, vm_status, newStatus))

            if _toKillVM():  # FIXME
                self._kill_a_vm(vm_id)

        return (newStatus, errStr)

    # check workers

    def check_workers(self, workspec_list):
        # Check for all workers
        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            retIterator = thread_pool.map(self._check_a_vm, workspec_list)

        retList = list(retIterator)

        return True, retList
