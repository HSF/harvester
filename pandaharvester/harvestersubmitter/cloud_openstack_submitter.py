import os
import tempfile

try:
    import subprocess32 as subprocess
except BaseException:
    import subprocess
import random
import uuid

from concurrent.futures import ThreadPoolExecutor

import re

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.cloud_openstack_utils import OS_SimpleClient


# setup base logger
baseLogger = core_utils.setup_logger("cloud_openstack_submitter")


def _init_script_replace(string, **kwarg):
    new_string = string
    macro_map = {
        "\$\(workerID\)": str(kwarg["workerID"]),
        "\$\(batchID\)": str(kwarg["batchID"]),
        "\$\(accessPoint\)": str(kwarg["accessPoint"]),
    }
    for k, v in macro_map.items():
        new_string = re.sub(k, v, new_string)
    return new_string


# make cloud initialization script
def _make_init_script(workspec, template_str):
    # make logger
    tmpLog = core_utils.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="_make_init_script")

    # make init tempfile
    tmpFile = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix="_init.sh", dir=workspec.get_access_point())
    new_template_str = _init_script_replace(template_str, **workspec.__dict__)
    tmpFile.write(new_template_str)
    tmpFile.close()
    tmpLog.debug("done")
    return tmpFile.name


# Cloud Openstack submitter
class CloudOpenstackSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.nProcesses = 4
        self.vm_client = OS_SimpleClient(auth_config_json_file=self.authConfigFile)

    def _submit_a_vm(self, workspec):
        # set logger
        tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="_submit_a_vm")

        # initial return values
        tmpRetVal = (None, "Nothing done")

        # decide id
        vm_name = "harvester-vm_{0}".format(str(uuid.uuid4()))

        # # decide image
        vm_image_id = self.vmImageID

        # decide flavor
        # FIXME
        if workspec.nCore == 1:
            vm_flavor_id = self.jobType_vmFlavor_map["SCORE"]
        elif workspec.nCore == 8:
            vm_flavor_id = self.jobType_vmFlavor_map["MCORE"]
        else:
            vm_flavor_id = self.jobType_vmFlavor_map["other"]

        # decide userdata
        with open(self.initScriptTemplate) as _f:
            template_str = _f.read()
        vm_userdata_file = _make_init_script(workspec, template_str)
        vm_userdata = open(vm_userdata_file, "r")

        # get image and flavor
        try:
            vm_image = self.vm_client.nova.glance.find_image(vm_image_id)
            vm_flavor = self.vm_client.nova.flavors.get(vm_flavor_id)
        except Exception as _e:
            errStr = "Failed to create a VM with name={0} ; {1}".format(vm_name, _e)
            tmpLog.error(errStr)
            tmpRetVal = (None, errStr)
            return tmpRetVal

        # create a VM
        try:
            self.vm_client.nova.servers.create(name=vm_name, image=vm_image, flavor=vm_flavor, userdata=vm_userdata, **self.vmCreateAttributes)
        except Exception as _e:
            errStr = "Failed to create a VM with name={0} ; {1}".format(vm_name, _e)
            tmpLog.error(errStr)
            tmpRetVal = (None, errStr)
        else:
            try:
                vm_server = self.vm_client.nova.servers.list(search_opts={"name": vm_name}, limit=1)[0]
                vm_id = vm_server.id
            except Exception as _e:
                errStr = "Failed to create a VM with name={0} ; {1}".format(vm_name, _e)
                tmpLog.error(errStr)
                tmpRetVal = (None, errStr)
            else:
                workspec.batchID = vm_id
                tmpLog.info("Created a VM with name={vm_name} id={vm_id}".format(vm_name=vm_name, vm_id=vm_id))
                tmpRetVal = (True, "")

        vm_userdata.close()

        # return
        return tmpRetVal

    # submit workers

    def submit_workers(self, workspec_list):
        # set logger
        tmpLog = self.make_logger(baseLogger, method_name="submit_workers")

        nWorkers = len(workspec_list)
        tmpLog.debug("start nWorkers={0}".format(nWorkers))

        # exec with multi-thread
        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            retValList = thread_pool.map(self._submit_a_vm, workspec_list)
        tmpLog.debug("{0} workers submitted".format(nWorkers))

        # return
        retList = list(retValList)

        tmpLog.debug("done")

        return retList
