"""
Based on: https://cloud.google.com/compute/docs/tutorials/python-guide#before-you-begin
"""

import time

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

# from requests.exceptions import SSLError
from pandaharvester.harvestercloud.googlecloud import compute, GoogleVM, ZONE, PROJECT
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper

# setup base logger
base_logger = core_utils.setup_logger("google_submitter")


def wait_for_operation(project, zone, operation_name):
    """
    Waits for an operation to complete.
    TODO: decide whether we want to block or just move on and list the instance status later
    :param project:
    :param zone:
    :param operation_name:
    :return:
    """
    tmp_log = core_utils.make_logger(base_logger, method_name="wait_for_operation")
    tmp_log.debug("Waiting for operation to finish...")

    while True:
        result = compute.zoneOperations().get(project=project, zone=zone, operation=operation_name).execute()

        if result["status"] == "DONE":
            if "error" in result:
                raise Exception(result["error"])
            tmp_log.debug("Operation finished...")
            return result

        time.sleep(1)


def create_vm(work_spec, queue_config):
    """
    Boots up a VM in GCE based on the worker specifications

    :param work_spec: worker specifications
    :return:
    """
    work_spec.reset_changed_list()

    tmp_log = core_utils.make_logger(base_logger, "workerID={0}".format(work_spec.workerID), method_name="submit_a_worker")

    tmp_log.debug(
        "nCore={0} minRamCount={1} maxDiskCount={2} maxWalltime={0}".format(
            work_spec.nCore, work_spec.minRamCount, work_spec.maxDiskCount, work_spec.maxWalltime
        )
    )

    try:
        vm = GoogleVM(work_spec, queue_config)

        try:
            zone = queue_config.zone
        except AttributeError:
            zone = ZONE

    except Exception as e:
        tmp_log.debug("VM preparation failed with: {0}".format(e))
        # there was some problem preparing the VM, usually related to interaction with GCE
        # since the VM was not submitted yet, we mark the worker as "missed"
        return (False, str(e)), work_spec.get_changed_attributes()

    try:
        tmp_log.debug("Going to submit VM {0}".format(vm.name))
        work_spec.batchID = vm.name
        operation = compute.instances().insert(project=PROJECT, zone=zone, body=vm.config).execute()
        # tmp_log.debug('Submitting VM {0}'.format(vm.name))
        # wait_for_operation(PROJECT, ZONE, operation['name'])
        tmp_log.debug("Submitted VM {0}".format(vm.name))

        return (True, "OK"), work_spec.get_changed_attributes()
    except Exception as e:
        tmp_log.debug("GCE API exception: {0}".format(e))
        # Despite the exception we will consider the submission successful to set the worker as "submitted".
        # This is related to the GCE API reliability. We have observed that despite failures (time outs, SSL errors, etc)
        # in many cases the VMs still start and we don't want VMs that are not inventorized. If the VM submission failed
        # the harvester monitor will see when listing the running VMs
        return (True, str(e)), work_spec.get_changed_attributes()


class GoogleSubmitter(PluginBase):
    """
    Plug-in for Google Cloud Engine VM submission. In this case the worker will abstract a VM running a job
    """

    def __init__(self, **kwarg):
        self.logBaseURL = "http://localhost/test"
        PluginBase.__init__(self, **kwarg)

        self.queue_config_mapper = QueueConfigMapper()

    def submit_workers(self, work_spec_list):
        """
        :param work_spec_list: list of workers to submit
        :return:
        """

        tmp_log = self.make_logger(base_logger, method_name="submit_workers")
        tmp_log.debug("start nWorkers={0}".format(len(work_spec_list)))

        ret_list = []
        if not work_spec_list:
            tmp_log.debug("empty work_spec_list")
            return ret_list

        # we assume all work_specs in the list belong to the same queue
        queue_config = self.queue_config_mapper.get_queue(work_spec_list[0].computingSite)

        # Create VMs in parallel
        # authentication issues when running the Cloud API in multiprocess
        # pool_size = min(len(work_spec_list), 10)
        # with Pool(pool_size) as pool:
        #    ret_val_list = pool.map(create_vm, work_spec_list, lock)

        ret_val_list = []
        for work_spec in work_spec_list:
            ret_val_list.append(create_vm(work_spec, queue_config))

        # Propagate changed attributes
        for work_spec, tmp_val in zip(work_spec_list, ret_val_list):
            ret_val, tmp_dict = tmp_val

            work_spec.set_attributes_with_dict(tmp_dict)
            work_spec.set_log_file("batch_log", "{0}/{1}.log".format(self.logBaseURL, work_spec.batchID))
            work_spec.set_log_file("stdout", "{0}/{1}.out".format(self.logBaseURL, work_spec.batchID))
            work_spec.set_log_file("stderr", "{0}/{1}.err".format(self.logBaseURL, work_spec.batchID))
            ret_list.append(ret_val)

        tmp_log.debug("done")

        return ret_list
