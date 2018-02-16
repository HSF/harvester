"""
Based on: https://cloud.google.com/compute/docs/tutorials/python-guide#before-you-begin
"""

import uuid
import os
import subprocess

from concurrent.futures import ProcessPoolExecutor as Pool

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import work_spec
import googleapiclient.discovery

# setup base logger
base_logger = core_utils.setup_logger('google_submitter')

# compute
compute = googleapiclient.discovery.build('compute', 'v1')


def wait_for_operation(compute, project, zone, operation_name):
    """
    Waits for an operation to complete.
    TODO: decide whether we want to block or just move on and list the instance status later
    :param compute:
    :param project:
    :param zone:
    :param operation_name:
    :return:
    """
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(project=project, zone=zone, operation=operation_name).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)


def create_vm(work_spec):
    """
    Boots up a VM in GCE based on the worker specifications

    :param work_spec: worker specifications
    :return:
    """

    tmp_log = core_utils.make_logger(base_logger, 'workerID={0}'.format(work_spec.workerID),
                                     method_name='submit_a_worker')
    work_spec.reset_changed_list()

    tmp_log.debug('nCore={0} minRamCount={1} maxDiskCount={2} maxWalltime={0}'.format(work_spec.nCore,
                                                                                      work_spec.minRamCount,
                                                                                      work_spec.maxDiskCount,
                                                                                      work_spec.maxWalltime))

    operation = compute.instances().insert(project=project, zone=zone, body=config).execute()
    wait_for_operation(compute, project, zone, operation['name'])

    work_spec.batchID = operation['targetId']

    return (True, stdout_str + stderr_str), work_spec.get_changed_attributes()


class GoogleSubmitter(PluginBase):
    """
    Plug-in for Google Cloud Engine VM submission. In this case the worker will abstract a VM running a job
    """

    def __init__(self, **kwarg):
        self.google_library = GoogleLibrary()
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
        pool_size = min(work_spec_list, 10) # TODO: think about the optimal pool size
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




class GoogleVM():

    def __init__(self, work_spec):
        self.work_spec = work_spec
        self.name = 'VM_NAME'
        self.image = self.resolve_image_url()
        self.instance_type = self.resolve_instance_type()
        self.config = self.prepare_metadata()

    def resolve_image_url(self):
        """
        TODO: implement
        :param work_spec: worker specifications
        :return: URL pointing to the machine type to use
        """
        # Get the latest Debian Jessie image
        image_response = compute.images().getFromFamily(project='debian-cloud', family='debian-8').execute()
        source_disk_image = image_response['selfLink']

        return source_disk_image

    def resolve_instance_type(self):
        """
        Resolves the ideal instance type for the work specifications. An overview on VM types can be found here: https://cloud.google.com/compute/docs/machine-types

        TODO: for the moment we will just assume we need the standard type, but in the future this function can be expanded
        TODO: to consider also custom VMs, hi/lo mem, many-to-one mode, etc.

        :param work_spec: worker specifications
        :return: instance type name
        """

        # Choose the region
        region = 'region'  # TODO: fill properly

        # Calculate the number of VCPUs
        cores = 8 # default value. TODO: probably should except if we don't find a suitable number
        standard_cores = [1, 2, 4, 8, 16, 32, 64, 96]
        for standard_core in standard_cores:
            if self.work_spec.nCore < standard_core:
                cores = standard_core
                break

        instance_type = 'zones/{0}/machineTypes/n1-standard-{1}'.format(region, cores)

        return instance_type

    def configure_disk(self):
        """
        TODO: configure the ephemeral disk used by the VM
        :return: dictionary with disk configuration
        """

        return {}

    def prepare_metadata(self):
        """
        TODO: prepare any user data and metadata that we want to pass to the VM instance
        :return:
        """

        config = {'name': self.name,
                  'machineType': self.instance_type,

                  # Specify the boot disk and the image to use as a source.
                  'disks':
                      [
                          {
                              'boot': True,
                              'autoDelete': True,
                              'initializeParams': {'sourceImage': self.image}
                           }
                      ],

                  # Specify a network interface with NAT to access the public internet
                  'networkInterfaces':
                      [
                          {
                              'network': 'global/networks/default',
                              'accessConfigs':
                                  [
                                      {
                                          'type': 'ONE_TO_ONE_NAT',
                                          'name': 'External NAT'}
                                  ]
                          }
                      ],

                  # Allow the instance to access cloud storage and logging.
                  'serviceAccounts':
                      [
                          {
                              'email': 'default',
                              'scopes':
                                  [
                                      'https://www.googleapis.com/auth/devstorage.read_write',
                                      'https://www.googleapis.com/auth/logging.write'
                                  ]
                          }
                      ],

                  # Metadata is readable from the instance and allows you to
                  # pass configuration from deployment scripts to instances.
                  'metadata':
                      {
                          'items':
                              [
                                  {
                                      # Startup script is automatically executed by the instance upon startup
                                      'key': 'startup-script',
                                      'value': startup_script
                                  },
                                  {
                                      'key': 'bucket',
                                      'value': bucket
                                  }
                              ]
                      }
        }

        return config


