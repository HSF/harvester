import os
import tempfile
import subprocess
import random
import uuid

from concurrent.futures import ThreadPoolExecutor

import re

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.cloud-openstack_utils import OS_SimpleClient


# setup base logger
baseLogger = core_utils.setup_logger('cloud-openstack_submitter')


def _init_script_replace(string, **kwarg):
    new_string = string
    macro_map = {
                '\$\(workerID\)': str(kwarg['workerID']),
                '\$\(batchID\)': str(kwarg['batchID']),
                '\$\(accessPoint\)': str(kwarg['accessPoint']),
                }
    for k, v in macro_map.items():
        new_string = re.sub(k, v, new_string)
    return new_string


# make cloud initialization script
def _make_init_script(workspec, template_str):
    # make logger
    tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID), method_name='_make_init_script')

    # make init tempfile
    tmpFile = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_init.sh', dir=workspec.get_access_point())
    new_template_str = _init_script_replace(template_str, workspec.__dict__)
    tmpFile.write(new_template_str)
    tmpFile.close()
    tmpLog.debug('done')
    return tmpFile.name


# Cloud Openstack submitter
class CloudOpenstackSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.nProcesses = 4
        self.vm_client = OS_SimpleClient(auth_config_json_file=self.AuthConfigFile)

        # set vm maps
        #TODO: be configurable
        self.VM_FLAVOR_MAP = {
                            'SCORE': 'm1.small',
                            'MCORE': 'm1.xlarge',
                            'other': 'm1.medium',
                        }

        self.VM_IAMGE_MAP = {
                            'el6': 'uCernVM3-2.8-6',
                            'el7': 'uCernVM4-2.8-6',
                        }

        # set other vm attributes
        #TODO: be configurable
        self.VM_CREATE_ATTRIBUTES = {
                        'key_name': 'opskey',
                        'nics': {'net-id': '536855fc-8cb2-423a-84d7-d0dc1ce2dff7',},
                    }


    def _submit_a_vm(self, workspec):
        # set logger
        tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID), method_name='submit_workers')

        # initial return values
        tmpRetVal = (None, 'Nothing done')

        # decide id
        vm_id = uuid.uuid4()

        # decide image
        if True: #FIXME
            vm_image = self.VM_IAMGE_MAP['el6']

        # decide flavor
        #FIXME
        if workspec.nCore == 1:
            vm_flavor = self.VM_FLAVOR_MAP['SCORE']
        elif workspec.nCore == 8:
            vm_flavor = self.VM_FLAVOR_MAP['SCORE']
        else:
            vm_flavor = self.VM_FLAVOR_MAP['other']

        # decide userdata
        with open(self.InitScriptTemplate) as _f:
            template_str = _f.read()
        vm_userdata = _make_init_script(workspec, template_str):

        # create a VM
        try:
            self.vm_client.nova.create( name=vm_name_id,
                                        image=vm_image,
                                        flavor=vm_flavor,
                                        reservation_id=vm_id,
                                        userdata=vm_userdata,
                                        **self.VM_CREATE_ATTRIBUTES)
        except Exception as _e:
            errStr = 'Failed to create a VM with id={0} ; {1}'.format(vm_id, _e)
            tmpLog.error(errStr)
            tmpRetVal = (False, errStr)
        else:
            try:
                vm_server = self.vm_client.nova.get(vm_id)
                vm_server.id
            except Exception as _e:
                errStr = 'Failed to create a VM with id={0} ; {1}'.format(vm_id, _e)
                tmpLog.error(errStr)
                tmpRetVal = (False, errStr)
            else:
                if vm_server.id == vm_id:
                    workspec.batchID = vm_id
                    tmpLog.info('Created a VM with id={0}'.format(vm_id))
                    tmpRetVal = (True, '')
                else:
                    errStr = 'Created VM id={0} different from specified VM id={1}'.format(vm_server.id, vm_id)
                    tmpLog.error(errStr)
                    tmpRetVal = (False, errStr)

        # return
        return tmpRetVal


    # submit workers
    def submit_workers(self, workspec_list):
        # set logger
        tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID), method_name='submit_workers')
        tmpLog.debug('start nWorkers={0}'.format(len(workspec_list)))

        # exec with multi-thread
        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            retValList = thread_pool.map(self._submit_a_vm, workspec_list)
        tmpLog.debug('{0} workers submitted'.format(nWorkers))

        # return
        retList = list(retValList)

        tmpLog.debug('done')

        return retList
