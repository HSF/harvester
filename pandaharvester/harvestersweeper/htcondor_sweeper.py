
#=== Imports ==================================================

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils

import os
import shutil
import subprocess

#==============================================================

#=== Definitions ==============================================

## Logger
baseLogger = core_utils.setup_logger('htcondor_sweeper')

#==============================================================

#=== Functions ================================================

def _runShell(cmd):
    cmd = str(cmd)
    p = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdOut, stdErr = p.communicate()
    retCode = p.returncode
    return (retCode, stdOut, stdErr)

#==============================================================

#=== Classes ==================================================

# dummy plugin for sweeper
class HTCondorSweeper(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # kill a worker
    def kill_worker(self, workspec):
        """Kill a worker in a scheduling system like batch systems and computing elements.

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """

        ## Make logger
        tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
                                        method_name='kill_worker')

        ## Kill command
        comStr = 'condor_rm {0}'.format(workspec.batchID)
        (retCode, stdOut, stdErr) = _runShell(comStr)
        if retCode != 0:
            comStr = 'condor_q -l {0}'.format(workspec.batchID)
            (retCode, stdOut, stdErr) = _runShell(comStr)
            if str(workspec.batchID) in str(stdOut) or retCode != 0:
                ## Command failed to kill
                errStr = 'command "{0}" failed, retCode={1}, error: {2} {3}'.format(comStr, retCode, stdOut, stdErr)
                tmpLog.error(errStr)
                return False, errStr
            else:
                ## Found already killed
                tmpLog.warning('Found workerID={0} batchID={1} already killed'.format(workspec.workerID, workspec.batchID))
        else:
            tmpLog.info('Succeeded to kill workerID={0} batchID={1}'.format(workspec.workerID, workspec.batchID))

        ## Return
        return True, ''

    # cleanup for a worker
    def sweep_worker(self, workspec):
        """Perform cleanup procedures for a worker, such as deletion of work directory.

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """

        ## Make logger
        tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
                                        method_name='sweep_worker')

        ## Clean up worker directory
        try:
            shutil.rmtree(workspec.accessPoint)
        except OSError, _err:
            if 'No such file or directory' in _err.strerror:
                tmpLog.debug('Found that {0} was already removed'.format(_err.filename))
            pass
        tmpLog.info('Succeeded to clean up worker directory: Removed {0}'.format(workspec.workerID, workspec.accessPoint))

        ## Clean up preparator base directory (staged-in files)
        try:
            preparatorBasePath = self.preparatorBasePath
        except AttributeError:
            tmpLog.debug('No preparator base directory is configured. Skipped cleaning up preparator directory')
            pass
        else:
            if os.path.isdir(preparatorBasePath):
                if not workspec.get_jobspec_list():
                    tmpLog.warning('No job PandaID found relate to workerID={0}. Skipped cleaning up preparator directory'.format(workspec.workerID))
                else:
                    for jobspec in workspec.get_jobspec_list():
                        preparator_dir_for_cleanup = os.path.join(preparatorBasePath, str(jobspec.PandaID))
                        if os.path.isdir(preparator_dir_for_cleanup) and preparator_dir_for_cleanup != preparatorBasePath:
                            try:
                                shutil.rmtree(preparator_dir_for_cleanup)
                            except OSError, _err:
                                if 'No such file or directory' in _err.strerror:
                                    tmpLog.debug('Found that {0} was already removed'.format(_err.filename))
                                pass
                            tmpLog.info('Succeeded to clean up preparator directory: Removed {0}'.format(preparator_dir_for_cleanup))
                        else:
                            errStr = 'Failed to clean up preparator directory: {0} does not exist or invalid to be cleaned up'.format(preparator_dir_for_cleanup)
                            tmpLog.error(errStr)
                            return False, errStr
            else:
                errStr = 'Configuration error: Preparator base directory {0} does not exist'.format(preparatorBasePath)
                tmpLog.error(errStr)
                return False, errStr

        tmpLog.info('Succeeded to clean up everything about workerID={0}'.format(workspec.workerID))

        ## Return
        return True, ''

#==============================================================
