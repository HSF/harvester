# === Imports ==================================================

import os

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

try:
    import subprocess32 as subprocess
except BaseException:
    import subprocess

import shutil

# ==============================================================

# === Definitions ==============================================

# Logger
baseLogger = core_utils.setup_logger("cobalt_sweeper")

# ==============================================================

# === Functions ================================================


def _runShell(cmd):
    cmd = str(cmd)
    p = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdOut, stdErr = p.communicate()
    retCode = p.returncode
    return (retCode, stdOut, stdErr)


# ==============================================================

# === Classes ==================================================

# dummy plugin for sweeper


class CobaltSweeper(PluginBase):
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

        # Make logger
        tmpLog = self.make_logger(baseLogger, f"workerID={workspec.workerID}", method_name="kill_worker")

        # Kill command
        comStr = f"qdel {workspec.batchID}"
        (retCode, stdOut, stdErr) = _runShell(comStr)
        if retCode != 0:
            # Command failed
            errStr = f'command "{comStr}" failed, retCode={retCode}, error: {stdOut} {stdErr}'
            tmpLog.error(errStr)
            return False, errStr
        else:
            tmpLog.info(f"Succeeded to kill workerID={workspec.workerID} batchID={workspec.workerID}")

        # Return
        return True, ""

    # cleanup for a worker
    def sweep_worker(self, workspec):
        """Perform cleanup procedures for a worker, such as deletion of work directory.

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """

        # Make logger
        tmpLog = self.make_logger(baseLogger, f"workerID={workspec.workerID}", method_name="sweep_worker")

        # Clean up worker directory
        if os.path.exists(workspec.accessPoint):
            shutil.rmtree(workspec.accessPoint)
            tmpLog.info(" removed {1}".format(workspec.workerID, workspec.accessPoint))
        else:
            tmpLog.info("access point already removed.")
        # Return
        return True, ""


# ==============================================================
