try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

from pandaharvester.harvestercore import core_utils

from .base_cred_manager import BaseCredManager

# logger
_logger = core_utils.setup_logger("grid_cred_manager")


# credential manager using grid-proxy
class GridCredManager(BaseCredManager):
    # constructor
    def __init__(self, **kwarg):
        BaseCredManager.__init__(self, **kwarg)

    # check proxy
    def check_credential(self):
        # make logger
        mainLog = self.make_logger(_logger, method_name="check_credential")
        comStr = f"grid-proxy-info -exists -hours 72 -file {self.outCertFile}"
        mainLog.debug(comStr)
        try:
            p = subprocess.Popen(comStr.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
        except Exception:
            core_utils.dump_error_message(mainLog)
            return False
        mainLog.debug(f"retCode={retCode} stdOut={stdOut} stdErr={stdErr}")
        return retCode == 0

    # renew proxy
    def renew_credential(self):
        # make logger
        mainLog = self.make_logger(_logger, method_name="renew_credential")
        comStr = f"grid-proxy-init -out {self.outCertFile} -valid 96:00 -cert {self.inCertFile}"
        mainLog.debug(comStr)
        try:
            p = subprocess.Popen(comStr.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            mainLog.debug(f"retCode={retCode} stdOut={stdOut} stdErr={stdErr}")
        except Exception:
            stdOut = ""
            stdErr = core_utils.dump_error_message(mainLog)
            retCode = -1
        return retCode == 0, f"{stdOut} {stdErr}"
