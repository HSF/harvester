try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool

from .base_cred_manager import BaseCredManager

# logger
_logger = core_utils.setup_logger("proxy_cache_cred_manager")


# credential manager with proxy cache
class ProxyCacheCredManager(BaseCredManager):
    # constructor
    def __init__(self, **kwarg):
        BaseCredManager.__init__(self, **kwarg)

    # check proxy
    def check_credential(self):
        # make logger
        mainLog = self.make_logger(_logger, method_name="check_credential")
        comStr = f"voms-proxy-info -exists -hours 72 -file {self.outCertFile}"
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
        # make communication channel to PanDA
        com = CommunicatorPool()
        proxy, msg = com.get_proxy(self.voms, (self.inCertFile, self.inCertFile))
        if proxy is not None:
            pFile = open(self.outCertFile, "w")
            pFile.write(proxy)
            pFile.close()
        else:
            mainLog.error(f"failed to renew credential with a server message : {msg}")
        return proxy is not None, msg
