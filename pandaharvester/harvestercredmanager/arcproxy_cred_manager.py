import re
import subprocess

from .base_cred_manager import BaseCredManager
from pandaharvester.harvestercore import core_utils

# logger
_logger = core_utils.setup_logger("arcproxy_cred_manager")


# credential manager with no-voms proxy using arcproxy
class ArcproxyCredManager(BaseCredManager):
    # constructor
    def __init__(self, **kwarg):
        BaseCredManager.__init__(self, **kwarg)

    # check proxy
    def check_credential(self):
        # make logger
        mainLog = self.make_logger(_logger, method_name="check_credential")
        # output is lifetime left of voms extension in seconds
        comStr = "arcproxy -i vomsACvalidityLeft -P {0}".format(self.outCertFile)
        mainLog.debug(comStr)
        try:
            p = subprocess.run(comStr.split(), encoding="utf-8", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdOut = p.stdout.strip()
            stdErr = p.stderr
            retCode = p.returncode
        except Exception:
            core_utils.dump_error_message(mainLog)
            return False
        mainLog.debug("retCode={0} stdOut={1} stdErr={2}".format(retCode, stdOut, stdErr))
        if retCode != 0 or not re.match(r"\d+", stdOut):
            mainLog.error("Unexpected output from arcproxy: {0}".format(stdOut))
            return False
        # return whether lifetime is greater than three days
        return int(stdOut) > 3600 * 72

    # renew proxy
    def renew_credential(self):
        # make logger
        mainLog = self.make_logger(_logger, method_name="renew_credential")
        comStr = "arcproxy -S {0} -P {1} -c validityPeriod=96h -c vomsACvalidityPeriod=96h -C {2} -K {2}".format(self.voms, self.outCertFile, self.inCertFile)
        mainLog.debug(comStr)
        try:
            p = subprocess.run(comStr.split(), encoding="utf-8", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdOut = p.stdout
            stdErr = p.stderr
            retCode = p.returncode
            mainLog.debug("retCode={0} stdOut={1} stdErr={2}".format(retCode, stdOut, stdErr))
        except Exception:
            stdOut = ""
            stdErr = core_utils.dump_error_message(mainLog)
            retCode = -1
        return retCode == 0, "{0} {1}".format(stdOut, stdErr)
