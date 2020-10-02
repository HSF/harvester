try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils

# logger
_logger = core_utils.setup_logger('no_voms_cred_manager')


# credential manager with no-voms proxy
class NoVomsCredManager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check proxy lifetime for monitoring/alerting purposes
    def check_credential_lifetime(self):
        main_log = self.make_logger(_logger, method_name='check_credential_lifetime')
        lifetime = None
        try:
            command_str = "voms-proxy-info -timeleft -file {0}".format(self.outCertFile)
            p = subprocess.Popen(command_str.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            return_code = p.returncode
            main_log.debug('retCode={0} stdout={1} stderr={2}'.format(return_code, stdout, stderr))
            if return_code == 0:  # OK
                lifetime = int(stdout) / 3600
        except Exception:
            core_utils.dump_error_message(main_log)

        main_log.debug('returning lifetime {0}'.format(lifetime))
        return lifetime

    # check proxy
    def check_credential(self):
        # make logger
        mainLog = self.make_logger(_logger, method_name='check_credential')
        comStr = "voms-proxy-info -exists -hours 72 -file {0}".format(self.outCertFile)
        mainLog.debug(comStr)
        try:
            p = subprocess.Popen(comStr.split(),
                                 shell=False,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
        except Exception:
            core_utils.dump_error_message(mainLog)
            return False
        mainLog.debug('retCode={0} stdOut={1} stdErr={2}'.format(retCode, stdOut, stdErr))
        return retCode == 0

    # renew proxy
    def renew_credential(self):
        # make logger
        mainLog = self.make_logger(_logger, method_name='renew_credential')
        comStr = "voms-proxy-init -rfc -noregen -voms {0} -out {1} -valid 96:00 -cert={2} -key={2}".format(self.voms,
                                                                                                           self.outCertFile,
                                                                                                           self.inCertFile)
        mainLog.debug(comStr)
        try:
            p = subprocess.Popen(comStr.split(),
                                 shell=False,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            mainLog.debug('retCode={0} stdOut={1} stdErr={2}'.format(retCode, stdOut, stdErr))
        except Exception:
            stdOut = ''
            stdErr = core_utils.dump_error_message(mainLog)
            retCode = -1
        return retCode == 0, "{0} {1}".format(stdOut, stdErr)
