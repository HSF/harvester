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

    # check proxy
    def check_credential(self):
        # make logger
        mainLog = core_utils.make_logger(_logger, method_name='check_credential')
        comStr = "voms-proxy-info -exists -hours 72 -file {0}".format(self.outCertFile)
        mainLog.debug(comStr)
        try:
            p = subprocess.Popen(comStr.split(),
                                 shell=False,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
        except:
            core_utils.dump_error_message(mainLog)
            return False
        mainLog.debug('retCode={0} stdOut={1} stdErr={2}'.format(retCode, stdOut, stdErr))
        return retCode == 0

    # renew proxy
    def renew_credential(self):
        # make logger
        mainLog = core_utils.make_logger(_logger, method_name='renew_credential')
        comStr = "voms-proxy-init -rfc -voms {0} -out {1} -valid 96:00 -cert={2}".format(self.voms,
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
        except:
            stdOut = ''
            stdErr = core_utils.dump_error_message(mainLog)
            retCode = -1
        return retCode == 0, "{0} {1}".format(stdOut, stdErr)
