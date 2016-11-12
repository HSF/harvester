import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
_logger = core_utils.setup_logger()


# credential manager using grid-proxy
class GridCredManager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check proxy
    def check_credential(self, proxy_file):
        # make logger
        mainLog = core_utils.make_logger(_logger)
        comStr = "grid-proxy-info -exists -hours 72 -file {0}".format(proxy_file)
        mainLog.debug(comStr)
        p = subprocess.Popen(comStr.split(),
                             shell=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdOut, stdErr = p.communicate()
        retCode = p.returncode
        mainLog.debug('retCode={0} stdOut={1} stdErr={2}'.format(retCode, stdOut, stdErr))
        return retCode == 0

    # renew proxy
    def renew_credential(self, proxy_file):
        # make logger
        mainLog = core_utils.make_logger(_logger)
        comStr = "grid-proxy-init -out {0} -valid 96:00 -cert {1}".format(proxy_file,
                                                                          self.config.certFile)
        p = subprocess.Popen(comStr.split(),
                             shell=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdOut, stdErr = p.communicate()
        retCode = p.returncode
        mainLog.debug('retCode={0} stdOut={1} stdErr={2}'.format(retCode, stdOut, stdErr))
        return retCode == 0, stdOut + ' ' + stdErr
