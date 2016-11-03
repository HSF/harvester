import subprocess

from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.PluginBase import PluginBase

# logger
_logger = CoreUtils.setupLogger()


# credential manager using grid-proxy
class GridCredManager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check proxy
    def checkCredential(self, proxy_file):
        # make logger
        self.mainLog = CoreUtils.makeLogger(_logger)
        comStr = "grid-proxy-info -exists -hours 72 -file {0}".format(proxy_file)
        self.mainLog.debug(comStr)
        p = subprocess.Popen(comStr.split(),
                             shell=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdOut, stdErr = p.communicate()
        retCode = p.returncode
        self.mainLog.debug('retCode={0} stdOut={1} stdErr={2}'.format(retCode, stdOut, stdErr))
        return retCode == 0

    # renew proxy
    def renewCredential(self, proxy_file):
        comStr = "grid-proxy-init -out {0} -valid 96:00 -cert {1}".format(proxy_file,
                                                                          self.config.certFile)
        p = subprocess.Popen(comStr.split(),
                             shell=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdOut, stdErr = p.communicate()
        retCode = p.returncode
        self.mainLog.debug('retCode={0} stdOut={1} stdErr={2}'.format(retCode, stdOut, stdErr))
        return retCode == 0, stdOut + ' ' + stdErr
