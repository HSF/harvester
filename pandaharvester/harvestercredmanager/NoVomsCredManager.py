import subprocess

from pandaharvester.harvestercore.PluginBase import PluginBase
from pandaharvester.harvestercore import CoreUtils

# logger
_logger = CoreUtils.setupLogger()


# credential manager with no-voms proxy
class NoVomsCredManager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check proxy
    def checkCredential(self, proxyFile):
        # make logger
        self.mainLog = CoreUtils.makeLogger(_logger)
        comStr = "voms-proxy-info -exists -hours 72 -file {0}".format(proxyFile)
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
    def renewCredential(self, proxyFile):
        comStr = "voms-proxy-init -voms {0} -out {1} -valid 96:00 -cert={2}".format(self.config.voms,
                                                                                    proxyFile,
                                                                                    self.config.certFile)
        p = subprocess.Popen(comStr.split(),
                             shell=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdOut, stdErr = p.communicate()
        retCode = p.returncode
        self.mainLog.debug('retCode={0} stdOut={1} stdErr={2}'.format(retCode, stdOut, stdErr))
        return retCode == 0, stdOut + ' ' + stdErr
