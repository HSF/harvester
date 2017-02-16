import subprocess

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool

# logger
_logger = core_utils.setup_logger()


# credential manager with proxy cache
class ProxyCacheCredManager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check proxy
    def check_credential(self, proxy_file):
        # make logger
        mainLog = core_utils.make_logger(_logger)
        comStr = "voms-proxy-info -exists -hours 72 -file {0}".format(proxy_file)
        mainLog.debug(comStr)
        try:
            p = subprocess.Popen(comStr.split(),
                                 shell=False,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
        except:
            stdOut = ''
            stdErr = core_utils.dump_error_message(mainLog)
            retCode = 1
        mainLog.debug('retCode={0} stdOut={1} stdErr={2}'.format(retCode, stdOut, stdErr))
        return retCode == 0

    # renew proxy
    def renew_credential(self, proxy_file):
        # make logger
        mainLog = core_utils.make_logger(_logger)
        # make communication channel to PanDA
        com = CommunicatorPool()
        proxy, msg = com.get_proxy(self.config.voms)
        if proxy is not None:
            pFile = open(proxy_file, 'w')
            pFile.write(proxy)
            pFile.close()
        else:
            mainLog.error('failed to renew credential with {0}'.format(msg))
        return proxy is not None, msg
