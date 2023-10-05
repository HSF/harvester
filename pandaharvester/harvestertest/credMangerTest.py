import sys
import logging

from future.utils import iteritems

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_factory import PluginFactory


# Define a helper function - get list
def get_list(data):
    if isinstance(data, list):
        return data
    else:
        return [data]


pluginFactory = PluginFactory()

# get the configuration details - from the harvester config file

# get module and class names
moduleNames = get_list(harvester_config.credmanager.moduleName)
classNames = get_list(harvester_config.credmanager.className)
# file names of original certificates
if hasattr(harvester_config.credmanager, "inCertFile"):
    inCertFiles = get_list(harvester_config.credmanager.inCertFile)
else:
    inCertFiles = get_list(harvester_config.credmanager.certFile)
# file names of certificates to be generated
if hasattr(harvester_config.credmanager, "outCertFile"):
    outCertFiles = get_list(harvester_config.credmanager.outCertFile)
else:
    # use the file name of the certificate for panda connection as output name
    outCertFiles = get_list(harvester_config.pandacon.cert_file)
# VOMS
vomses = get_list(harvester_config.credmanager.voms)

# logger
_logger = core_utils.setup_logger("credManagerTest")

# get plugin(s)
exeCores = []
for moduleName, className, inCertFile, outCertFile, voms in zip(moduleNames, classNames, inCertFiles, outCertFiles, vomses):
    pluginPar = {}
    pluginPar["module"] = moduleName
    pluginPar["name"] = className
    pluginPar["inCertFile"] = inCertFile
    pluginPar["outCertFile"] = outCertFile
    pluginPar["voms"] = voms
    exeCore = pluginFactory.get_plugin(pluginPar)
    exeCores.append(exeCore)

# setup logger to write to screen also
for loggerName, loggerObj in iteritems(logging.Logger.manager.loggerDict):
    if loggerName.startswith("panda.log"):
        if len(loggerObj.handlers) == 0:
            continue
        if loggerName.split(".")[-1] in ["db_proxy"]:
            continue
        stdoutHandler = logging.StreamHandler(sys.stdout)
        stdoutHandler.setFormatter(loggerObj.handlers[0].formatter)
        loggerObj.addHandler(stdoutHandler)

# loop over all plugins
for exeCore in exeCores:
    # do nothing
    if exeCore is None:
        continue
    # make logger
    mainLog = core_utils.make_logger(_logger, "{0} {1}".format(exeCore.__class__.__name__, exeCore.outCertFile), method_name="execute")
    # list the plugin name
    mainLog.debug("plugin={0}".format(exeCore.__class__.__name__))
    # check credential
    mainLog.debug("check credential")
    isValid = exeCore.check_credential()
    if isValid:
        mainLog.debug("valid")
    elif not isValid:
        # renew it if necessary
        mainLog.debug("invalid")
        mainLog.debug("renew credential")
        tmpStat, tmpOut = exeCore.renew_credential()
        if not tmpStat:
            mainLog.error("failed : {0}".format(tmpOut))
            continue
    mainLog.debug("done")
