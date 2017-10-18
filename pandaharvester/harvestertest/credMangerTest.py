import sys
import os
import uuid
import random
import string

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_factory import PluginFactory

pluginFactory = PluginFactory()

# get plugin
pluginPar = {}
pluginPar['module'] = harvester_config.credmanager.moduleName
pluginPar['name'] = harvester_config.credmanager.className
pluginPar['config'] = harvester_config.credmanager
pluginPar['outCertFile'] = harvester_config.credmanager.certFile
pluginPar['voms'] = 'atlas'

ProxyCacheCredManager = pluginFactory.get_plugin(pluginPar)
print "plugin={0}".format(ProxyCacheCredManager.__class__.__name__)

# check credential
print "check credential"
isValid = ProxyCacheCredManager.check_credential()
if isValid:
    print "credential is valid"
elif not isValid:
    # renew it if necessary
    print "credential is invalid!!!"
    print
    print "renew credential"
    tmpStat, tmpOut = ProxyCacheCredManager.renew_credential()
    if not tmpStat:
        print "failed : {0}".format(tmpOut)


