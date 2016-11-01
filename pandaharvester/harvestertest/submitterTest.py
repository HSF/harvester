import sys

queueName = sys.argv[1]

from pandaharvester.harvestercore.QueueConfigMapper import QueueConfigMapper

queueConfigMapper = QueueConfigMapper()

queueConfig = queueConfigMapper.getQueue(queueName)

import os
from pandaharvester.harvestercore.WorkSpec import WorkSpec

workSpec = WorkSpec()
workSpec.accessPoint = os.getcwd()

from pandaharvester.harvestercore.PluginFactory import PluginFactory

pluginFactory = PluginFactory()

# get submitter plugin
submitterCore = pluginFactory.getPlugin(queueConfig.submitter)
print "testing submission with plugin={0}".format(submitterCore.__class__.__name__)
tmpRetList = submitterCore.submitWorkers([workSpec])
tmpStat, tmpOut = tmpRetList[0]
if tmpStat:
    print " OK batchID={0}".format(workSpec.batchID)
else:
    print " NG {0}".format(tmpOut)
    sys.exit(1)

print

# get monitoring plug-in
monCore = pluginFactory.getPlugin(queueConfig.monitor)
print "testing monitoring for batchID={0} with plugin={1}".format(workSpec.batchID,
                                                                  monCore.__class__.__name__)
tmpStat, tmpOut = monCore.checkWorkers([workSpec])
tmpOut = tmpOut[0]
if tmpStat:
    print " OK workerStatus={0}".format(tmpOut[0])
else:
    print " NG {0}".format(tmpOut[1])
    sys.exit(1)
