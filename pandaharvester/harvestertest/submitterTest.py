import sys

queueName = sys.argv[1]

from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper

queueConfigMapper = QueueConfigMapper()

queueConfig = queueConfigMapper.get_queue(queueName)

import os
from pandaharvester.harvestercore.work_spec import WorkSpec

workSpec = WorkSpec()
workSpec.accessPoint = os.getcwd()

from pandaharvester.harvestercore.plugin_factory import PluginFactory

pluginFactory = PluginFactory()

# get submitter plugin
submitterCore = pluginFactory.get_plugin(queueConfig.submitter)
print "testing submission with plugin={0}".format(submitterCore.__class__.__name__)
tmpRetList = submitterCore.submit_workers([workSpec])
tmpStat, tmpOut = tmpRetList[0]
if tmpStat:
    print " OK batchID={0}".format(workSpec.batchID)
else:
    print " NG {0}".format(tmpOut)
    sys.exit(1)

print

# get monitoring plug-in
monCore = pluginFactory.get_plugin(queueConfig.monitor)
print "testing monitoring for batchID={0} with plugin={1}".format(workSpec.batchID,
                                                                  monCore.__class__.__name__)
tmpStat, tmpOut = monCore.check_workers([workSpec])
tmpOut = tmpOut[0]
if tmpStat:
    print " OK workerStatus={0}".format(tmpOut[0])
else:
    print " NG {0}".format(tmpOut[1])
    sys.exit(1)
