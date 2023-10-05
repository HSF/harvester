from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
import sys

queueName = sys.argv[1]


queueConfigMapper = QueueConfigMapper()

queueConfig = queueConfigMapper.get_queue(queueName)


jobSpec = JobSpec()
jobSpec.computingSite = sys.argv[1]
jobSpec.jobParams = {
    "inFiles": "EVNT.06820166._000001.pool.root.1",
    "scopeIn": "mc15_13TeV",
    "fsize": "196196765",
    "GUID": "B7F387CD-1F97-1C47-88BD-D8785442C49D",
    "checksum": "ad:326e445d",
    "ddmEndPointIn": "MWT2_DATADISK",
    "realDatasetsIn": "mc15_13TeV:mc15_13TeV.301042.PowhegPythia8EvtGen_AZNLOCTEQ6L1_DYtautau_250M400.evgen.EVNT.e3649_tid06820166_00",
}


pluginFactory = PluginFactory()

# get plugin
preparatorCore = pluginFactory.get_plugin(queueConfig.preparator)
print("plugin={0}".format(preparatorCore.__class__.__name__))

print("testing preparation")
tmpStat, tmpOut = preparatorCore.trigger_preparation(jobSpec)
if tmpStat:
    print(" OK")
else:
    print(" NG {0}".format(tmpOut))

print

print("testing status check")
while True:
    tmpStat, tmpOut = preparatorCore.check_stage_in_status(jobSpec)
    if tmpStat is True:
        print(" OK")
        break
    elif tmpStat is False:
        print(" NG {0}".format(tmpOut))
        sys.exit(1)
    else:
        print(" still running. sleep 1 min")
        time.sleep(60)

print

print("checking path resolution")
tmpStat, tmpOut = preparatorCore.resolve_input_paths(jobSpec)
if tmpStat:
    print(" OK {0}".format(jobSpec.jobParams["inFilePaths"]))
else:
    print(" NG {0}".format(tmpOut))
