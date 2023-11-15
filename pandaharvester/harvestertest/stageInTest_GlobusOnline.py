import sys
import time

from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pilot.info.filespec import FileSpec

queueName = sys.argv[1]

queueConfigMapper = QueueConfigMapper()

queueConfig = queueConfigMapper.get_queue(queueName)

jobSpec = JobSpec()
new_file_data = {"scope": "test", "lfn": "TXT.19772875._044894.tar.gz.1", "attemptNr": 0}
new_file_spec = FileSpec(filetype="input", **new_file_data)
new_file_spec.attemptNr = 0
new_file_spec.path = "/home/psvirin/harvester3"

jobSpec.inFiles = {new_file_spec}
jobSpec.outFiles = {}
jobSpec.jobParams = {
    "inFiles": "TXT.19772875._044894.tar.gz.1",
    "scopeIn": "mc15_13TeV",
    "fsize": "658906675",
    "GUID": "7e3776f9bb0af341b03e59d3de895a13",
    "checksum": "ad:3734bdd9",
    "ddmEndPointIn": "BNL-OSG2_DATADISK",
    "realDatasetsIn": "mc15_13TeV.363638.MGPy8EG_N30NLO_Wmunu_Ht500_700_BFilter.merge.DAOD_STDM4.e4944_s2726_r7772_r7676_p2842_tid09596175_00",
}
jobSpec.computingSite = queueName
jobSpec.PandaID = "11111"


pluginFactory = PluginFactory()

# get plugin
preparatorCore = pluginFactory.get_plugin(queueConfig.preparator)
print(f"plugin={preparatorCore.__class__.__name__}")

print(jobSpec)

print("testing stagein:")
print(f"BasePath from preparator configuration: {preparatorCore.basePath} ")
preparatorCore.basePath = preparatorCore.basePath + "/testdata/"
print(f"basePath redifuned for test data: {preparatorCore.basePath} ")

tmpStat, tmpOut = preparatorCore.trigger_preparation(jobSpec)
if tmpStat:
    print(" OK")
else:
    print(f" NG {tmpOut}")

print

print("testing status check")
while True:
    tmpStat, tmpOut = preparatorCore.check_stage_in_status(jobSpec)
    if tmpStat is True:
        print(" OK")
        break
    elif tmpStat is False:
        print(f" NG {tmpOut}")
        sys.exit(1)
    else:
        print(" still running. sleep 1 min")
        time.sleep(60)

print

print("checking path resolution")
tmpStat, tmpOut = preparatorCore.resolve_input_paths(jobSpec)
if tmpStat:
    print(f" OK {jobSpec.jobParams['inFilePaths']}")
else:
    print(f" NG {tmpOut}")
