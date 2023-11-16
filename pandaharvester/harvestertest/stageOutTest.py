import atexit
import os
import random
import string
import sys
import uuid

from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper

file_prefix = "panda.sgotest."


def exit_func():
    for f in os.listdir("."):
        if f.startswith(file_prefix):
            os.remove(f)


atexit.register(exit_func)

queueName = sys.argv[1]
queueConfigMapper = QueueConfigMapper()
queueConfig = queueConfigMapper.get_queue(queueName)

fileSpec = FileSpec()
fileSpec.fileType = "output"
fileSpec.lfn = file_prefix + uuid.uuid4().hex + ".gz"
fileSpec.fileAttributes = {"guid": str(uuid.uuid4())}
fileSpec.chksum = "0d439274"
assFileSpec = FileSpec()
assFileSpec.lfn = file_prefix + uuid.uuid4().hex
assFileSpec.fileType = "es_output"
assFileSpec.fsize = random.randint(10, 100)
assFileSpec.path = os.getcwd() + "/" + assFileSpec.lfn
oFile = open(assFileSpec.lfn, "w")
oFile.write("".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(assFileSpec.fsize)))
oFile.close()
fileSpec.add_associated_file(assFileSpec)
jobSpec = JobSpec()
jobSpec.jobParams = {
    "outFiles": fileSpec.lfn + ",log",
    "scopeOut": "panda",
    "scopeLog": "panda",
    "logFile": "log",
    "realDatasets": "panda." + fileSpec.lfn,
    "ddmEndPointOut": "BNL-OSG2_DATADISK",
}
jobSpec.add_out_file(fileSpec)

pluginFactory = PluginFactory()

# get stage-out plugin
stagerCore = pluginFactory.get_plugin(queueConfig.stager)
print(f"plugin={stagerCore.__class__.__name__}")

print("testing zip")
tmpStat, tmpOut = stagerCore.zip_output(jobSpec)
if tmpStat:
    print(" OK")
else:
    print(f" NG {tmpOut}")

print()

print("testing stage-out")
transferID = None
tmpStat, tmpOut = stagerCore.trigger_stage_out(jobSpec)
if tmpStat:
    if fileSpec.fileAttributes is None and "transferID" in fileSpec.fileAttributes:
        transferID = fileSpec.fileAttributes["transferID"]
    print(f" OK transferID={transferID}")
else:
    print(f" NG {tmpOut}")
    sys.exit(1)

print()

print(f"checking status for transferID={transferID}")
tmpStat, tmpOut = stagerCore.check_stage_out_status(jobSpec)
if tmpStat:
    print(" OK")
else:
    print(f" NG {tmpOut}")
