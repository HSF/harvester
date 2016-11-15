import sys
import os
import uuid
import random
import string
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.plugin_factory import PluginFactory

queueName = sys.argv[1]
queueConfigMapper = QueueConfigMapper()
queueConfig = queueConfigMapper.get_queue(queueName)

fileSpec = FileSpec()
fileSpec.fileType = 'output'
fileSpec.lfn = 'panda.sgotest.' + uuid.uuid4().hex + '.gz'
fileSpec.fileAttributes = {}
assFileSpec = FileSpec()
assFileSpec.lfn = 'panda.sgotest.' + uuid.uuid4().hex
assFileSpec.fileType = 'output'
assFileSpec.fsize = random.randint(10, 100)
assFileSpec.path = os.getcwd() + '/' + assFileSpec.lfn
oFile = open(assFileSpec.lfn, 'w')
oFile.write(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(assFileSpec.fsize)))
oFile.close()
fileSpec.add_associated_file(assFileSpec)
jobSpec = JobSpec()
jobSpec.jobParams = {'outFiles': fileSpec.lfn + ',log',
                     'scopeOut': 'panda',
                     'scopeLog': 'panda',
                     'logFile': 'log',
                     'realDatasets': 'panda.' + fileSpec.lfn,
                     'ddmEndPointOut': 'BNL-OSG2_DATADISK',
                     }
jobSpec.add_out_file(fileSpec)

pluginFactory = PluginFactory()

# get stage-out plugin
stagerCore = pluginFactory.get_plugin(queueConfig.stager)
print "plugin={0}".format(stagerCore.__class__.__name__)

print "testing zip"
tmpStat, tmpOut = stagerCore.zip_output(jobSpec)
if tmpStat:
    print " OK"
else:
    print " NG {0}".format(tmpOut)

print

print "testing standard stage-out"
tmpStat, tmpOut = stagerCore.trigger_stage_out(jobSpec)
if tmpStat:
    transferID = fileSpec.fileAttributes['transferID']
    print " OK transferID={0}".format(transferID)
else:
    print " NG {0}".format(tmpOut)
    sys.exit(1)

print

print "checking status for transferID={0}".format(transferID)
tmpStat, tmpOut = stagerCore.check_status(jobSpec)
if tmpStat:
    print " OK"
else:
    print " NG {0}".format(tmpOut)
