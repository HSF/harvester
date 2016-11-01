import sys

queueName = sys.argv[1]

from pandaharvester.harvestercore.QueueConfigMapper import QueueConfigMapper

queueConfigMapper = QueueConfigMapper()

queueConfig = queueConfigMapper.getQueue(queueName)

import os
import uuid
import random
import string
from pandaharvester.harvestercore.JobSpec import JobSpec
from pandaharvester.harvestercore.FileSpec import FileSpec

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
fileSpec.addAssociatedFile(assFileSpec)
jobSpec = JobSpec()
jobSpec.jobParams = {'outFiles': fileSpec.lfn + ',log',
                     'scopeOut': 'panda',
                     'scopeLog': 'panda',
                     'logFile': 'log',
                     'realDatasets': 'panda.' + fileSpec.lfn,
                     'ddmEndPointOut': 'BNL-OSG2_DATADISK',
                     }
jobSpec.addOutFile(fileSpec)

from pandaharvester.harvestercore.PluginFactory import PluginFactory

pluginFactory = PluginFactory()

# get stage-out plugin
stagerCore = pluginFactory.getPlugin(queueConfig.stager)
print "plugin={0}".format(stagerCore.__class__.__name__)

print "testing zip"
tmpStat, tmpOut = stagerCore.zipOutput(jobSpec)
if tmpStat:
    print " OK"
else:
    print " NG {0}".format(tmpOut)

print

sys.exit(0)

print "testing standard stage-out"
tmpStat, tmpOut = stagerCore.triggerStageOut(jobSpec)
if tmpStat:
    transferID = fileSpec.fileAttributes['transferID']
    print " OK transferID={0}".format(transferID)
else:
    print " NG {0}".format(tmpOut)
    sys.exit(1)

print

print "checking status for transferID={0}".format(transferID)
tmpStat, tmpOut = stagerCore.checkStatus(jobSpec)
if tmpStat:
    print " OK"
else:
    print " NG {0}".format(tmpOut)
