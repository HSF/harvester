import sys
import os
import os.path
import hashlib
import uuid
import random
import string
import time
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.plugin_factory import PluginFactory

def dump(obj):
   for attr in dir(obj):
       if hasattr( obj, attr ):
           print( "obj.%s = %s" % (attr, getattr(obj, attr)))

ueueName = 'ALCF_Theta'
job_id = 1111
globus_sleep_time = 15  # seconds

if len(sys.argv) > 1:
   queueName = sys.argv[1]
if len(sys.argv) > 2:
   job_id = int(sys.argv[2])
if len(sys.argv) > 3:
   globus_sleep_time = int(sys.argv[3])

queueConfigMapper = QueueConfigMapper()
queueConfig = queueConfigMapper.get_queue(queueName)
print queueConfig.stager
print queueConfig.stager['Globus_srcPath']
print queueConfig.stager['srcEndpoint']
print queueConfig.stager['Globus_dstPath']
print queueConfig.stager['dstEndpoint']
print queueConfig.stager['zipDir']

print "Initial queueConfig.stager = ",queueConfig.stager
queueConfig.stager['module'] = 'pandaharvester.harvesterstager.go_stager'
queueConfig.stager['name'] = 'GlobusStager'
print "Modified queueConfig.stager = ",queueConfig.stager

scope = 'panda'

fileSpec = FileSpec()
fileSpec.fileType = 'es_output'
fileSpec.lfn = 'panda.sgotest.' + uuid.uuid4().hex + '.gz'
fileSpec.fileAttributes = {}
assFileSpec = FileSpec()
assFileSpec.lfn = 'panda.sgotest.' + uuid.uuid4().hex
assFileSpec.fileType = 'es_output'
assFileSpec.fsize = random.randint(10, 100)
# create source file
hash = hashlib.md5()
hash.update('%s:%s' % (scope, fileSpec.lfn))
hash_hex = hash.hexdigest()
correctedscope = "/".join(scope.split('.'))
assFileSpec.path = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(endPoint=queueConfig.stager['Globus_srcPath'],
                                                                     scope=correctedscope,
                                                                     hash1=hash_hex[0:2],
                                                                     hash2=hash_hex[2:4],
                                                                     lfn=assFileSpec.lfn)
if not os.path.exists(os.path.dirname(assFileSpec.path)):
   print "os.makedirs({})".format(os.path.dirname(assFileSpec.path))
   os.makedirs(os.path.dirname(assFileSpec.path))
oFile = open(assFileSpec.path, 'w')
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
jobSpec.computingSite = queueName
jobSpec.PandaID = job_id
jobSpec.add_out_file(fileSpec)

print "file to transfer - {}".format(assFileSpec.path) 
print "dump(jobSpec)"
#dump(jobSpec)
                                                                                                                                                           

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



print "testing stage-out"
tmpStat, tmpOut = stagerCore.trigger_stage_out(jobSpec)
if tmpStat:
    print " OK "
else:
    print " NG {0}".format(tmpOut)
    sys.exit(1)

print "sleep {0} seconds".format(globus_sleep_time)
time.sleep(globus_sleep_time)

print "checking status for transfer"
tmpStat, tmpOut = stagerCore.check_status(jobSpec)
if tmpStat:
    print " OK"
else:
    print " NG {0}".format(tmpOut)

 
