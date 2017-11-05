import sys
import time
import os
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.job_spec import JobSpec

def dump(obj):
   for attr in dir(obj):
       if hasattr( obj, attr ):
           print( "obj.%s = %s" % (attr, getattr(obj, attr)))

print len(sys.argv)
queueName = 'ALCF_Theta'
job_id = 1111
globus_sleep_time = 15

if len(sys.argv) > 1:
   queueName = sys.argv[1]
if len(sys.argv) > 2:
   job_id = int(sys.argv[2])
if len(sys.argv) > 3:
   globus_sleep_time = int(sys.argv[3])


queueConfigMapper = QueueConfigMapper()
queueConfig = queueConfigMapper.get_queue(queueName)
initial_queueConfig_preparator = queueConfig.preparator
queueConfig.preparator['module'] = 'pandaharvester.harvesterpreparator.go_preparator'
queueConfig.preparator['name'] = 'GlobusPreparator'
modified_queueConfig_preparator = queueConfig.preparator

pluginFactory = PluginFactory()
# get stage-out plugin
preparatorCore = pluginFactory.get_plugin(queueConfig.preparator)

# logger
_logger = core_utils.setup_logger('stageInTest_go_preparator')
tmpLog = core_utils.make_logger(_logger, method_name='stageInTest_go_preparator')
tmpLog.debug('start')

for loggerName, loggerObj in logging.Logger.manager.loggerDict.iteritems():
   #print "loggerName - {}".format(loggerName)
   if loggerName.startswith('panda.log'):
      if len(loggerObj.handlers) == 0:
         continue
      if loggerName.split('.')[-1] in ['db_proxy']:
         continue
      stdoutHandler = logging.StreamHandler(sys.stdout)
      stdoutHandler.setFormatter(loggerObj.handlers[0].formatter)
      loggerObj.addHandler(stdoutHandler)

msgStr = "plugin={0}".format(preparatorCore.__class__.__name__)
tmpLog.debug(msgStr)
msgStr = "Initial queueConfig.preparator = {}".format(initial_queueConfig_preparator)
tmpLog.debug(msgStr)
msgStr = "Modified queueConfig.preparator = {}".format(modified_queueConfig_preparator)
tmpLog.debug(msgStr)

scope = 'panda'


proxy = DBProxy()
communicator = CommunicatorPool()
cacher = Cacher(communicator, single_mode=True)
cacher.run()

Globus_srcPath = queueConfig.preparator['Globus_dstPath']
srcEndpoint = queueConfig.preparator['dstEndpoint']
basePath = queueConfig.preparator['basePath']
Globus_dstPath = queueConfig.preparator['Globus_srcPath']
dstEndpoint = queueConfig.preparator['scrEndpoint']

# create Globus transfer client to send initial files to remote Globus source
tc = globus_utils.create_globus_transfer_client(client_id,privateKey)
if not tc :
   try:
      # Test endpoints for activation
      tmpStatsrc, srcStr = globus_utils.check_endpoint_activation(tc,srcEndpoint)
      tmpStatdst, dstStr = globus_utils.check_endpoint_activation(tc,dstEndpoint)
      if tmpStatsrc and tmpStatdst:
         errStr = 'source Endpoint and destination Endpoint activated'
         tmpLog.debug(errStr)
      else:
         errStr = ''
         if not tmpStatsrc :
            errStr += ' source Endpoint not activated '
         if not tmpStatdst :
            errStr += ' destination Endpoint not activated '
         tmpLog.error(errStr)
         sys.exit(2)
      # both endpoints activated now prepare to transfer data
      tdata = TransferData(tc,srcEndpoint,dstEndpoint,sync_level="checksum")
   except:
      errStat, errMsg = globus_utils.handle_globus_exception(tmpLog)
      sys.exit(1)
else : 
   errStr = 'failed to create Initial globus transfer client'
   tmpLog.error(errStr)
   sys.exit(1)


#dump(queueConfig)

jobSpec = JobSpec()

jobSpec.computingSite = queueName
jobSpec.PandaID = job_id
jobSpec.modificationTime = datetime.datetime.now()
realDataset = 'panda.sgotest.' + uuid.uuid4().hex
ddmEndPointOut = 'BNL-OSG2_DATADISK'
outFiles_scope_str = ''
outFiles_str = ''
realDatasets_str = ''
ddmEndPointOut_str = ''
# create up 5 files for input
for index in range(random.randint(1, 5)):
   fileSpec = FileSpec()
   assFileSpec = FileSpec()
   fileSpec.fileType = 'input'
   assFileSpec.lfn = 'panda.sgotest.' + uuid.uuid4().hex
   fileSpec.lfn = assFileSpec.lfn 
   fileSpec.scope = 'panda'
   outFiles_scope_str += 'panda,' 
   outFiles_str += fileSpec.lfn + ','
   realDatasets_str += realDataset + ","
   ddmEndPointOut_str += ddmEndPointOut + ","
   assFileSpec.fileType = 'input'
   assFileSpec.fsize = random.randint(10, 100)
   # create source file
   hash = hashlib.md5()
   hash.update('%s:%s' % (fileSpec.scope, fileSpec.lfn))
   hash_hex = hash.hexdigest()
   correctedscope = "/".join(scope.split('.'))
   fileSpec.path = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(endPoint=queueConfig.preparator['Globus_srcPath'],
                                                                     scope=correctedscope,
                                                                     hash1=hash_hex[0:2],
                                                                     hash2=hash_hex[2:4],
                                                                     lfn=fileSpec.lfn)
   # now create the temporary file
   assFileSpec.path = "{mountPoint}/testdata/{lfn}".format(mountPoint=queueConfig.preparator['basePath'],
                                                           lfn=assFileSpec.lfn)

   if not os.path.exists(os.path.dirname(assFileSpec.path)):
      tmpLog.debug("os.makedirs({})".format(os.path.dirname(assFileSpec.path)))
      os.makedirs(os.path.dirname(assFileSpec.path))
   oFile = open(assFileSpec.path, 'w')
   oFile.write(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(assFileSpec.fsize)))
   oFile.close()
   fileSpec.add_associated_file(assFileSpec)
   # add to Globus transfer list
   tdata.add_item(assFileSpec.path,fileSpec.path)
   #print "dump(fileSpec)"
   #dump(fileSpec)
   # add input file to jobSpec
   jobSpec.add_in_file(fileSpec)
   #
   tmpLog.debug("source file to transfer - {}".format(assFileSpec.path)) 
   tmpLog.debug("destination file to transfer - {}".format(fileSpec.path)) 
   #print "dump(jobSpec)"
   #dump(jobSpec)
# add log file info
outFiles_str += 'log'
realDatasets_str += 'log.'+ uuid.uuid4().hex
ddmEndPointOut_str += 'MWT2-UC_DATADISK'
# remove final ","
outFiles_scope_str = outFiles_scope_str[:-1]
jobSpec.jobParams['realDatasets'] = realDatasets_str
msgStr = "jobSpec.jobParams ={}".format(jobSpec.jobParams) 
tmpLog.debug(msgStr)
msgStr = "len(jobSpec.get_output_file_attributes()) = {0} type - {1}".format(len(jobSpec.get_output_file_attributes()),type(jobSpec.get_output_file_attributes()))
tmpLog.debug(msgStr)
for key, value in jobSpec.get_output_file_attributes().iteritems():
   msgStr = "output file attributes - pre DB {0} {1}".format(key,value)
   tmpLog.debug(msgStr)


print queueConfig.preparator
print queueConfig.preparator['Globus_srcPath']
print queueConfig.preparator['srcEndpoint']
print queueConfig.preparator['Globus_dstPath']
print queueConfig.preparator['dstEndpoint']

print "dump(jobSpec)"
#dump(jobSpec)

from pandaharvester.harvestercore.plugin_factory import PluginFactory

pluginFactory = PluginFactory()

# get plugin
#GoPreparator  
preparatorCore = pluginFactory.get_plugin(queueConfig.preparator)
print "plugin={0}".format(preparatorCore.__class__.__name__)

print "testing stagein:"
print "BasePath from preparator configuration: %s " % preparatorCore.basePath
preparatorCore.basePath = preparatorCore.basePath + "/testdata"
print "basePath redefined for test data: %s " % preparatorCore.basePath


tmpStat, tmpOut = preparatorCore.trigger_preparation(jobSpec)
if tmpStat:
    print " OK"
else:
    print " NG {0}".format(tmpOut)

print "sleep {0} seconds".format(globus_sleep_time)
time.sleep(globus_sleep_time)

print "testing status check"
while True:
    tmpStat, tmpOut = preparatorCore.check_status(jobSpec)
    if tmpStat == True:
        print " OK"
        break
    elif tmpStat == False:
        print " NG {0}".format(tmpOut)
        sys.exit(1)
    else:
        print " still running. sleep 1 min"
        time.sleep(60)

print

print "checking path resolution"
tmpStat, tmpOut = preparatorCore.resolve_input_paths(jobSpec)
if tmpStat:
    print " OK {0}".format(jobSpec.jobParams['inFilePaths'])
else:
    print " NG {0}".format(tmpOut)
