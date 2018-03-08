import sys
import os
import os.path
import hashlib
import datetime
import uuid
import random
import string
import time
import threading
import logging
from future.utils import iteritems
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvesterbody.cacher import Cacher
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
from pandaharvester.harvestercore import core_utils  
from pandaharvester.harvestermisc import globus_utils

from globus_sdk import TransferClient
from globus_sdk import TransferData
from globus_sdk import NativeAppAuthClient
from globus_sdk import RefreshTokenAuthorizer


#initial variables
fileTableName = 'file_table'
queueName = 'ALCF_Theta'
begin_job_id = 1111
end_job_id = 1113
globus_sleep_time = 15

# connection lock
conLock = threading.Lock()


def dump(obj):
   for attr in dir(obj):
       if hasattr( obj, attr ):
           print( "obj.%s = %s" % (attr, getattr(obj, attr)))



if len(sys.argv) > 1:
   queueName = sys.argv[1]
if len(sys.argv) > 2:
   begin_job_id = int(sys.argv[2])
if len(sys.argv) > 3:
   end_job_id = int(sys.argv[3])
if len(sys.argv) > 4:
   globus_sleep_time = int(sys.argv[4])

queueConfigMapper = QueueConfigMapper()
queueConfig = queueConfigMapper.get_queue(queueName)
initial_queueConfig_preparator = queueConfig.preparator
queueConfig.preparator['module'] = 'pandaharvester.harvesterpreparator.go_bulk_preparator'
queueConfig.preparator['name'] = 'GlobusBulkPreparator'
modified_queueConfig_preparator = queueConfig.preparator

pluginFactory = PluginFactory()
# get stage-out plugin
preparatorCore = pluginFactory.get_plugin(queueConfig.preparator)

# logger
_logger = core_utils.setup_logger('stageInTest_go_bulk_preparator')
tmpLog = core_utils.make_logger(_logger, method_name='stageInTest_go_bulk_preparator')
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

Globus_srcPath = queueConfig.preparator['Globus_srcPath']
srcEndpoint = queueConfig.preparator['srcEndpoint']
basePath = queueConfig.preparator['basePath']
Globus_dstPath = queueConfig.preparator['Globus_dstPath']
dstEndpoint = queueConfig.preparator['dstEndpoint']

# check if db lock exits
locked = preparatorCore.dbInterface.get_object_lock('dummy_id_for_in_0',lock_interval=120)
if not locked:
   tmpLog.debug('DB Already locked by another thread')
# now unlock db
unlocked = preparatorCore.dbInterface.release_object_lock('dummy_id_for_in_0')
if unlocked :
   tmpLog.debug('unlocked db')
else:
   tmpLog.debug(' Could not unlock db')

# need to get client_id and refresh_token from PanDA server via harvester cache mechanism
c_data = preparatorCore.dbInterface.get_cache('globus_secret')
client_id = None
privateKey = None
if (not c_data == None) and  c_data.data['StatusCode'] == 0 :
   client_id = c_data.data['publicKey']  # client_id
   refresh_token = c_data.data['privateKey'] # refresh_token
else :
   client_id = None
   refresh_token = None
   tc = None
   errStr = 'failed to get Globus Client ID and Refresh Token'
   tmpLog.error(errStr)
   sys.exit(1)

# create Globus transfer client to send initial files to remote Globus source
tmpStat, tc = globus_utils.create_globus_transfer_client(tmpLog,client_id,refresh_token)
if not tmpStat:
   tc = None
   errStr = 'failed to create Globus Transfer Client'
   tmpLog.error(errStr)
   sys.exit(1)
try:
   # We are sending test files from our destination machine to the source machine
   # Test endpoints for activation
   tmpStatsrc, srcStr = globus_utils.check_endpoint_activation(tmpLog,tc,dstEndpoint)
   tmpStatdst, dstStr = globus_utils.check_endpoint_activation(tmpLog,tc,srcEndpoint)
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
   # We are sending test files from our destination machine to the source machine
   tdata = TransferData(tc,dstEndpoint,srcEndpoint,sync_level="checksum")
except:
   errStat, errMsg = globus_utils.handle_globus_exception(tmpLog)
   sys.exit(1)
 
# loop over the job id's creating various JobSpecs
jobSpec_list = []
for job_id in range(begin_job_id,end_job_id+1):
   jobSpec = JobSpec()
   jobSpec.jobParams = {
                        'scopeLog': 'panda',
                        'logFile': 'log',
                        }
   jobSpec.computingSite = queueName
   jobSpec.PandaID = job_id
   jobSpec.modificationTime = datetime.datetime.now()
   realDataset = 'panda.sgotest.' + uuid.uuid4().hex
   ddmEndPointIn = 'BNL-OSG2_DATADISK'
   inFiles_scope_str = ''
   inFiles_str = ''
   realDatasets_str = ''
   realDatasetsIn_str = ''
   ddmEndPointIn_str = ''
   GUID_str = ''
   fsize_str = '' 
   checksum_str = ''
   scope_in_str = ''

   # create up 5 files for input
   for index in range(random.randint(1, 5)):
      fileSpec = FileSpec()
      assFileSpec = FileSpec()
      # some dummy inputs
      GUID_str += 'd82e8e5e301b77489fd4da04bcdd6565,'
      fsize_str += '3084569129,'
      checksum_str += 'ad:9f60d29f,'
      scope_in_str += 'panda,'
      #
      fileSpec.fileType = 'input'
      assFileSpec.lfn = 'panda.sgotest.' + uuid.uuid4().hex
      fileSpec.lfn = assFileSpec.lfn 
      fileSpec.scope = 'panda'
      inFiles_scope_str += 'panda,' 
      inFiles_str += fileSpec.lfn + ','
      realDatasets_str += realDataset + ","
      realDatasetsIn_str += realDataset + ","
      ddmEndPointIn_str += ddmEndPointIn + ","
      assFileSpec.fileType = 'input'
      assFileSpec.fsize = random.randint(10, 100)
      # create source file
      hash = hashlib.md5()
      hash.update('%s:%s' % (fileSpec.scope, fileSpec.lfn))
      hash_hex = hash.hexdigest()
      correctedscope = "/".join(scope.split('.'))
      fileSpec.path = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(endPoint=queueConfig.preparator['Globus_dstPath'],
                                                                        scope=correctedscope,
                                                                        hash1=hash_hex[0:2],
                                                                        hash2=hash_hex[2:4],
                                                                        lfn=fileSpec.lfn)
      assFileSpec.path = fileSpec.path
      fileSpec.add_associated_file(assFileSpec)
      # add input file to jobSpec
      jobSpec.add_in_file(fileSpec)
      # now create the temporary file
      tmpfile_path = "{mountPoint}/testdata/{lfn}".format(mountPoint=queueConfig.preparator['basePath'],
                                                                        lfn=assFileSpec.lfn)

      if not os.path.exists(os.path.dirname(tmpfile_path)):
         tmpLog.debug("os.makedirs({})".format(os.path.dirname(tmpfile_path)))
         os.makedirs(os.path.dirname(tmpfile_path))
      oFile = open(tmpfile_path, 'w')
      oFile.write(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(assFileSpec.fsize)))
      oFile.close()
      # create destination file path
      destfile_path = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(endPoint=queueConfig.preparator['Globus_srcPath'],
                                                                        scope=correctedscope,
                                                                        hash1=hash_hex[0:2],
                                                                        hash2=hash_hex[2:4],
                                                                        lfn=fileSpec.lfn)
      # add to Globus transfer list
      tdata.add_item(tmpfile_path,destfile_path)
      #print "dump(fileSpec)"
      #dump(fileSpec)
      #
      tmpLog.debug("source file to transfer - {}".format(tmpfile_path)) 
      tmpLog.debug("destination file to transfer - {}".format(destfile_path)) 
      #print "dump(jobSpec)"
      #dump(jobSpec)
   # remove final ","
   realDatasetsIn_str=realDatasetsIn_str[:-1]
   inFiles_str = inFiles_str[:-1]
   inFiles_scope_str = inFiles_scope_str[:-1]
   GUID_str = GUID_str[:-1]
   fsize_str = fsize_str[:-1]
   checksum_str = checksum_str[:-1]
   scope_in_str = scope_in_str[:-1]
   jobSpec.jobParams['realDatasets'] = realDatasets_str
   jobSpec.jobParams['ddmEndPointIn'] = ddmEndPointIn_str
   jobSpec.jobParams['inFiles'] = inFiles_str
   jobSpec.jobParams['GUID'] = GUID_str
   jobSpec.jobParams['fsize'] = fsize_str
   jobSpec.jobParams['checksum'] = checksum_str
   jobSpec.jobParams['scopeIn'] = scope_in_str
   jobSpec.jobParams['realDatasetsIn'] = realDatasetsIn_str
   msgStr = "jobSpec.jobParams ={}".format(jobSpec.jobParams) 
   tmpLog.debug(msgStr)
   jobSpec_list.append(jobSpec)
 

# now load into DB JobSpec's and output FileSpec's from jobSpec_list
tmpStat = proxy.insert_jobs(jobSpec_list)
if tmpStat:
   msgStr = "OK Loaded jobs into DB"
   tmpLog.debug(msgStr)
else:
   msgStr = "NG Could not load jobs into DB"
   tmpLog.debug(msgStr)
tmpStat = proxy.insert_files(jobSpec_list)
if tmpStat:
   msgStr = "OK Loaded files into DB"
   tmpLog.debug(msgStr)
else:
   msgStr = "NG Could not load files into DB"
   tmpLog.debug(msgStr)

# transfer dummy files to Remote site for input 
transfer_result = tc.submit_transfer(tdata)
# check status code and message
tmpLog.debug(str(transfer_result))
if transfer_result['code'] == "Accepted":
   # succeeded
   # set transfer ID which are used for later lookup
   transferID = transfer_result['task_id']
   tmpLog.debug('done')
else:
   tmpLog.error('Failed to send intial files')
   sys.exit(3)

print "sleep {0} seconds".format(globus_sleep_time)
time.sleep(globus_sleep_time)

# enter polling loop to see if the intial files have transfered
maxloop = 5
iloop = 0
NotFound = True
while (iloop < maxloop) and NotFound :
   # get transfer task
   tmpStat, transferTasks = globus_utils.get_transfer_task_by_id(tmpLog,tc,transferID)
   # return a temporary error when failed to get task
   if not tmpStat:
      errStr = 'failed to get transfer task'
      tmpLog.error(errStr)
   else:
      # return a temporary error when task is missing 
      tmpLog.debug('transferTasks : {} '.format(transferTasks))
      if transferID not in transferTasks:
         errStr = 'transfer task ID - {} is missing'.format(transferID)
         tmpLog.error(errStr)
      else:
         # succeeded in finding a transfer task by tranferID
         if transferTasks[transferID]['status'] == 'SUCCEEDED':
            tmpLog.debug('transfer task {} succeeded'.format(transferID))
            NotFound = False
         # failed
         if transferTasks[transferID]['status'] == 'FAILED':
            errStr = 'transfer task {} failed'.format(transferID)
            tmpLog.error(errStr)
      # another status
      tmpStr = 'transfer task {0} status: {1}'.format(transferID,transferTasks[transferID]['status'])
      tmpLog.debug(tmpStr)
   if NotFound :
      print "sleep {0} seconds".format(globus_sleep_time)
      time.sleep(globus_sleep_time)
      ++iloop
if NotFound :
   errStr = 'transfer task ID - {} is missing'.format(transferID)
   tmpLog.error(errStr)
   sys.exit(1)


print "plugin={0}".format(preparatorCore.__class__.__name__)

print "testing stagein:"
print "BasePath from preparator configuration: %s " % preparatorCore.basePath

# Now loop over the jobSpec's

for jobSpec in jobSpec_list:
   # print out jobSpec PandID
   msgStr = "jobSpec PandaID - {}".format(jobSpec.PandaID)
   msgStr = "testing trigger_stage_out"
   tmpLog.debug(msgStr)
   tmpStat, tmpOut = preparatorCore.trigger_preparation(jobSpec)
   if tmpStat:
      msgStr = " OK "
      tmpLog.debug(msgStr)
   elif tmpStat == None:
      msgStr = " Temporary failure NG {0}".format(tmpOut)
      tmpLog.debug(msgStr)
   elif not tmpStat:
      msgStr = " NG {0}".format(tmpOut)
      tmpLog.debug(msgStr)
      sys.exit(1)
   print
   # check status to actually trigger transfer
   # get the files with the group_id and print out
   msgStr = "dummy_transfer_id = {}".format(preparatorCore.get_dummy_transfer_id())
   files = proxy.get_files_with_group_id(preparatorCore.get_dummy_transfer_id())
   files = preparatorCore.dbInterface.get_files_with_group_id(preparatorCore.get_dummy_transfer_id())
   msgStr = "checking status for transfer and perhaps ultimately triggering the transfer"
   tmpLog.debug(msgStr)
   tmpStat, tmpOut = preparatorCore.check_status(jobSpec)
   if tmpStat:
      msgStr = " OK"
      tmpLog.debug(msgStr)
   elif tmpStat == None:
      msgStr = " Temporary failure NG {0}".format(tmpOut)
      tmpLog.debug(msgStr)
   elif not tmpStat:
      msgStr = " NG {0}".format(tmpOut)
      tmpLog.debug(msgStr)

# sleep for 10 minutes 1 second
      
msgStr = "Sleep for 601 seconds"
#msgStr = "Sleep for 181 seconds"
tmpLog.debug(msgStr)
#time.sleep(181)
time.sleep(601)
msgStr = "now check the jobs"
tmpLog.debug(msgStr)

for jobSpec in jobSpec_list:
   # print out jobSpec PandID
   msgStr = "jobSpec PandaID - {}".format(jobSpec.PandaID)
   tmpLog.debug(msgStr)
   msgStr = "checking status for transfer and perhaps ultimately triggering the transfer"
   tmpStat, tmpOut = preparatorCore.check_status(jobSpec)
   if tmpStat:
      msgStr = " OK"
      tmpLog.debug(msgStr)
   elif tmpStat == None:
      msgStr = " Temporary failure NG {0}".format(tmpOut)
      tmpLog.debug(msgStr)
   elif not tmpStat:
      msgStr = " NG {0}".format(tmpOut)
      tmpLog.debug(msgStr)

# sleep for 3 minutes
      
msgStr = "Sleep for 180 seconds"
tmpLog.debug(msgStr)
time.sleep(180)
msgStr = "now check the jobs"
tmpLog.debug(msgStr)

for jobSpec in jobSpec_list:
   # print out jobSpec PandID
   msgStr = "jobSpec PandaID - {}".format(jobSpec.PandaID)
   tmpLog.debug(msgStr)
   msgStr = "checking status for transfer and perhaps ultimately triggering the transfer"
   tmpStat, tmpOut = preparatorCore.check_status(jobSpec)
   if tmpStat:
      msgStr = " OK"
      tmpLog.debug(msgStr)
   elif tmpStat == None:
      msgStr = " Temporary failure NG {0}".format(tmpOut)
      tmpLog.debug(msgStr)
   elif not tmpStat:
      msgStr = " NG {0}".format(tmpOut)
      tmpLog.debug(msgStr)
