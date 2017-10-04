import sys
import os
import os.path
import hashlib
import datetime
import uuid
import random
import string
import time
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.core_utils import core_utils  

fileTableName = 'file_table'
queueName = 'ALCF_Theta'
begin_job_id = 1111
end_job_id = 1113

# logger
_logger = core_utils.setup_logger('stageOutTest_go_bulk_stager')


def dump(obj):
   for attr in dir(obj):
       if hasattr( obj, attr ):
           print( "obj.%s = %s" % (attr, getattr(obj, attr)))

# define a variation on db_proxy.insert_jobs to insert files into the files table
def insert_files(jobspec_list,dbproxy_obj):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name='insert_files')
        tmpLog.debug('{0} jobs'.format(len(jobspec_list)))
        try:
            # sql to insert a file
            sqlF = "INSERT INTO {0} ({1}) ".format(fileTableName, FileSpec.column_names())
            sqlF += FileSpec.bind_values_expression()
            # loop over all jobs
            varMapsF = []
            for jobSpec in jobspec_list:
                for fileSpec in jobSpec.outFiles:
                    varMap = fileSpec.values_list()
                    varMapsF.append(varMap)
            # insert
            dbproxy_obj.executemany(sqlF, varMapsF)
            # commit
            dbproxy_obj.commit()
            # return
            return True
        except:
            # roll back
            dbproxy_obj.rollback()
            # dump error
            core_utils.dump_error_message(tmpLog)
            # return
            return False





if len(sys.argv) > 1:
   queueName = sys.argv[1]
if len(sys.argv) > 2:
   begin_job_id = int(sys.argv[2])
if len(sys.argv) > 3:
   end_job_id = int(sys.argv[3])

queueConfigMapper = QueueConfigMapper()
queueConfig = queueConfigMapper.get_queue(queueName)
print queueConfig.stager
print queueConfig.stager['Globus_srcPath']
print queueConfig.stager['srcEndpoint']
print queueConfig.stager['Globus_dstPath']
print queueConfig.stager['dstEndpoint']
print queueConfig.stager['zipDir']

print "Initial queueConfig.stager = ",queueConfig.stager
queueConfig.stager['module'] = 'pandaharvester.harvesterstager.go_bulk_stager'
queueConfig.stager['name'] = 'GlobusBulkStager'
print "Modified queueConfig.stager = ",queueConfig.stager


scope = 'panda'

proxy = DBProxy()
pluginFactory = PluginFactory()

# get stage-out plugin
stagerCore = pluginFactory.get_plugin(queueConfig.stager)
print "plugin={0}".format(stagerCore.__class__.__name__)

# check if db lock exits
locked = stagerCore.dbInterface.get_object_lock('dummy_id_for_out_0',lock_interval=120)
if not locked:
   print 'Already locked by another thread'
# now unlock db
unlocked = stagerCore.dbInterface.release_object_lock('dummy_id_for_out_0')
if unlocked :
   print 'unlocked db'
else:
   print ' Could not unlock db'

# loop over the job id's creating various JobSpecs
jobSpec_list = []
for job_id in range(begin_job_id,end_job_id+1):
   jobSpec = JobSpec()
   jobSpec.jobParams = {'scopeOut': 'panda',
                        'scopeLog': 'panda',
                        'logFile': 'log',
                        'realDatasets': 'panda.sgotest.' + uuid.uuid4().hex , 
                        'ddmEndPointOut': 'BNL-OSG2_DATADISK'
                        }
   jobSpec.computingSite = queueName
   jobSpec.PandaID = job_id
   jobSpec.modificationTime = datetime.datetime.now()
   # create up 5 files for output
   for index in range(random.randint(1, 5)):
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
      # add output file to jobSpec
      jobSpec.add_out_file(fileSpec)
      #
      print "file to transfer - {}".format(assFileSpec.path) 
      #print "dump(jobSpec)"
      dump(jobSpec)
   jobSpec_list.append(jobSpec)

# now load into DB FileSpec's from jobSpec_list
tmpStat = insert_files(jobSpec_list,proxy)
if tmpStat:
   print "OK Loaded files into DB"
else:
   print "NG Could not loaded files into DB"

# Now loop over the jobSpec's

for jobSpec in jobSpec_list:
   # print out jobSpec PandID
   print "jobSpec PandaID - {}".format(jobSpec.PandaID)
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
   elif tmpStat == None:
      print " Temporary failure NG {0}".format(tmpOut)
   elif not tmpStat:
      print " NG {0}".format(tmpOut)
      sys.exit(1)
   print
   # get the files with the group_id and print out
   print "dummy_transfer_id = {}".format(stagerCore.get_dummy_transfer_id())
   files = proxy.get_files_with_group_id(stagerCore.get_dummy_transfer_id())
   print "files - {}".format(files)
   files = stagerCore.dbInterface.get_files_with_group_id(stagerCore.get_dummy_transfer_id())
   print "files - {}".format(files)


   print "checking status for transfer and perhaps ultimately triggering the transfer"
   tmpStat, tmpOut = stagerCore.check_status(jobSpec)
   if tmpStat:
      print " OK"
   elif tmpStat == None:
      print " Temporary failure NG {0}".format(tmpOut)
   elif not tmpStat:
      print " NG {0}".format(tmpOut)

# sleep for 10 minutes 1 second
      
#print "Sleep for 601 seconds"
#time.sleep(601)
print "Sleep for 181 seconds"
time.sleep(181)
print "now check the jobs"

for jobSpec in jobSpec_list:
   # print out jobSpec PandID
   print "jobSpec PandaID - {}".format(jobSpec.PandaID)
   print
   print "checking status for transfer and perhaps ultimately triggering the transfer"
   tmpStat, tmpOut = stagerCore.check_status(jobSpec)
   if tmpStat:
      print " OK"
   elif tmpStat == None:
      print " Temporary failure NG {0}".format(tmpOut)
   elif not tmpStat:
      print " NG {0}".format(tmpOut)
