import os
from pandaharvester.harvesterconfig import harvester_config

try:
    os.remove(harvester_config.db.database_filename)
except:
    pass

from pandaharvester.harvestercore.DBProxy import DBProxy
from pandaharvester.harvestercore.JobSpec import JobSpec
from pandaharvester.harvestercore.QueueConfigMapper import QueueConfigMapper

from pandaharvester.harvesterconfig import harvester_config

queueConfigMapper = QueueConfigMapper()

proxy = DBProxy()
proxy.makeTables(queueConfigMapper)

job = JobSpec()
job.PandaID = 1
import datetime
job.modificationTime = datetime.datetime.now()
print "check database access"
print "INSERT ..."
tmpRet = proxy.insertJobs([job])
if tmpRet:
    print " OK"
else:
    print " NG"
print "READ ..."
newJob = proxy.getJob(1)
if newJob != None:
    print " OK"
else:
    print " NG"

print
print "check panda access"
from pandaharvester.harvestercore.CommunicatorPool import CommunicatorPool
a = CommunicatorPool()
tmpRet,tmpCode,tmpStr = a.isAlive()
if tmpRet == True and tmpCode == 200 and tmpStr == 'alive=yes':
    print " OK"
else:
    print " NG ret:{0} code:{1} out:{2}".format(tmpRet,tmpCode,tmpStr)
print
