import os
os.environ['PANDA_HOME'] = '/afs/cern.ch/user/t/tmaeno/harvester'

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
proxy.insertJobs([job])

newJob = proxy.getJob(1)


from pandaharvester.harvestercore.CommunicatorPool import CommunicatorPool
a = CommunicatorPool()
a.getJobs('siteName','nodeName','prodSourceLabel','computingElement',1)
