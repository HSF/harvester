try:
    os.remove(harvester_config.db.database_filename)
except:
    pass

import datetime
from pandaharvester.harvestercore.DBProxy import DBProxy
from pandaharvester.harvestercore.JobSpec import JobSpec
from pandaharvester.harvestercore.QueueConfigMapper import QueueConfigMapper
from pandaharvester.harvestercore.CommunicatorPool import CommunicatorPool

queueConfigMapper = QueueConfigMapper()

proxy = DBProxy()
proxy.makeTables(queueConfigMapper)

job = JobSpec()
job.PandaID = 1


job.modificationTime = datetime.datetime.now()
proxy.insertJobs([job])

newJob = proxy.getJob(1)


a = CommunicatorPool()
a.getJobs('siteName', 'nodeName', 'prodSourceLabel', 'computingElement', 1)
