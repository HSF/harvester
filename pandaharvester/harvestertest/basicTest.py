import os
import datetime
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.db_proxy import DBProxy
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
try:
    os.remove(harvester_config.db.database_filename)
except:
    pass

queueConfigMapper = QueueConfigMapper()

proxy = DBProxy()
proxy.make_tables(queueConfigMapper)

job = JobSpec()
job.PandaID = 1


job.modificationTime = datetime.datetime.now()
proxy.insert_jobs([job])

newJob = proxy.get_job(1)


a = CommunicatorPool()
a.get_jobs('siteName', 'nodeName', 'prodSourceLabel', 'computingElement', 1)
