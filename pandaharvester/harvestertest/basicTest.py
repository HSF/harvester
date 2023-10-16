import os
import sys
import logging
import datetime
from future.utils import iteritems
from pandaharvester.harvesterconfig import harvester_config

try:
    os.remove(harvester_config.db.database_filename)
except Exception:
    pass
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool

for loggerName, loggerObj in iteritems(logging.Logger.manager.loggerDict):
    if loggerName.startswith("panda.log"):
        if len(loggerObj.handlers) == 0:
            continue
        if loggerName.split(".")[-1] in ["db_proxy"]:
            continue
        stdoutHandler = logging.StreamHandler(sys.stdout)
        stdoutHandler.setFormatter(loggerObj.handlers[0].formatter)
        loggerObj.addHandler(stdoutHandler)

queueConfigMapper = QueueConfigMapper()

proxy = DBProxy()
proxy.make_tables(queueConfigMapper)

job = JobSpec()
job.PandaID = 1


job.modificationTime = datetime.datetime.now()
proxy.insert_jobs([job])

newJob = proxy.get_job(1)


a = CommunicatorPool()
a.get_jobs("siteName", "nodeName", "prodSourceLabel", "computingElement", 1, {})
