import os
import sys
import logging
import datetime
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
try:
    os.remove(harvester_config.db.database_filename)
except:
    pass

for loggerName, loggerObj in logging.Logger.manager.loggerDict.iteritems():
    if loggerName.startswith('panda.log'):
        if len(loggerObj.handlers) == 0:
            continue
        if loggerName.split('.')[-1] in ['db_proxy']:
            continue
        stdoutHandler = logging.StreamHandler(sys.stdout)
        stdoutHandler.setFormatter(loggerObj.handlers[0].formatter)
        loggerObj.addHandler(stdoutHandler)

queueConfigMapper = QueueConfigMapper()

proxy = DBProxy()
proxy.make_tables(queueConfigMapper)


a = CommunicatorPool()
return_object = a.check_panda()
print return_object

