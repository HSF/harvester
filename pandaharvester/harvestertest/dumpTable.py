import os
import sys
import logging
import datetime
import sqlite3
import pprint
from future.utils import iteritems
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool


for loggerName, loggerObj in iteritems(logging.Logger.manager.loggerDict):
    if loggerName.startswith('panda.log'):
        if len(loggerObj.handlers) == 0:
            continue
        if loggerName.split('.')[-1] in ['db_proxy']:
            continue
        stdoutHandler = logging.StreamHandler(sys.stdout)
        stdoutHandler.setFormatter(loggerObj.handlers[0].formatter)
        loggerObj.addHandler(stdoutHandler)

pp = pprint.PrettyPrinter(indent=4)

queueConfigMapper = QueueConfigMapper()

proxy = DBProxy()

sqlJ ="SELECT * FROM job_table"

resultsJobcur = proxy.execute(sqlJ)
resultsJob = resultsJobcur.fetchall()
proxy.commit()

sqlF ="SELECT * FROM file_table"

resultsFilescur = proxy.execute(sqlF)
resultsFiles = resultsFilescur.fetchall()
proxy.commit()

print "job_table - "
print resultsJob[0].keys()
for row in resultsJob:
    print tuple(row)
print

print "file_table - "
print resultsFiles[0].keys()
for row in resultsFiles:
    print tuple(row)
#pp.pprint(resultsFiles)
print
