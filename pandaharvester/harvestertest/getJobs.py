#
# This file is used to call the dbproxy and get the list of all jobs in the database
#
import os
import sys
import logging
import datetime
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.db_proxy import DBProxy
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool

for loggerName, loggerObj in logging.Logger.manager.loggerDict.iteritems():
    if loggerName.startswith('panda.log'):
        if len(loggerObj.handlers) == 0:
            continue
        if loggerName.split('.')[-1] in ['db_proxy']:
            continue
        stdoutHandler = logging.StreamHandler(sys.stdout)
        stdoutHandler.setFormatter(loggerObj.handlers[0].formatter)
        loggerObj.addHandler(stdoutHandler)

queueName = sys.argv[1]
queueConfigMapper = QueueConfigMapper()
queueConfig = queueConfigMapper.get_queue(queueName)

proxy = DBProxy()

# get all jobs in table
print 'try to get all jobs'
alljobs = proxy.get_jobs()
print('got {0} jobs'.format(len(alljobs)))
# loop over all found jobs
if len(alljobs) > 0 :
    for jobSpec in alljobs:
        print(' PandaID = %d status = %s subStatus = %s lockedBy = %s' %(jobSpec.PandaID,jobSpec.status,jobSpec.subStatus,jobSpec.lockedBy))


