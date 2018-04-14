import os
import sys
from future.utils import iteritems

from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_factory import PluginFactory

from pandaharvester.harvestercore.agent_queues import MonitorQueue


# start test

mq = MonitorQueue()

print('sleepTime', mq.config.sleepTime)

workspec1 = WorkSpec()
workspec1.workerID = 777
workspec1.computingSite = 'TEST-SITE1'

workspec2 = WorkSpec()
workspec2.workerID = 888
workspec2.computingSite = 'TEST-SITE2'

workspec_bulk = [workspec1, workspec2]

print('size', mq.agent_queue.size())

print('put')
mq.agent_queue.put(workspec_bulk)

print('size', mq.agent_queue.size())

print('_peek', mq.agent_queue._peek())

print('get')
print(mq.agent_queue.get())

print('size', mq.agent_queue.size())
