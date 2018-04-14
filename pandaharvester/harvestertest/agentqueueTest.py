import os
import sys
import time
import random

from future.utils import iteritems

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_factory import PluginFactory

from pandaharvester.harvestercore.agent_queues import MonitorQueue


# start test

mq = MonitorQueue()

print('sleepTime', mq.config.sleepTime)

def single_thread_test(nObjects=3):
    time_point = time.time()
    print('clear')
    mq.agent_queue.clear()
    print('size', mq.agent_queue.size())
    time_consumed = time.time() - time_point
    print('Time consumed: ', time_consumed)

    time_point = time.time()
    for i in range(nObjects):
        workspec = WorkSpec()
        workspec.workerID = i
        data = {'random': [random.random(), random.random()]}
        workspec.workAttributes = data

        # print('put')
        mq.agent_queue.put(workspec)
        # print('size', mq.agent_queue.size())
    time_consumed = time.time() - time_point
    print('Time consumed: {0} sec ; Avg: {1} obj/sec '.format(time_consumed, nObjects/time_consumed))

    print('_peek')
    print(mq.agent_queue._peek())

    time_point = time.time()
    for i in range(nObjects):
        # print('get')
        obj = mq.agent_queue.get()
        # print(obj)
        # print('size', mq.agent_queue.size())
    time_consumed = time.time() - time_point
    print('Time consumed: {0} sec ; Avg: {1} obj/sec '.format(time_consumed, nObjects/time_consumed))


single_thread_test(nObjects=1000)

# workspec1 = WorkSpec()
# workspec1.workerID = 777
# workspec1.computingSite = 'TEST-SITE1'
#
# workspec2 = WorkSpec()
# workspec2.workerID = 888
# workspec2.computingSite = 'TEST-SITE2'
#
# workspec_bulk = [workspec1, workspec2]
