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

from pandaharvester.harvestercore.fifos import MonitorFIFO


# start test

mq = MonitorFIFO()

print("sleepTime", mq.config.sleepTime)


def single_thread_test(nObjects=3, protective=False):
    time_point = time.time()
    print("clear")
    mq.fifo.clear()
    print("size", mq.size())
    time_consumed = time.time() - time_point
    print("Time consumed: ", time_consumed)

    time_point = time.time()
    for i in range(nObjects):
        workspec = WorkSpec()
        workspec.workerID = i
        data = {"random": [random.random(), random.random()]}
        workspec.workAttributes = data
        # print('put')
        mq.put(workspec)
        # print('size', mq.size())
    time_consumed = time.time() - time_point
    print("Time consumed: {0} sec ; Avg: {1} obj/sec ".format(time_consumed, nObjects / time_consumed))

    print("size", mq.size())

    print("peek")
    print(mq.peek())

    time_point = time.time()
    for i in range(nObjects):
        # print('get')
        obj = mq.get(timeout=3, protective=protective)
        # print(obj)
        # print('size', mq.size())
    time_consumed = time.time() - time_point
    print("Time consumed: {0} sec ; Avg: {1} obj/sec ".format(time_consumed, nObjects / time_consumed))


print("Normal test")
single_thread_test(nObjects=1000)
print("Protective test")
single_thread_test(nObjects=1000, protective=True)

mq.fifo.clear()

time_point = time.time()
print("MonitorFIFO.populate")
mq.populate(seconds_ago=0, clear_fifo=True)
time_consumed = time.time() - time_point
print("Time consumed: ", time_consumed)

# workspec1 = WorkSpec()
# workspec1.workerID = 777
# workspec1.computingSite = 'TEST-SITE1'
#
# workspec2 = WorkSpec()
# workspec2.workerID = 888
# workspec2.computingSite = 'TEST-SITE2'
#
# workspec_bulk = [workspec1, workspec2]
