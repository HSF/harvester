import os
import sys

from pandaharvester.harvestercore.communicator_pool import CommunicatorPool


pandaid = int(sys.argv[1])
jobsetid = int(sys.argv[2])
taskid = int(sys.argv[3])

try:
    n = int(sys.argv[4])
except Exception:
    n = 1

data = {pandaid: {"pandaID": pandaid, "taskID": taskid, "jobsetID": jobsetid, "nRanges": n}}

a = CommunicatorPool()
o = a.get_event_ranges(data, False, os.getcwd())
