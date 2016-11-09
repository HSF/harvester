import os
import sys
import json

from pandaharvester.harvestercore.DBProxy import DBProxy
from pandaharvester.harvestercore.CommunicatorPool import CommunicatorPool
from pandaharvester.harvestermessenger import SharedFileMessenger

workerID = int(sys.argv[1])

proxy = DBProxy()
workSpec = proxy.getWorkerWithID(workerID)
jobSpec = proxy.getJobsWithWorkerID(workerID, None)[0]

accessPoint = workSpec.getAccessPoint()

try:
    os.makedirs(accessPoint)
except:
    pass

node = {}
node['pandaID'] = jobSpec.PandaID
node['jobsetID'] = jobSpec.jobParams['jobsetID']
node['taskID'] = jobSpec.taskID


a = CommunicatorPool()
tmpStat, tmpVal = a.getEventRanges(node)

mess = SharedFileMessenger.SharedFileMessenger()
mess.feedEvents(workSpec, tmpVal)

