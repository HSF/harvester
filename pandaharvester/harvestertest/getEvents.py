import os
import sys
import json

workerID = int(sys.argv[1])

from pandaharvester.harvestercore.DBProxy import DBProxy

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

from pandaharvester.harvestermessenger import SharedFileMessenger

f = open(os.path.join(accessPoint, SharedFileMessenger.jsonEventsRequestFileName), 'w')
json.dump(node, f)
f.close()
