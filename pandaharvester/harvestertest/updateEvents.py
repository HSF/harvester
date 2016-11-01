import os
import sys
import json

workerID = int(sys.argv[1])

id = sys.argv[2]
status = sys.argv[3]

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
node['eventRangeID'] = id
node['eventStatus'] = status

from pandaharvester.harvestermessenger import SharedFileMessenger

f = open(os.path.join(accessPoint, SharedFileMessenger.jsonEventsUpdateFileName), 'w')
json.dump([node], f)
f.close()

if status == 'finished':
    lfn = id + '.data'
    data = {}
    data['path'] = os.path.join(accessPoint, lfn)
    data['type'] = 'output'
    data['fsize'] = 10 * 1024 * 1024
    data['eventRangeID'] = id
    node = {}
    node[lfn] = data
    f = open(os.path.join(accessPoint, SharedFileMessenger.jsonOutputsFileName), 'w')
    json.dump(node, f)
    f.close()
