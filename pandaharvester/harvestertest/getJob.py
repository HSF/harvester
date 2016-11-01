import os
import sys

workerID = int(sys.argv[1])

from pandaharvester.harvestercore.DBProxy import DBProxy

proxy = DBProxy()
workSpec = proxy.getWorkerWithID(workerID)

accessPoint = workSpec.getAccessPoint()

try:
    os.makedirs(accessPoint)
except:
    pass

from pandaharvester.harvestermessenger import SharedFileMessenger

f = open(os.path.join(accessPoint, SharedFileMessenger.jsonJobRequestFileName), 'w')
f.close()
