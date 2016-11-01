import os
import sys

workerID = int(sys.argv[1])
status = sys.argv[2]

from pandaharvester.harvestercore.DBProxy import DBProxy

proxy = DBProxy()
workSpec = proxy.getWorkerWithID(workerID)

accessPoint = workSpec.getAccessPoint()

try:
    os.makedirs(accessPoint)
except:
    pass

f = open(os.path.join(accessPoint, 'status.txt'), 'w')
f.write(status)
f.close()
