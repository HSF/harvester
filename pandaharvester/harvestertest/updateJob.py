import os
import sys

workerID = int(sys.argv[1])
status = sys.argv[2]

from pandaharvester.harvestercore.db_proxy import DBProxy

proxy = DBProxy()
workSpec = proxy.get_worker_with_id(workerID)

accessPoint = workSpec.get_access_point()

try:
    os.makedirs(accessPoint)
except:
    pass

f = open(os.path.join(accessPoint, 'status.txt'), 'w')
f.write(status)
f.close()
