import os
import sys

from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy

workerID = int(sys.argv[1])
status = sys.argv[2]

proxy = DBProxy()
workSpec = proxy.get_worker_with_id(workerID)

accessPoint = workSpec.get_access_point()

try:
    os.makedirs(accessPoint)
except BaseException:
    pass

f = open(os.path.join(accessPoint, "status.txt"), "w")
f.write(status)
f.close()
