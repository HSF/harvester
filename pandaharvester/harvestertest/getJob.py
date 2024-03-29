import os
import sys

from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestermessenger import shared_file_messenger

workerID = int(sys.argv[1])


proxy = DBProxy()
workSpec = proxy.get_worker_with_id(workerID)

accessPoint = workSpec.get_access_point()

try:
    os.makedirs(accessPoint)
except BaseException:
    pass


f = open(os.path.join(accessPoint, shared_file_messenger.jsonJobRequestFileName), "w")
f.close()
