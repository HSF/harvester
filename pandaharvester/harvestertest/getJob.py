import os
import sys

workerID = int(sys.argv[1])

from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy

proxy = DBProxy()
workSpec = proxy.get_worker_with_id(workerID)

accessPoint = workSpec.get_access_point()

try:
    os.makedirs(accessPoint)
except:
    pass

from pandaharvester.harvestermessenger import shared_file_messenger

f = open(os.path.join(accessPoint, shared_file_messenger.jsonJobRequestFileName), 'w')
f.close()
