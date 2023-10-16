import os
import sys
import json
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestermessenger import shared_file_messenger

workerID = int(sys.argv[1])

eventID = sys.argv[2]
status = sys.argv[3]

proxy = DBProxy()
workSpec = proxy.get_worker_with_id(workerID)
jobSpec = proxy.get_jobs_with_worker_id(workerID, None)[0]

accessPoint = workSpec.get_access_point()

try:
    os.makedirs(accessPoint)
except BaseException:
    pass

node = {}
node["eventRangeID"] = eventID
node["eventStatus"] = status

f = open(os.path.join(accessPoint, shared_file_messenger.jsonEventsUpdateFileName), "w")
json.dump([node], f)
f.close()

if status == "finished":
    lfn = id + ".data"
    data = {}
    data["path"] = os.path.join(accessPoint, lfn)
    data["type"] = "output"
    data["fsize"] = 10 * 1024 * 1024
    data["eventRangeID"] = eventID
    node = {}
    node[lfn] = data
    f = open(os.path.join(accessPoint, shared_file_messenger.jsonOutputsFileName), "w")
    json.dump(node, f)
    f.close()
