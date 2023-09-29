import os
import sys

from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
from pandaharvester.harvestermessenger import shared_file_messenger

workerID = int(sys.argv[1])

proxy = DBProxy()
workSpec = proxy.get_worker_with_id(workerID)
jobSpec = proxy.get_jobs_with_worker_id(workerID, None)[0]

accessPoint = workSpec.get_access_point()

try:
    os.makedirs(accessPoint)
except BaseException:
    pass

node = {}
node["pandaID"] = jobSpec.PandaID
node["jobsetID"] = jobSpec.jobParams["jobsetID"]
node["taskID"] = jobSpec.taskID


a = CommunicatorPool()
tmpStat, tmpVal = a.getEventRanges(node)

mess = shared_file_messenger.SharedFileMessenger()
mess.feed_events(workSpec, tmpVal)
