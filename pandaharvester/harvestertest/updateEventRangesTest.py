import sys
import uuid

from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
from pandaharvester.harvestercore.event_spec import EventSpec
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.job_spec import JobSpec

rID = sys.argv[1]
taskid = rID.split("-")[0]
pandaid = int(rID.split("-")[1])

job = JobSpec()
job.PandaID = pandaid
event = EventSpec()
file = FileSpec()
file.status = "finished"
file.objstoreID = 9575
file.pathConvention = 1000
file.lfn = str(uuid.uuid4().hex) + ".zip"
file.fsize = 555
file.chksum = "0d2a9dc9"
event.eventRangeID = rID
event.eventStatus = "finished"
job.zipEventMap = {1: {"events": [event], "zip": file}}


a = CommunicatorPool()
a.update_jobs([job])
