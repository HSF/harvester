import os
import sys
from future.utils import iteritems

from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
from pandaharvester.harvestercore.job_spec import JobSpec

queueName = sys.argv[1]
queueConfigMapper = QueueConfigMapper()
queueConfig = queueConfigMapper.get_queue(queueName)

pluginFactory = PluginFactory()

com = CommunicatorPool()

# get job
jobSpecList = []
if queueConfig.mapType != WorkSpec.MT_NoJob:
    jobs, errStr = com.get_jobs(queueConfig.queueName, 'nodeName', queueConfig.prodSourceLabel,
                                'computingElement', 1, None)
    if len(jobs) == 0:
        print ("Failed to get jobs at {0} due to {1}".format(queueConfig.queueName, errStr))
        sys.exit(0)

    jobSpec = JobSpec()
    jobSpec.convert_job_json(jobs[0])

    # set input file paths
    inFiles = jobSpec.get_input_file_attributes()
    for inLFN, inFile in iteritems(inFiles):
        inFile['path'] = '{0}/{1}'.format(os.getcwd(), inLFN)
    jobSpec.set_input_file_paths(inFiles)
    jobSpecList.append(jobSpec)

maker = pluginFactory.get_plugin(queueConfig.workerMaker)
workSpec = maker.make_worker(jobSpecList, queueConfig)

workSpec.accessPoint = queueConfig.messenger['accessPoint']
workSpec.mapType = queueConfig.mapType
workSpec.computingSite = queueConfig.queueName

# set job to worker if not job-level late binding
if not queueConfig.useJobLateBinding:
    workSpec.hasJob = 1
    workSpec.set_jobspec_list(jobSpecList)

messenger = pluginFactory.get_plugin(queueConfig.messenger)
messenger.setup_access_points([workSpec])

# get plugin for messenger
if queueConfig.mapType != WorkSpec.MT_NoJob:
    messenger = pluginFactory.get_plugin(queueConfig.messenger)
    messenger.feed_jobs(workSpec, jobSpecList)

    jobSpec = jobSpecList[0]
    if 'eventService' in jobSpec.jobParams:
        workSpec.eventsRequest = WorkSpec.EV_useEvents

    if workSpec.hasJob == 1 and workSpec.eventsRequest == WorkSpec.EV_useEvents:
        workSpec.eventsRequest = WorkSpec.EV_requestEvents
        eventsRequestParams = dict()
        eventsRequestParams[jobSpec.PandaID] = {'pandaID': jobSpec.PandaID,
                                                'taskID': jobSpec.taskID,
                                                'jobsetID': jobSpec.jobParams['jobsetID'],
                                                'nRanges': jobSpec.jobParams['coreCount'],
                                                }
        workSpec.eventsRequestParams = eventsRequestParams

        tmpStat, events = com.get_event_ranges(workSpec.eventsRequestParams)
        # failed
        if tmpStat is False:
            print ('failed to get events with {0}'.format(events))
            sys.exit(0)
        tmpStat = messenger.feed_events(workSpec, events)
        if tmpStat is False:
            print ('failed to feed events with {0}'.format(events))
            sys.exit(0)

# get submitter plugin
submitterCore = pluginFactory.get_plugin(queueConfig.submitter)
print ("testing submission with plugin={0}".format(submitterCore.__class__.__name__))
tmpRetList = submitterCore.submit_workers([workSpec])
tmpStat, tmpOut = tmpRetList[0]
if tmpStat:
    print (" OK batchID={0}".format(workSpec.batchID))
else:
    print (" NG {0}".format(tmpOut))
    sys.exit(1)

print

# get monitoring plug-in
monCore = pluginFactory.get_plugin(queueConfig.monitor)
print ("testing monitoring for batchID={0} with plugin={1}".format(workSpec.batchID,
                                                                   monCore.__class__.__name__))
tmpStat, tmpOut = monCore.check_workers([workSpec])
tmpOut = tmpOut[0]
if tmpStat:
    print (" OK workerStatus={0}".format(tmpOut[0]))
else:
    print (" NG {0}".format(tmpOut[1]))
    sys.exit(1)
