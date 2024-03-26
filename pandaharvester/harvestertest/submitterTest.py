import os
import sys

from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.resource_type_constants import (
    BASIC_RESOURCE_TYPE_SINGLE_CORE,
)
from pandaharvester.harvestercore.resource_type_mapper import ResourceTypeMapper
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestermisc import signal_utils

fork_child_pid = os.fork()
if fork_child_pid != 0:
    signal_utils.set_suicide_handler(None)
    os.wait()
else:
    if len(sys.argv) not in (2, 4):
        print("Wrong number of parameters. You can either:")
        print("  - specify the queue name")
        print("  - specify the queue name, jobType (managed, user) and resourceType (e.g. SCORE, SCORE_HIMEM, MCORE, MCORE_HIMEM)")
        sys.exit(0)

    queueName = sys.argv[1]
    queueConfigMapper = QueueConfigMapper()
    queueConfig = queueConfigMapper.get_queue(queueName)

    resource_type_mapper = ResourceTypeMapper()

    if queueConfig.prodSourceLabel in ("user", "managed"):
        jobType = queueConfig.prodSourceLabel
    else:
        jobType = "managed"  # default, can be overwritten by parameters

    resourceType = BASIC_RESOURCE_TYPE_SINGLE_CORE  # default, can be overwritten by parameters

    if len(sys.argv) == 4:
        # jobType should be 'managed' or 'user'. If not specified will default to a production job
        if sys.argv[2] in ("user", "managed"):
            jobType = sys.argv[2]
        else:
            print(f"value for jobType not valid, defaulted to {jobType}")

        # resourceType should be a valid resource type, e.g. 'SCORE', 'SCORE_HIMEM', 'MCORE', 'MCORE_HIMEM'. If not specified defaults to single core
        if resource_type_mapper.is_valid_resource_type(sys.argv[3]):
            resourceType = sys.argv[3]
        else:
            print(f"value for resourceType not valid, defaulted to {resourceType}")

    print(f"Running with queueName:{queueName}, jobType:{jobType}, resourceType:{resourceType}")

    pluginFactory = PluginFactory()

    com = CommunicatorPool()

    # get job
    jobSpecList = []
    if queueConfig.mapType != WorkSpec.MT_NoJob:
        jobs, errStr = com.get_jobs(queueConfig.queueName, "nodeName", queueConfig.prodSourceLabel, "computingElement", 1, None)
        if len(jobs) == 0:
            print(f"Failed to get jobs at {queueConfig.queueName} due to {errStr}")
            sys.exit(0)

        jobSpec = JobSpec()
        jobSpec.convert_job_json(jobs[0])

        # set input file paths
        inFiles = jobSpec.get_input_file_attributes()
        for inLFN, inFile in inFiles.items():
            inFile["path"] = f"{os.getcwd()}/{inLFN}"
        jobSpec.set_input_file_paths(inFiles)
        jobSpecList.append(jobSpec)

    maker = pluginFactory.get_plugin(queueConfig.workerMaker)
    workSpec = maker.make_worker(jobSpecList, queueConfig, jobType, resourceType)

    workSpec.accessPoint = queueConfig.messenger["accessPoint"]
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
        if "eventService" in jobSpec.jobParams:
            workSpec.eventsRequest = WorkSpec.EV_useEvents

        if workSpec.hasJob == 1 and workSpec.eventsRequest == WorkSpec.EV_useEvents:
            workSpec.eventsRequest = WorkSpec.EV_requestEvents
            eventsRequestParams = dict()
            eventsRequestParams[jobSpec.PandaID] = {
                "pandaID": jobSpec.PandaID,
                "taskID": jobSpec.taskID,
                "jobsetID": jobSpec.jobParams["jobsetID"],
                "nRanges": jobSpec.jobParams["coreCount"],
            }
            workSpec.eventsRequestParams = eventsRequestParams

            tmpStat, events = com.get_event_ranges(workSpec.eventsRequestParams, False, os.getcwd())
            # failed
            if tmpStat is False:
                print(f"failed to get events with {events}")
                sys.exit(0)
            tmpStat = messenger.feed_events(workSpec, events)
            if tmpStat is False:
                print(f"failed to feed events with {events}")
                sys.exit(0)

    # get submitter plugin
    submitterCore = pluginFactory.get_plugin(queueConfig.submitter)
    print(f"testing submission with plugin={submitterCore.__class__.__name__}")
    tmpRetList = submitterCore.submit_workers([workSpec])
    tmpStat, tmpOut = tmpRetList[0]
    if tmpStat:
        print(f" OK batchID={workSpec.batchID}")
    else:
        print(f" NG {tmpOut}")
        sys.exit(1)

    print("")

    # get monitoring plug-in
    monCore = pluginFactory.get_plugin(queueConfig.monitor)
    print(f"testing monitoring for batchID={workSpec.batchID} with plugin={monCore.__class__.__name__}")
    tmpStat, tmpOut = monCore.check_workers([workSpec])
    tmpOut = tmpOut[0]
    if tmpStat:
        print(f" OK workerStatus={tmpOut[0]}")
    else:
        print(f" NG {tmpOut[1]}")
        sys.exit(1)
