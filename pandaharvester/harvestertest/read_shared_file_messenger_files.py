#!/bin/env python
import json
import os
import re
import os.path
import tarfile
import fnmatch
import sys
from pprint import pprint

from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
from pandaharvester.harvestercore.job_spec import JobSpec

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.event_spec import EventSpec
from pandaharvester.harvesterconfig import harvester_config

# list of shared_file_messenger files
file_list = []

# json for worker attributes
jsonAttrsFileName = harvester_config.payload_interaction.workerAttributesFile
file_list.append(("json for worker attributes", jsonAttrsFileName))

# json for job report
jsonJobReport = harvester_config.payload_interaction.jobReportFile
file_list.append(("json for job report", jsonJobReport))

# json for outputs
jsonOutputsFileName = harvester_config.payload_interaction.eventStatusDumpJsonFile
file_list.append(("json for outputs", jsonOutputsFileName))

# xml for outputs
xmlOutputsBaseFileName = harvester_config.payload_interaction.eventStatusDumpXmlFile

# json for job request
jsonJobRequestFileName = harvester_config.payload_interaction.jobRequestFile
file_list.append(("json for job request", jsonJobRequestFileName))

# json for job spec
jsonJobSpecFileName = harvester_config.payload_interaction.jobSpecFile
file_list.append(("json for job spec", jsonJobSpecFileName))

# json for event request
jsonEventsRequestFileName = harvester_config.payload_interaction.eventRequestFile
file_list.append(("json for event request", jsonEventsRequestFileName))

# json to feed events
jsonEventsFeedFileName = harvester_config.payload_interaction.eventRangesFile
file_list.append(("json to feed events", jsonEventsFeedFileName))

# json to update events
jsonEventsUpdateFileName = harvester_config.payload_interaction.updateEventsFile
file_list.append(("json to update events", jsonEventsUpdateFileName))


access_point = sys.argv[1]


# Now loop over all of the json files "
for description, jsonFileName in file_list:
    print("{0} : {1}".format(description, jsonFileName))
    jsonFilePath = os.path.join(access_point, jsonFileName)
    print("looking for attributes file {0}".format(jsonFilePath))
    if not os.path.exists(jsonFilePath):
        # not found
        print("not found")
    else:
        try:
            with open(jsonFilePath) as data_file:
                data = json.load(data_file)
            pprint(data)
        except BaseException:
            print("failed to load {0}".format(jsonFilePath))
            continue
