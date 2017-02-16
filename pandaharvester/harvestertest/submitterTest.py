import os
import sys

from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
from pandaharvester.harvestercore.job_spec import JobSpec

queueName = sys.argv[1]
queueConfigMapper = QueueConfigMapper()
queueConfig = queueConfigMapper.get_queue(queueName)

# communicator
com = CommunicatorPool()
jobs = com.get_jobs(queueConfig.queueName, 'nodeName', queueConfig.prodSourceLabel, 'computingElement', 1)
if len(jobs) == 0:
    print "No jobs at {0}. Please submit a task or job to PanDA beforehand".format(queueConfig.queueName)
    sys.exit(0)

jobSpec = JobSpec()
jobSpec.convert_job_json(jobs[0])

# set input file paths
inFiles = jobSpec.get_input_file_attributes()
for inLFN, inFile in inFiles.iteritems():
    inFile['path'] = '{0}/{1}'.format(os.getcwd(), inLFN)
jobSpec.set_input_file_paths(inFiles)

workSpec = WorkSpec()
workSpec.accessPoint = os.getcwd()
workSpec.mapType = WorkSpec.MT_OneToOne

pluginFactory = PluginFactory()

# get plugin for messenger
messenger = pluginFactory.get_plugin(queueConfig.messenger)
messenger.feed_jobs(workSpec, [jobSpec])

# get submitter plugin
submitterCore = pluginFactory.get_plugin(queueConfig.submitter)
print "testing submission with plugin={0}".format(submitterCore.__class__.__name__)
tmpRetList = submitterCore.submit_workers([workSpec])
tmpStat, tmpOut = tmpRetList[0]
if tmpStat:
    print " OK batchID={0}".format(workSpec.batchID)
else:
    print " NG {0}".format(tmpOut)
    sys.exit(1)

print

# get monitoring plug-in
monCore = pluginFactory.get_plugin(queueConfig.monitor)
print "testing monitoring for batchID={0} with plugin={1}".format(workSpec.batchID,
                                                                  monCore.__class__.__name__)
tmpStat, tmpOut = monCore.check_workers([workSpec])
tmpOut = tmpOut[0]
if tmpStat:
    print " OK workerStatus={0}".format(tmpOut[0])
else:
    print " NG {0}".format(tmpOut[1])
    sys.exit(1)
