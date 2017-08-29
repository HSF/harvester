import os
import json

from pandaharvester.harvesterconfig import harvester_config
from work_spec import WorkSpec
from panda_queue_spec import PandaQueueSpec


# class for queue config
class QueueConfig:

    def __init__(self, queue_name):
        self.queueName = queue_name
        # default parameters
        self.mapType = WorkSpec.MT_OneToOne
        self.useJobLateBinding = False
        self.zipPerMB = None
        self.siteName = ''
        self.maxWorkers = 0
        self.nNewWorkers = 0
        self.maxNewWorkersPerCycle = 0
        self.noHeartbeat = ''
        self.runMode = 'self'
        self.resourceType = PandaQueueSpec.RT_catchall
        self.getJobCriteria = None
        self.ddmEndpointIn = None
        self.allowJobMixture = False

    # get list of status without heartbeat
    def get_no_heartbeat_status(self):
        return self.noHeartbeat.split(',')

    # check if status without heartbeat
    def is_no_heartbeat_status(self, status):
        return status in self.get_no_heartbeat_status()


# mapper
class QueueConfigMapper:
    # constructor
    def __init__(self):
        self.queueConfig = {}
        # define config file path
        if os.path.isabs(harvester_config.qconf.configFile):
            confFilePath = harvester_config.qconf.configFile
        else:
            # check if in PANDA_HOME
            confFilePath = None
            if 'PANDA_HOME' in os.environ:
                confFilePath = os.path.join(os.environ['PANDA_HOME'],
                                            'etc/panda',
                                            harvester_config.qconf.configFile)
                if not os.path.exists(confFilePath):
                    confFilePath = None
            # look into /etc/panda
            if confFilePath is None:
                confFilePath = os.path.join('/etc/panda',
                                            harvester_config.qconf.configFile)
        # load config from json
        f = open(confFilePath)
        queueConfigJson = json.load(f)
        f.close()
        # set attributes
        for queueName, queueDict in queueConfigJson.iteritems():
            queueConfig = QueueConfig(queueName)
            for key, val in queueDict.iteritems():
                setattr(queueConfig, key, val)
            # queueName = siteName/resourceType
            queueConfig.siteName = queueConfig.queueName.split('/')[0]
            if queueConfig.siteName != queueConfig.queueName:
                queueConfig.resourceType = queueConfig.queueName.split('/')[-1]
            # additional criteria for getJob
            if queueConfig.getJobCriteria is not None:
                tmpCriteria = dict()
                for tmpItem in queueConfig.getJobCriteria.split(','):
                    tmpKey, tmpVal = tmpItem.split('=')
                    tmpCriteria[tmpKey] = tmpVal
                if len(tmpCriteria) == 0:
                    queueConfig.getJobCriteria = None
                else:
                    queueConfig.getJobCriteria = tmpCriteria
            self.queueConfig[queueName] = queueConfig

    # check if valid queue
    def has_queue(self, queue_name):
        return queue_name in self.queueConfig

    # get queue config
    def get_queue(self, queue_name):
        if not self.has_queue(queue_name):
            return None
        return self.queueConfig[queue_name]

    # all queue configs
    def get_all_queues(self):
        return self.queueConfig
