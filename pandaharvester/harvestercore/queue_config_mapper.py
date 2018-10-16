import os
import json
import copy
import datetime
import threading
import importlib
import six
from future.utils import iteritems

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError

from pandaharvester.harvesterconfig import harvester_config
from .work_spec import WorkSpec
from .panda_queue_spec import PandaQueueSpec
from . import core_utils
from .core_utils import SingletonWithID
from .db_proxy_pool import DBProxyPool as DBProxy
from .plugin_factory import PluginFactory
from .queue_config_dump_spec import QueueConfigDumpSpec
from .db_interface import DBInterface


# logger
_logger = core_utils.setup_logger('queue_config_mapper')
_dbInterface = DBInterface()

# make logger
def _make_logger(base_log=_logger, token=None, method_name=None, send_dialog=True):
    if send_dialog and _dbInterface:
        hook = _dbInterface
    else:
        hook = None
    return core_utils.make_logger(base_log, token=token, method_name=method_name, hook=hook)


# class for queue config
class QueueConfig:

    def __init__(self, queue_name):
        self.queueName = queue_name
        self.pandaQueueName = None
        self.prodSourceLabel = 'managed'
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
        self.maxSubmissionAttempts = 3
        self.truePilot = False
        self.queueStatus = None
        self.prefetchEvents = True
        self.uniqueName = None
        self.configID = None

    # get list of status without heartbeat
    def get_no_heartbeat_status(self):
        return self.noHeartbeat.split(',')

    # check if status without heartbeat
    def is_no_heartbeat_status(self, status):
        return status in self.get_no_heartbeat_status()

    # get prodSourceLabel
    def get_source_label(self):
        if self.queueStatus == 'test':
            return 'test'
        return self.prodSourceLabel

    # set unique name
    def set_unique_name(self):
        self.uniqueName = core_utils.get_unique_queue_name(self.queueName, self.resourceType)

    # update attributes
    def update_attributes(self, data):
        for k, v in iteritems(data):
            setattr(self, k, v)

    # get synchronization level between job and worker
    def get_synchronization_level(self):
        if self.mapType == WorkSpec.MT_NoJob or self.truePilot or self.is_no_heartbeat_status('finished'):
            return 1
        return None

    # str
    def __str__(self):
        header = self.queueName + '\n' + '-' * len(self.queueName) + '\n'
        tmpStr = ''
        pluginStr = ''
        keys = list(self.__dict__.keys())
        keys.sort()
        for key in keys:
            val = self.__dict__[key]
            if isinstance(val, dict):
                pluginStr += ' {0} :\n'.format(key)
                pKeys = list(val.keys())
                pKeys.sort()
                for pKey in pKeys:
                    pVal = val[pKey]
                    pluginStr += '  {0} = {1}\n'.format(pKey, pVal)
            else:
                tmpStr += ' {0} = {1}\n'.format(key, val)
        return header + tmpStr + pluginStr


# mapper
class QueueConfigMapper(six.with_metaclass(SingletonWithID, object)):
    # constructor
    def __init__(self, update_db=True):
        self.lock = threading.Lock()
        self.lastUpdate = None
        self.dbProxy = DBProxy()
        self.toUpdateDB = update_db

    # load config from DB cache of URL with validation
    def _load_config_from_cache(self):
        mainLog = _make_logger(method_name='QueueConfigMapper._load_config_from_cache')
        # load config json on URL
        if core_utils.get_queues_config_url() is not None:
            queueConfig_cacheSpec = self.dbProxy.get_cache('queues_config_file')
            if queueConfig_cacheSpec is not None:
                queueConfigJson = queueConfig_cacheSpec.data
                if isinstance(queueConfigJson, dict):
                    return queueConfigJson
                else:
                    mainLog.error('Invalid JSON in cache queues_config_file. Skipped')
            else:
                mainLog.debug('queues config not fount in cache. Skipped')
        else:
            mainLog.debug('queues config URL not set. Skipped')
        return None

    # load config from local json file with syntax validation
    @staticmethod
    def _load_config_from_file():
        mainLog = _make_logger(method_name='QueueConfigMapper._load_config_from_file')
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
        # load from config file
        try:
            with open(confFilePath) as f:
                queueConfigJson = json.load(f)
        except OSError as e:
            mainLog.error('Cannot read file: {0} ; {1}'.format(confFilePath, e))
            return None
        except JSONDecodeError as e:
            mainLog.error('Invalid JSON in file: {0} ; {1}'.format(confFilePath, e))
            return None
        return queueConfigJson

    # get resolver module
    @staticmethod
    def _get_resolver():
        if hasattr(harvester_config.qconf, 'resolverModule') and \
                hasattr(harvester_config.qconf, 'resolverClass'):
            pluginConf = {'module': harvester_config.qconf.resolverModule,
                          'name': harvester_config.qconf.resolverClass}
            pluginFactory = PluginFactory()
            resolver = pluginFactory.get_plugin(pluginConf)
        else:
            resolver = None
        return resolver

    # load data
    def load_data(self):
        mainLog = _make_logger(method_name='QueueConfigMapper.load_data')
        # check interval
        timeNow = datetime.datetime.utcnow()
        if self.lastUpdate is not None and timeNow - self.lastUpdate < datetime.timedelta(minutes=10):
            return
        # start
        with self.lock:
            # init
            newQueueConfig = {}
            queueConfigJsonList = []
            queueNameList = set()
            templateQueueList = set()
            queueTemplateMap = dict()
            resolver = self._get_resolver()
            getQueuesDynamic = False
            invalidQueueList = set()
            # load config json on URL
            queueConfigJson = self._load_config_from_cache()
            if queueConfigJson is not None:
                queueConfigJsonList.append(queueConfigJson)
            # load config from local json file
            queueConfigJson = self._load_config_from_file()
            if queueConfigJson is not None:
                queueConfigJsonList.append(queueConfigJson)
            else:
                mainLog.warning('Failed to load config from local json file. Skipped')
            # get queue names from queue configs
            for queueConfigJson in queueConfigJsonList:
                queueNameList |= set(queueConfigJson.keys())
            # get queue names from resolver
            if resolver is not None and 'DYNAMIC' in harvester_config.qconf.queueList:
                getQueuesDynamic = True
                queueTemplateMap = resolver.get_all_queue_names()
                queueNameList |= set(queueTemplateMap.keys())

            # set attributes
            for queueName in queueNameList:
                for queueConfigJson in queueConfigJsonList:
                    # prepare queueDictList
                    queueDictList = []
                    if queueName in queueConfigJson:
                        queueDict = queueConfigJson[queueName]
                        queueDictList.append(queueDict)
                        # template
                        if 'templateQueueName' in queueDict:
                            templateQueueName = queueDict['templateQueueName']
                            templateQueueList.add(templateQueueName)
                            if templateQueueName in queueConfigJson:
                                queueDictList.insert(0, queueConfigJson[templateQueueName])
                    elif getQueuesDynamic:
                        templateQueueName = queueTemplateMap[queueName]
                        if templateQueueName not in queueConfigJson:
                            templateQueueName = harvester_config.qconf.defaultTemplateQueueName
                        templateQueueList.add(templateQueueName)
                        if templateQueueName in queueConfigJson:
                            queueDictList.insert(0, queueConfigJson[templateQueueName])
                    # fill in queueConfig
                    for queueDict in queueDictList:
                        # prepare queueConfig
                        if queueName in newQueueConfig:
                            queueConfig = newQueueConfig[queueName]
                        else:
                            queueConfig = QueueConfig(queueName)
                        # queueName = siteName/resourceType
                        queueConfig.siteName = queueConfig.queueName.split('/')[0]
                        if queueConfig.siteName != queueConfig.queueName:
                            queueConfig.resourceType = queueConfig.queueName.split('/')[-1]
                        # get common attributes
                        commonAttrDict = dict()
                        if isinstance(queueDict.get('common'), dict):
                            commonAttrDict = queueDict.get('common')
                        # according to queueDict
                        for key, val in iteritems(queueDict):
                            if isinstance(val, dict) and 'module' in val and 'name' in val:
                                val = copy.copy(val)
                                # check module and class name
                                try:
                                    _t3mP_1Mp0R7_mO6U1e__ = importlib.import_module(val['module'])
                                    _t3mP_1Mp0R7_N4m3__ = getattr(_t3mP_1Mp0R7_mO6U1e__, val['name'])
                                except Exception as _e:
                                    invalidQueueList.add(queueConfig.queueName)
                                    mainLog.error('Module or class not found. Omitted {0} in queue config ({1})'.format(
                                                    queueConfig.queueName, _e))
                                    continue
                                else:
                                    del _t3mP_1Mp0R7_mO6U1e__
                                    del _t3mP_1Mp0R7_N4m3__
                                # fill in siteName and queueName
                                if 'siteName' not in val:
                                    val['siteName'] = queueConfig.siteName
                                if 'queueName' not in val:
                                    val['queueName'] = queueConfig.queueName
                                # middleware
                                if 'middleware' in val and val['middleware'] in queueDict:
                                    # keep original config
                                    val['original_config'] = copy.deepcopy(val)
                                    # overwrite with middleware config
                                    for m_key, m_val in iteritems(queueDict[val['middleware']]):
                                        val[m_key] = m_val
                                # fill in common attributes for all plugins
                                for c_key, c_val in iteritems(commonAttrDict):
                                    val[c_key] = c_val
                            setattr(queueConfig, key, val)
                        # get Panda Queue Name
                        if resolver is not None:
                            queueConfig.pandaQueueName = resolver.get_panda_queue_name(queueConfig.siteName)
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
                        # removal of some attributes based on mapType
                        if queueConfig.mapType == WorkSpec.MT_NoJob:
                            for attName in ['nQueueLimitJob', 'nQueueLimitJobRatio']:
                                if hasattr(queueConfig, attName):
                                    delattr(queueConfig, attName)
                        # heartbeat suppression
                        if queueConfig.truePilot and queueConfig.noHeartbeat == '':
                            queueConfig.noHeartbeat = 'running,transferring,finished,failed'
                        # set unique name
                        queueConfig.set_unique_name()
                        # put into new queue configs
                        newQueueConfig[queueName] = queueConfig
            # delete invalid queues
            for invalidQueueName in invalidQueueList:
                if invalidQueueName in newQueueConfig:
                    del newQueueConfig[invalidQueueName]
            # delete templates
            for templateQueueName in templateQueueList:
                if templateQueueName in newQueueConfig:
                    del newQueueConfig[templateQueueName]
            for queueName in newQueueConfig.keys():
                if queueName.endswith('_TEMPLATE'):
                    del newQueueConfig[queueName]
                elif hasattr(newQueueConfig[queueName], 'isTemplateQueue') and \
                        getattr(newQueueConfig[queueName], 'isTemplateQueue') is True:
                    del newQueueConfig[queueName]
            # auto blacklisting
            autoBlacklist = False
            if resolver is not None and hasattr(harvester_config.qconf, 'autoBlacklist') and \
                    harvester_config.qconf.autoBlacklist:
                autoBlacklist = True
            # get queue dumps
            queueConfigDumps = self.dbProxy.get_queue_config_dumps()
            # get active queues
            activeQueues = dict()
            for queueName, queueConfig in iteritems(newQueueConfig):
                # get status
                if queueConfig.queueStatus is None and autoBlacklist:
                    queueConfig.queueStatus = resolver.get_queue_status(queueName)
                # get dynamic information
                if 'DYNAMIC' in harvester_config.qconf.queueList:
                    # UPS queue
                    if resolver.is_ups_queue(queueName):
                        queueConfig.runMode = 'slave'
                        queueConfig.mapType = 'NoJob'
                # set online if undefined
                if queueConfig.queueStatus is None:
                    queueConfig.queueStatus = 'online'
                queueConfig.queueStatus = queueConfig.queueStatus.lower()
                # look for configID
                dumpSpec = QueueConfigDumpSpec()
                dumpSpec.queueName = queueName
                dumpSpec.set_data(vars(queueConfig))
                if dumpSpec.dumpUniqueName in queueConfigDumps:
                    dumpSpec = queueConfigDumps[dumpSpec.dumpUniqueName]
                else:
                    # add dump
                    dumpSpec.creationTime = datetime.datetime.utcnow()
                    dumpSpec.configID = self.dbProxy.get_next_seq_number('SEQ_configID')
                    tmpStat = self.dbProxy.add_queue_config_dump(dumpSpec)
                    if not tmpStat:
                        dumpSpec.configID = self.dbProxy.get_config_id_dump(dumpSpec)
                        if dumpSpec.configID is None:
                            raise Exception('failed to get configID for {0}'.format(dumpSpec.dumpUniqueName))
                    queueConfigDumps[dumpSpec.dumpUniqueName] = dumpSpec
                queueConfig.configID = dumpSpec.configID
                # ignore offline
                if queueConfig.queueStatus == 'offline':
                    continue
                if 'ALL' not in harvester_config.qconf.queueList and \
                        'DYNAMIC' not in harvester_config.qconf.queueList and \
                        queueName not in harvester_config.qconf.queueList:
                    continue
                activeQueues[queueName] = queueConfig
            self.queueConfig = newQueueConfig
            self.activeQueues = activeQueues
            newQueueConfigWithID = dict()
            for dumpSpec in queueConfigDumps.values():
                queueConfig = QueueConfig(dumpSpec.queueName)
                queueConfig.update_attributes(dumpSpec.data)
                queueConfig.configID = dumpSpec.configID
                newQueueConfigWithID[dumpSpec.configID] = queueConfig
            self.queueConfigWithID = newQueueConfigWithID
            self.lastUpdate = datetime.datetime.utcnow()
        # update database
        if self.toUpdateDB:
            self.dbProxy.fill_panda_queue_table(self.activeQueues.keys(), self)

    # check if valid queue
    def has_queue(self, queue_name, config_id=None):
        self.load_data()
        if config_id is not None:
            return config_id in self.queueConfigWithID
        return queue_name in self.queueConfig

    # get queue config
    def get_queue(self, queue_name, config_id=None):
        self.load_data()
        if config_id is not None and config_id in self.queueConfigWithID:
            return self.queueConfigWithID[config_id]
        if queue_name in self.queueConfig:
            return self.queueConfig[queue_name]
        return None

    # all queue configs
    def get_all_queues(self):
        self.load_data()
        return self.queueConfig

    # all active queue config
    def get_active_queues(self):
        self.load_data()
        return self.activeQueues

    def get_active_ups_queues(self):
        """
        Get active UPS candidates
        :return:
        """
        active_ups_queues = []
        active_queues = self.get_active_queues()
        for queue_name, queue_attribs in iteritems(active_queues):
            try:
                if queue_attribs.runMode == 'slave' and queue_attribs.mapType == 'NoJob':
                    active_ups_queues.append(queue_name)
            except KeyError:
                continue
        return active_ups_queues

    # all queues with config IDs
    def get_all_queues_with_config_ids(self):
        self.load_data()
        return self.queueConfigWithID
