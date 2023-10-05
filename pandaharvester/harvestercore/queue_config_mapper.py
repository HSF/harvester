import os
import json
import copy
import time
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
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from .work_spec import WorkSpec
from .panda_queue_spec import PandaQueueSpec
from . import core_utils
from .core_utils import SingletonWithID
from .db_proxy_pool import DBProxyPool as DBProxy
from .plugin_factory import PluginFactory
from .queue_config_dump_spec import QueueConfigDumpSpec
from .db_interface import DBInterface


# logger
_logger = core_utils.setup_logger("queue_config_mapper")
_dbInterface = DBInterface()


# make logger
def _make_logger(base_log=_logger, token=None, method_name=None, send_dialog=True):
    if send_dialog and _dbInterface:
        hook = _dbInterface
    else:
        hook = None
    return core_utils.make_logger(base_log, token=token, method_name=method_name, hook=hook)


# class for queue config
class QueueConfig(object):
    def __init__(self, queue_name):
        self.queueName = queue_name
        self.pandaQueueName = None
        self.prodSourceLabel = "managed"
        # default parameters
        self.mapType = WorkSpec.MT_OneToOne
        self.useJobLateBinding = False
        self.zipPerMB = None
        self.siteName = ""
        self.maxWorkers = 0
        self.nNewWorkers = 0
        self.maxNewWorkersPerCycle = 0
        self.noHeartbeat = ""
        self.runMode = "self"
        self.resourceType = PandaQueueSpec.RT_catchall
        self.jobType = PandaQueueSpec.JT_catchall
        self.getJobCriteria = None
        self.ddmEndpointIn = None
        self.allowJobMixture = False
        self.maxSubmissionAttempts = 3
        self.truePilot = False
        self.queueStatus = None
        self.prefetchEvents = True
        self.uniqueName = None
        self.configID = None
        self.initEventsMultipler = 2

    # get list of status without heartbeat
    def get_no_heartbeat_status(self):
        return self.noHeartbeat.split(",")

    # check if status without heartbeat
    def is_no_heartbeat_status(self, status):
        return status in self.get_no_heartbeat_status()

    # get prodSourceLabel
    def get_source_label(self, job_type=None, is_gu=None):
        # if queue is in test status, only submit workers for HC jobs
        # No longer need this as panda only dispatches test jobs when PQ in test
        # if self.queueStatus == 'test':
        #     return 'test'

        # grandly unified queues: prodsourcelabel in job has precedence over queue prodsourcelabel
        if job_type in ("user", "panda"):
            return "user"

        # grandly unified queues: call to getJobs should not request for a particular prodSourceLabel
        if is_gu:
            return "unified"

        return self.prodSourceLabel

    # set unique name
    def set_unique_name(self):
        self.uniqueName = core_utils.get_unique_queue_name(self.queueName, self.resourceType, self.prodSourceLabel)

    # update attributes
    def update_attributes(self, data):
        for k, v in iteritems(data):
            setattr(self, k, v)

    # get synchronization level between job and worker
    def get_synchronization_level(self):
        if self.mapType == WorkSpec.MT_NoJob or self.truePilot or self.is_no_heartbeat_status("finished"):
            return 1
        return None

    # str
    def __str__(self):
        header = self.queueName + "\n" + "-" * len(self.queueName) + "\n"
        tmpStr = ""
        pluginStr = ""
        keys = sorted(self.__dict__.keys())
        for key in keys:
            val = self.__dict__[key]
            if isinstance(val, dict):
                pluginStr += " {0} :\n".format(key)
                pKeys = sorted(val.keys())
                for pKey in pKeys:
                    pVal = val[pKey]
                    pluginStr += "  {0} = {1}\n".format(pKey, pVal)
            else:
                tmpStr += " {0} = {1}\n".format(key, val)
        return header + tmpStr + pluginStr


# mapper
class QueueConfigMapper(six.with_metaclass(SingletonWithID, object)):
    """
    Using some acronyms here:
    LT = local template, written in local queueconfig file
    RT = remote template, on http source fetched by cacher
    FT = final template of a queue
    LQ = local queue configuration, written in local queueconfig file
    RQ = remote queue configuration, on http source fetched by cacher, static
    DQ = dynamic queue configuration, configured with information from resolver

    Relations:
    FT = LT if existing else RT
    FQ = FT updated with RQ, then updated with DQ, then updated with LQ
    """

    mandatory_attrs = set(
        [
            "messenger",
            "monitor",
            "preparator",
            "stager",
            "submitter",
            "sweeper",
            "workerMaker",
        ]
    )
    dynamic_queue_generic_attrs = set(
        [
            "nQueueLimitWorker",
            "maxWorkers",
            "maxNewWorkersPerCycle",
            "nQueueLimitJob",
            "nQueueLimitJobRatio",
            "nQueueLimitJobMax",
            "nQueueLimitJobMin",
            "nQueueLimitWorkerRatio",
            "nQueueLimitWorkerMax",
            "nQueueLimitWorkerMin",
        ]
    )
    updatable_plugin_attrs = set(
        ["common", "messenger", "monitor", "preparator", "stager", "submitter", "sweeper", "workerMaker", "throttler", "zipper", "aux_preparator", "extractor"]
    )

    # constructor
    def __init__(self, update_db=True):
        self.lock = threading.Lock()
        self.lastUpdate = None
        self.lastReload = None
        self.lastCheck = None
        self.dbProxy = DBProxy()
        self.toUpdateDB = update_db
        try:
            self.configFromCacher = harvester_config.qconf.configFromCacher
        except AttributeError:
            self.configFromCacher = False
        try:
            self.updateInterval = harvester_config.qconf.updateInterval
        except AttributeError:
            self.updateInterval = 600
        try:
            self.checkInterval = harvester_config.qconf.checkInterval
        except AttributeError:
            self.checkInterval = 5
        finally:
            self.checkInterval = min(self.checkInterval, self.updateInterval)

    # load config from DB cache of URL with validation
    def _load_config_from_cache(self):
        mainLog = _make_logger(method_name="QueueConfigMapper._load_config_from_cache")
        # load config json on URL
        if self.configFromCacher:
            queueConfig_cacheSpec = self.dbProxy.get_cache("queues_config_file")
            if queueConfig_cacheSpec is not None:
                queueConfigJson = queueConfig_cacheSpec.data
                if isinstance(queueConfigJson, dict):
                    return queueConfigJson
                else:
                    mainLog.error("Invalid JSON in cache queues_config_file. Skipped")
            else:
                mainLog.debug("queues config not fount in cache. Skipped")
        else:
            mainLog.debug("queues config URL not set. Skipped")
        return None

    # load config from local json file with syntax validation
    @staticmethod
    def _load_config_from_file():
        mainLog = _make_logger(method_name="QueueConfigMapper._load_config_from_file")
        # define config file path
        if os.path.isabs(harvester_config.qconf.configFile):
            confFilePath = harvester_config.qconf.configFile
        else:
            # check if in PANDA_HOME
            confFilePath = None
            if "PANDA_HOME" in os.environ:
                confFilePath = os.path.join(os.environ["PANDA_HOME"], "etc/panda", harvester_config.qconf.configFile)
                if not os.path.exists(confFilePath):
                    confFilePath = None
            # look into /etc/panda
            if confFilePath is None:
                confFilePath = os.path.join("/etc/panda", harvester_config.qconf.configFile)
        # load from config file
        try:
            with open(confFilePath) as f:
                queueConfigJson = json.load(f)
        except OSError as e:
            mainLog.error("Cannot read file: {0} ; {1}".format(confFilePath, e))
            return None
        except JSONDecodeError as e:
            mainLog.error("Invalid JSON in file: {0} ; {1}".format(confFilePath, e))
            return None
        return queueConfigJson

    # get resolver module
    @staticmethod
    def _get_resolver():
        if hasattr(harvester_config.qconf, "resolverModule") and hasattr(harvester_config.qconf, "resolverClass"):
            pluginConf = {"module": harvester_config.qconf.resolverModule, "name": harvester_config.qconf.resolverClass}
            pluginFactory = PluginFactory()
            resolver = pluginFactory.get_plugin(pluginConf)
        else:
            resolver = None
        return resolver

    # update last reload time
    def _update_last_reload_time(self, ts=None):
        if ts is None:
            ts = time.time()
        new_info = "{0:.3f}".format(ts)
        return self.dbProxy.refresh_cache("_qconf_last_reload", "_universal", new_info)

    # get last reload time
    def _get_last_reload_time(self):
        cacheSpec = self.dbProxy.get_cache("_qconf_last_reload", "_universal", from_local_cache=False)
        if cacheSpec is None:
            return None
        timestamp = float(cacheSpec.data)
        return timestamp

    # load data
    def load_data(self, refill_table=False):
        mainLog = _make_logger(method_name="QueueConfigMapper.load_data")
        # check if to update
        with self.lock:
            time_now = datetime.datetime.utcnow()
            updateInterval_td = datetime.timedelta(seconds=self.updateInterval)
            checkInterval_td = datetime.timedelta(seconds=self.checkInterval)
            # skip if lastCheck is fresh (within checkInterval)
            if self.lastCheck is not None and time_now - self.lastCheck < checkInterval_td:
                return
            self.lastCheck = time_now
            # get last_reload_timestamp from DB
            last_reload_timestamp = self._get_last_reload_time()
            self.lastReload = None if last_reload_timestamp is None else datetime.datetime.utcfromtimestamp(last_reload_timestamp)
            # skip if lastReload is fresh and lastUpdate fresher than lastReload (within updateInterval)
            if (
                self.lastReload is not None
                and self.lastUpdate is not None
                and self.lastReload < self.lastUpdate
                and time_now - self.lastReload < updateInterval_td
            ):
                return
        # start
        with self.lock:
            # update timesatmp of last reload, lock with check interval
            got_timesatmp_update_lock = self.dbProxy.get_process_lock("qconf_reload", "qconf_universal", self.updateInterval)
            if got_timesatmp_update_lock:
                now_ts = time.time()
                retVal = self._update_last_reload_time(now_ts)
                self.lastReload = datetime.datetime.utcfromtimestamp(now_ts)
                if retVal:
                    mainLog.debug("updated last reload timestamp")
                else:
                    mainLog.warning("failed to update last reload timestamp. Skipped")
            else:
                mainLog.debug("did not get qconf_reload timestamp lock. Skipped to update last reload timestamp")
            # init
            newQueueConfig = dict()
            localTemplatesDict = dict()
            remoteTemplatesDict = dict()
            finalTemplatesDict = dict()
            localQueuesDict = dict()
            remoteQueuesDict = dict()
            dynamicQueuesDict = dict()
            allQueuesNameList = set()
            getQueuesDynamic = False
            invalidQueueList = set()
            pandaQueueDict = PandaQueuesDict()
            # get resolver
            resolver = self._get_resolver()
            if resolver is None:
                mainLog.debug("No resolver is configured")
            # load config json from cacher (RT & RQ)
            queueConfigJson_cacher = self._load_config_from_cache()
            if queueConfigJson_cacher is not None:
                for queueName, queueDict in iteritems(queueConfigJson_cacher):
                    if queueDict.get("isTemplateQueue") is True or queueName.endswith("_TEMPLATE"):
                        # is RT
                        queueDict["isTemplateQueue"] = True
                        queueDict.pop("templateQueueName", None)
                        remoteTemplatesDict[queueName] = queueDict
                    else:
                        # is RQ
                        queueDict["isTemplateQueue"] = False
                        remoteQueuesDict[queueName] = queueDict
            # load config from local json file (LT & LQ)
            queueConfigJson_local = self._load_config_from_file()
            if queueConfigJson_local is not None:
                for queueName, queueDict in iteritems(queueConfigJson_local):
                    if queueDict.get("isTemplateQueue") is True or queueName.endswith("_TEMPLATE"):
                        # is LT
                        queueDict["isTemplateQueue"] = True
                        queueDict.pop("templateQueueName", None)
                        localTemplatesDict[queueName] = queueDict
                    else:
                        # is LQ
                        queueDict["isTemplateQueue"] = False
                        localQueuesDict[queueName] = queueDict
            else:
                mainLog.warning("Failed to load config from local json file. Skipped")
            # fill in final template (FT)
            finalTemplatesDict.update(remoteTemplatesDict)
            finalTemplatesDict.update(localTemplatesDict)
            finalTemplatesDict.pop(None, None)
            # remove queues with invalid templateQueueName
            for acr, queuesDict in [("RQ", remoteQueuesDict), ("LQ", localQueuesDict)]:
                for queueName, queueDict in iteritems(queuesDict.copy()):
                    templateQueueName = queueDict.get("templateQueueName")
                    if templateQueueName is not None and templateQueueName not in finalTemplatesDict:
                        del queuesDict[queueName]
                        mainLog.warning('Invalid templateQueueName "{0}" for {1} ({2}). Skipped'.format(templateQueueName, queueName, acr))
            # get queue names from resolver and fill in dynamic queue (DQ)
            if resolver is not None and "DYNAMIC" in harvester_config.qconf.queueList:
                getQueuesDynamic = True
                dynamicQueuesNameList = resolver.get_all_queue_names()
                for queueName in dynamicQueuesNameList.copy():
                    queueDict = dict()
                    # template and default template via workflow
                    templateQueueName = None
                    resolver_harvester_template = None
                    if resolver is not None:
                        resolver_harvester_template = resolver.get_harvester_template(queueName)
                        resolver_type, resolver_workflow = resolver.get_type_workflow(queueName)
                    if resolver_harvester_template:
                        templateQueueName = resolver_harvester_template
                    elif not (resolver_type is None or resolver_workflow is None):
                        templateQueueName = "{pq_type}.{workflow}".format(pq_type=resolver_type, workflow=resolver_workflow)
                    else:
                        templateQueueName = harvester_config.qconf.defaultTemplateQueueName
                    if templateQueueName not in finalTemplatesDict:
                        # remove queues with invalid templateQueueName
                        dynamicQueuesNameList.discard(queueName)
                        mainLog.warning('Invalid templateQueueName "{0}" for {1} (DQ). Skipped'.format(templateQueueName, queueName))
                        continue
                    # parameters
                    resolver_harvester_params = resolver.get_harvester_params(queueName)
                    for key, val in iteritems(resolver_harvester_params):
                        if key in self.dynamic_queue_generic_attrs:
                            queueDict[key] = val
                    # fill in dynamic queue configs
                    queueDict["templateQueueName"] = templateQueueName
                    queueDict["isTemplateQueue"] = False
                    dynamicQueuesDict[queueName] = queueDict
            # fill in all queue name list (names of RQ + DQ + LQ)
            allQueuesNameList |= set(remoteQueuesDict)
            allQueuesNameList |= set(dynamicQueuesDict)
            allQueuesNameList |= set(localQueuesDict)
            allQueuesNameList.discard(None)
            # set attributes
            for queueName in allQueuesNameList:
                # sources or queues and templates
                queueSourceList = []
                templateSourceList = []
                # prepare templateQueueName
                templateQueueName = None
                for queuesDict in [remoteQueuesDict, dynamicQueuesDict, localQueuesDict]:
                    if queueName not in queuesDict:
                        continue
                    tmp_queueDict = queuesDict[queueName]
                    tmp_templateQueueName = tmp_queueDict.get("templateQueueName")
                    if tmp_templateQueueName is not None:
                        templateQueueName = tmp_templateQueueName
                # prepare queueDict
                queueDict = dict()
                if templateQueueName in finalTemplatesDict:
                    queueDict.update(copy.deepcopy(finalTemplatesDict[templateQueueName]))
                for acr, templatesDict in [("RT", remoteTemplatesDict), ("LT", localTemplatesDict)]:
                    if templateQueueName in templatesDict:
                        templateSourceList.append(acr)
                # update queueDict
                for acr, queuesDict in [("RQ", remoteQueuesDict), ("DQ", dynamicQueuesDict), ("LQ", localQueuesDict)]:
                    if queueName not in queuesDict:
                        continue
                    queueSourceList.append(acr)
                    tmp_queueDict = queuesDict[queueName]
                    for key, val in iteritems(tmp_queueDict):
                        val = copy.deepcopy(val)
                        if key in self.updatable_plugin_attrs and isinstance(queueDict.get(key), dict) and isinstance(val, dict):
                            # update plugin parameters instead of overwriting whole plugin section
                            queueDict[key].update(val)
                        else:
                            queueDict[key] = val
                # record sources of the queue config and its templates in log
                if templateQueueName:
                    mainLog.debug(
                        ("queue {queueName} comes from {queueSource} " "(with template {templateName} " "from {templateSource})").format(
                            queueName=queueName,
                            templateName=templateQueueName,
                            queueSource=",".join(queueSourceList),
                            templateSource=",".join(templateSourceList),
                        )
                    )
                else:
                    mainLog.debug("queue {queueName} comes from {queueSource}".format(queueName=queueName, queueSource=",".join(queueSourceList)))
                # prepare queueConfig
                if queueName in newQueueConfig:
                    queueConfig = newQueueConfig[queueName]
                else:
                    queueConfig = QueueConfig(queueName)
                # queueName = siteName/resourceType
                queueConfig.siteName = queueConfig.queueName.split("/")[0]
                if queueConfig.siteName != queueConfig.queueName:
                    queueConfig.resourceType = queueConfig.queueName.split("/")[-1]
                # get common attributes
                commonAttrDict = dict()
                if isinstance(queueDict.get("common"), dict):
                    commonAttrDict = queueDict.get("common")
                # according to queueDict
                for key, val in iteritems(queueDict):
                    if isinstance(val, dict) and "module" in val and "name" in val:
                        # plugin attributes
                        val = copy.deepcopy(val)
                        # fill in common attributes for all plugins
                        for c_key, c_val in iteritems(commonAttrDict):
                            if c_key not in val and c_key not in ("module", "name"):
                                val[c_key] = c_val
                        # check module and class name
                        try:
                            _t3mP_1Mp0R7_mO6U1e__ = importlib.import_module(val["module"])
                            _t3mP_1Mp0R7_N4m3__ = getattr(_t3mP_1Mp0R7_mO6U1e__, val["name"])
                        except Exception as _e:
                            invalidQueueList.add(queueConfig.queueName)
                            mainLog.error("Module or class not found. Omitted {0} in queue config ({1})".format(queueConfig.queueName, _e))
                            continue
                        else:
                            del _t3mP_1Mp0R7_mO6U1e__
                            del _t3mP_1Mp0R7_N4m3__
                        # fill in siteName and queueName
                        if "siteName" not in val:
                            val["siteName"] = queueConfig.siteName
                        if "queueName" not in val:
                            val["queueName"] = queueConfig.queueName
                        # middleware
                        if "middleware" in val and val["middleware"] in queueDict:
                            # keep original config
                            val["original_config"] = copy.deepcopy(val)
                            # overwrite with middleware config
                            for m_key, m_val in iteritems(queueDict[val["middleware"]]):
                                val[m_key] = m_val
                    setattr(queueConfig, key, val)
                # delete isTemplateQueue attribute
                try:
                    if getattr(queueConfig, "isTemplateQueue"):
                        mainLog.error("Internal error: isTemplateQueue is True. Omitted {0} in queue config".format(queueConfig.queueName))
                        invalidQueueList.add(queueConfig.queueName)
                    else:
                        delattr(queueConfig, "isTemplateQueue")
                except AttributeError as _e:
                    mainLog.error('Internal error with attr "isTemplateQueue". Omitted {0} in queue config ({1})'.format(queueConfig.queueName, _e))
                    invalidQueueList.add(queueConfig.queueName)
                # get Panda Queue Name
                if resolver is not None:
                    queueConfig.pandaQueueName = resolver.get_panda_queue_name(queueConfig.siteName)
                # additional criteria for getJob
                if queueConfig.getJobCriteria is not None:
                    tmpCriteria = dict()
                    for tmpItem in queueConfig.getJobCriteria.split(","):
                        tmpKey, tmpVal = tmpItem.split("=")
                        tmpCriteria[tmpKey] = tmpVal
                    if len(tmpCriteria) == 0:
                        queueConfig.getJobCriteria = None
                    else:
                        queueConfig.getJobCriteria = tmpCriteria
                # nullify job attributes if NoJob mapType
                if queueConfig.mapType == WorkSpec.MT_NoJob:
                    for attName in ["nQueueLimitJob", "nQueueLimitJobRatio", "nQueueLimitJobMax", "nQueueLimitJobMin"]:
                        setattr(queueConfig, attName, None)
                # heartbeat suppression
                if queueConfig.truePilot and queueConfig.noHeartbeat == "":
                    queueConfig.noHeartbeat = "running,transferring,finished,failed"
                # set unique name
                queueConfig.set_unique_name()
                # put into new queue configs
                newQueueConfig[queueName] = queueConfig
                # Check existence of mandatory attributes
                if queueName in newQueueConfig:
                    queueConfig = newQueueConfig[queueName]
                    missing_attr_list = []
                    for _attr in self.mandatory_attrs:
                        if not hasattr(queueConfig, _attr):
                            invalidQueueList.add(queueConfig.queueName)
                            missing_attr_list.append(_attr)
                    if missing_attr_list:
                        mainLog.error(
                            "Missing mandatory attributes {0} . Omitted {1} in queue config".format(",".join(missing_attr_list), queueConfig.queueName)
                        )
            # delete invalid queues
            for invalidQueueName in invalidQueueList:
                if invalidQueueName in newQueueConfig:
                    del newQueueConfig[invalidQueueName]
            # auto blacklisting
            autoBlacklist = False
            if resolver is not None and hasattr(harvester_config.qconf, "autoBlacklist") and harvester_config.qconf.autoBlacklist:
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
                if "DYNAMIC" in harvester_config.qconf.queueList:
                    # UPS queue
                    if resolver is not None and resolver.is_ups_queue(queueName):
                        queueConfig.runMode = "slave"
                        queueConfig.mapType = "NoJob"
                # set online if undefined
                if queueConfig.queueStatus is None:
                    queueConfig.queueStatus = "online"
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
                    dumpSpec.configID = self.dbProxy.get_next_seq_number("SEQ_configID")
                    tmpStat = self.dbProxy.add_queue_config_dump(dumpSpec)
                    if not tmpStat:
                        dumpSpec.configID = self.dbProxy.get_config_id_dump(dumpSpec)
                        if dumpSpec.configID is None:
                            mainLog.error("failed to get configID for {0}".format(dumpSpec.dumpUniqueName))
                            continue
                    queueConfigDumps[dumpSpec.dumpUniqueName] = dumpSpec
                queueConfig.configID = dumpSpec.configID
                # ignore offline
                if queueConfig.queueStatus == "offline":
                    continue
                # filter for pilot version
                if (
                    hasattr(harvester_config.qconf, "pilotVersion")
                    and pandaQueueDict.get(queueConfig.siteName) is not None
                    and pandaQueueDict.get(queueConfig.siteName).get("pilot_version") != str(harvester_config.qconf.pilotVersion)
                ):
                    continue
                if (
                    "ALL" not in harvester_config.qconf.queueList
                    and "DYNAMIC" not in harvester_config.qconf.queueList
                    and queueName not in harvester_config.qconf.queueList
                ):
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
            # update lastUpdate and lastCheck
            self.lastUpdate = datetime.datetime.utcnow()
            self.lastCheck = self.lastUpdate
        # update database
        if self.toUpdateDB:
            self.dbProxy.fill_panda_queue_table(self.activeQueues.keys(), self, refill_table=refill_table)
            mainLog.debug("updated to DB")
        # done
        mainLog.debug("done")

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
                if queue_attribs.runMode == "slave" and queue_attribs.mapType == "NoJob":
                    active_ups_queues.append(queue_name)
            except KeyError:
                continue
        return active_ups_queues

    # all queues with config IDs
    def get_all_queues_with_config_ids(self):
        self.load_data()
        return self.queueConfigWithID
