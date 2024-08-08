import copy
import datetime
import importlib
import json
import os
import threading
from json.decoder import JSONDecodeError

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict

from . import core_utils
from .core_utils import SingletonWithID
from .db_interface import DBInterface
from .db_proxy_pool import DBProxyPool as DBProxy
from .panda_queue_spec import PandaQueueSpec
from .plugin_factory import PluginFactory
from .queue_config_dump_spec import QueueConfigDumpSpec
from .work_spec import WorkSpec

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
        self.siteFamily = ""
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
        for k, v in data.items():
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
                pluginStr += f" {key} :\n"
                pKeys = sorted(val.keys())
                for pKey in pKeys:
                    pVal = val[pKey]
                    pluginStr += f"  {pKey} = {pVal}\n"
            else:
                tmpStr += f" {key} = {val}\n"
        return header + tmpStr + pluginStr


# mapper
class QueueConfigMapper(metaclass=SingletonWithID):
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
    queue_limit_attrs = set(
        [
            "maxWorkers",
            "maxNewWorkersPerCycle",
            "nQueueLimitWorker",
            "nQueueLimitWorkerMax",
            "nQueueLimitWorkerRatio",
            "nQueueLimitWorkerMin",
            "nQueueLimitWorkerCores",
            "nQueueLimitWorkerCoresRatio",
            "nQueueLimitWorkerCoresMin",
            "nQueueLimitWorkerMemory",
            "nQueueLimitWorkerMemoryRatio",
            "nQueueLimitWorkerMemoryMin",
            "nQueueLimitJob",
            "nQueueLimitJobMax",
            "nQueueLimitJobRatio",
            "nQueueLimitJobMin",
            "nQueueLimitJobCores",
            "nQueueLimitJobCoresRatio",
            "nQueueLimitJobCoresMin",
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
        self.last_cache_ts = None
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
        mainLog = _make_logger(token=f"id={core_utils.get_pid()}", method_name="_load_config_from_cache")
        # load config json on URL
        if self.configFromCacher:
            queueConfig_cacheSpec = self.dbProxy.get_cache("queues_config_file", from_local_cache=False)
            if queueConfig_cacheSpec is not None:
                queueConfigJson = queueConfig_cacheSpec.data
                if isinstance(queueConfigJson, dict):
                    return queueConfigJson, queueConfig_cacheSpec.lastUpdate
                else:
                    mainLog.error("Invalid JSON in cache queues_config_file. Skipped")
            else:
                mainLog.debug("queues config not fount in cache. Skipped")
        else:
            mainLog.debug("queues config URL not set. Skipped")
        return None, None

    # load config from local json file with syntax validation
    @staticmethod
    def _load_config_from_file():
        mainLog = _make_logger(token=f"id={core_utils.get_pid()}", method_name="_load_config_from_file")
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
            mainLog.error(f"Cannot read file: {confFilePath} ; {e}")
            return None
        except JSONDecodeError as e:
            mainLog.error(f"Invalid JSON in file: {confFilePath} ; {e}")
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
    def _update_last_reload_time(self, the_time=None):
        # update timestamp of last reload, lock with check interval
        got_update_lock = self.dbProxy.get_process_lock("qconf_reload", "qconf_universal", self.updateInterval)
        if got_update_lock:
            if the_time is None:
                the_time = core_utils.naive_utcnow()
            ts = the_time.timestamp()
            new_ts_info = f"{ts:.3f}"
            ret_val = self.dbProxy.refresh_cache("_qconf_last_reload", "_universal", new_ts_info)
            if ret_val:
                return True
            else:
                return False
        else:
            return None

    # get last reload time
    def _get_last_reload_time(self):
        cacheSpec = self.dbProxy.get_cache("_qconf_last_reload", "_universal", from_local_cache=False)
        if cacheSpec is None:
            return None
        timestamp = float(cacheSpec.data)
        return core_utils.naive_utcfromtimestamp(timestamp)

    # update last pq_table fill time
    def _update_pq_table(self, cache_time=None, refill_table=False):
        # update timestamp of last reload, lock with check interval
        got_update_lock = self.dbProxy.get_process_lock("pq_table_fill", "qconf_universal", 120)
        if got_update_lock:
            fill_ret_val = self.dbProxy.fill_panda_queue_table(self.activeQueues.keys(), self, refill_table=refill_table)
            now_time = core_utils.naive_utcnow()
            if fill_ret_val:
                now_ts = now_time.timestamp()
                now_ts_info = f"{now_ts:.3f}"
                self.dbProxy.refresh_cache("_pq_table_last_fill", "_universal", now_ts_info)
                if cache_time:
                    cache_ts = cache_time.timestamp()
                    cache_ts_info = f"{cache_ts:.3f}"
                    self.dbProxy.refresh_cache("_cache_to_fill_pq_table", "_universal", cache_ts_info)
            self.dbProxy.release_process_lock("pq_table_fill", "qconf_universal")
            if fill_ret_val:
                return True
            else:
                return False
        else:
            return None

    # get last pq_table fill time
    def _get_last_pq_table_fill_time(self):
        cacheSpec = self.dbProxy.get_cache("_pq_table_last_fill", "_universal", from_local_cache=False)
        if cacheSpec is None:
            return None
        timestamp = float(cacheSpec.data)
        return core_utils.naive_utcfromtimestamp(timestamp)

    # get time of last cache used to fill pq_table
    def _get_cache_to_fill_pq_table_time(self):
        cacheSpec = self.dbProxy.get_cache("_cache_to_fill_pq_table", "_universal", from_local_cache=False)
        if cacheSpec is None:
            return None
        timestamp = float(cacheSpec.data)
        return core_utils.naive_utcfromtimestamp(timestamp)

    # load data
    def load_data(self, refill_table=False):
        mainLog = _make_logger(token=f"id={core_utils.get_pid()}", method_name="load_data")
        # check if to update
        with self.lock:
            now_time = core_utils.naive_utcnow()
            updateInterval_td = datetime.timedelta(seconds=self.updateInterval)
            checkInterval_td = datetime.timedelta(seconds=self.checkInterval)
            # skip if lastCheck is fresh (within checkInterval)
            if self.lastCheck is not None and now_time - self.lastCheck < checkInterval_td:
                return
            self.lastCheck = now_time
            # get last_reload_timestamp from DB
            self.lastReload = self._get_last_reload_time()
            # get last_pq_table_fill_time from DB
            self.last_pq_table_fill_time = self._get_last_pq_table_fill_time()
            # get time of last cache used to fill pq_table from DB
            self.cache_to_fill_pq_table_time = self._get_cache_to_fill_pq_table_time()
            # skip if min_last_reload is fresh and lastUpdate fresher than lastReload (within updateInterval)
            if self.lastReload and self.lastUpdate:
                min_last_reload = self.lastReload
                if self.last_cache_ts:
                    min_last_reload = min(min_last_reload, self.last_cache_ts + updateInterval_td * 0.25)
                if self.lastReload < self.lastUpdate and now_time - min_last_reload < updateInterval_td:
                    return
        # start
        with self.lock:
            # update timestamp of last reload, lock with check interval
            now_time = core_utils.naive_utcnow()
            update_last_reload_ret_val = self._update_last_reload_time(now_time)
            if update_last_reload_ret_val:
                self.lastReload = now_time
                mainLog.debug("updated last reload timestamp")
            elif update_last_reload_ret_val is None:
                mainLog.debug("did not get qconf_reload timestamp lock. Skipped to update last reload timestamp")
            else:
                mainLog.warning("failed to update last reload timestamp. Skipped")
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
            queueConfigJson_cacher, self.last_cache_ts = self._load_config_from_cache()
            if self.last_cache_ts and self.cache_to_fill_pq_table_time and (self.last_cache_ts < self.cache_to_fill_pq_table_time):
                # cacher data outdated compared with pq_table fill time; warn
                mainLog.warning(f"Found cacher data outdated ({str(self.last_cache_ts)} < {str(self.cache_to_fill_pq_table_time)})")
            if queueConfigJson_cacher is not None:
                mainLog.debug("Applying cacher data")
                for queueName, queueDict in queueConfigJson_cacher.items():
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
                mainLog.debug("Applying local config")
                for queueName, queueDict in queueConfigJson_local.items():
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
                for queueName, queueDict in queuesDict.copy().items():
                    templateQueueName = queueDict.get("templateQueueName")
                    if templateQueueName is not None and templateQueueName not in finalTemplatesDict:
                        del queuesDict[queueName]
                        mainLog.warning(f'Invalid templateQueueName "{templateQueueName}" for {queueName} ({acr}). Skipped')
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
                        templateQueueName = f"{resolver_type}.{resolver_workflow}"
                    else:
                        templateQueueName = harvester_config.qconf.defaultTemplateQueueName
                    if templateQueueName not in finalTemplatesDict:
                        # remove queues with invalid templateQueueName
                        dynamicQueuesNameList.discard(queueName)
                        mainLog.warning(f'Invalid templateQueueName "{templateQueueName}" for {queueName} (DQ). Skipped')
                        continue
                    # parameters
                    resolver_harvester_params = resolver.get_harvester_params(queueName)
                    for key, val in resolver_harvester_params.items():
                        if key in self.queue_limit_attrs:
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
                    for key, val in tmp_queueDict.items():
                        val = copy.deepcopy(val)
                        if key in self.updatable_plugin_attrs and isinstance(queueDict.get(key), dict) and isinstance(val, dict):
                            # update plugin parameters instead of overwriting whole plugin section
                            queueDict[key].update(val)
                        else:
                            queueDict[key] = val
                # record sources of the queue config and its templates in log
                if templateQueueName:
                    mainLog.debug(
                        f"queue {queueName} comes from {','.join(queueSourceList)} (with template {templateQueueName} from {','.join(templateSourceList)})"
                    )
                else:
                    mainLog.debug(f"queue {queueName} comes from {','.join(queueSourceList)}")
                # prepare queueConfig
                if queueName in newQueueConfig:
                    queueConfig = newQueueConfig[queueName]
                else:
                    queueConfig = QueueConfig(queueName)
                # queueName = siteName/resourceType
                queueConfig.siteName = queueConfig.queueName.split("/")[0]
                if queueConfig.siteName != queueConfig.queueName:
                    queueConfig.resourceType = queueConfig.queueName.split("/")[-1]
                if not queueConfig.siteFamily:
                    queueConfig.siteFamily = queueConfig.siteName
                # get common attributes
                commonAttrDict = dict()
                if isinstance(queueDict.get("common"), dict):
                    commonAttrDict = queueDict.get("common")
                # according to queueDict
                for key, val in queueDict.items():
                    if isinstance(val, dict) and "module" in val and "name" in val:
                        # plugin attributes
                        val = copy.deepcopy(val)
                        # fill in common attributes for all plugins
                        for c_key, c_val in commonAttrDict.items():
                            if c_key not in val and c_key not in ("module", "name"):
                                val[c_key] = c_val
                        # check module and class name
                        try:
                            _t3mP_1Mp0R7_mO6U1e__ = importlib.import_module(val["module"])
                            _t3mP_1Mp0R7_N4m3__ = getattr(_t3mP_1Mp0R7_mO6U1e__, val["name"])
                        except Exception as _e:
                            invalidQueueList.add(queueConfig.queueName)
                            mainLog.error(f"Module or class not found. Omitted {queueConfig.queueName} in queue config ({_e})")
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
                            for m_key, m_val in queueDict[val["middleware"]].items():
                                val[m_key] = m_val
                    setattr(queueConfig, key, val)
                # delete isTemplateQueue attribute
                try:
                    if getattr(queueConfig, "isTemplateQueue"):
                        mainLog.error(f"Internal error: isTemplateQueue is True. Omitted {queueConfig.queueName} in queue config")
                        invalidQueueList.add(queueConfig.queueName)
                    else:
                        delattr(queueConfig, "isTemplateQueue")
                except AttributeError as _e:
                    mainLog.error(f'Internal error with attr "isTemplateQueue". Omitted {queueConfig.queueName} in queue config ({_e})')
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
                # nullify all job limit attributes if NoJob mapType (PULL)
                if queueConfig.mapType == WorkSpec.MT_NoJob:
                    for attName in ["nQueueLimitJob", "nQueueLimitJobRatio", "nQueueLimitJobMax", "nQueueLimitJobMin"]:
                        setattr(queueConfig, attName, None)
                # nullify worker ratio limit attributes if jobful mapTypes (PUSH)
                if queueConfig.mapType != WorkSpec.MT_NoJob:
                    for attName in ["nQueueLimitWorkerRatio", "nQueueLimitWorkerMin"]:
                        setattr(queueConfig, attName, None)
                # sanity for worker and job limit attributes
                for key in self.queue_limit_attrs:
                    val = getattr(queueConfig, key, None)
                    if val is not None:
                        if not isinstance(val, int):
                            setattr(queueConfig, key, None)
                        elif val < 0:
                            setattr(queueConfig, key, 0)
                if getattr(queueConfig, "maxWorkers", None) is None:
                    queueConfig.maxWorkers = 0
                if getattr(queueConfig, "nQueueLimitWorker", None) is not None and getattr(queueConfig, "nQueueLimitWorkerMax", None) is not None:
                    max_queue_workers = min(queueConfig.nQueueLimitWorker, queueConfig.nQueueLimitWorkerMax, queueConfig.maxWorkers)
                    queueConfig.nQueueLimitWorker = max_queue_workers
                    queueConfig.nQueueLimitWorkerMax = max_queue_workers
                elif getattr(queueConfig, "nQueueLimitWorker", None) is not None:
                    queueConfig.nQueueLimitWorker = min(queueConfig.nQueueLimitWorker, queueConfig.maxWorkers)
                elif getattr(queueConfig, "nQueueLimitWorkerMax", None) is not None:
                    queueConfig.nQueueLimitWorkerMax = min(queueConfig.nQueueLimitWorkerMax, queueConfig.maxWorkers)
                    queueConfig.nQueueLimitWorker = queueConfig.nQueueLimitWorkerMax
                if getattr(queueConfig, "nQueueLimitWorkerMin", None) is not None and getattr(queueConfig, "nQueueLimitWorker", None) is not None:
                    queueConfig.nQueueLimitWorkerMin = min(queueConfig.nQueueLimitWorkerMin, queueConfig.nQueueLimitWorker, queueConfig.maxWorkers)
                if getattr(queueConfig, "nQueueLimitWorkerCoresMin", None) is not None and getattr(queueConfig, "nQueueLimitWorkerCores", None) is not None:
                    queueConfig.nQueueLimitWorkerCoresMin = min(queueConfig.nQueueLimitWorkerCoresMin, queueConfig.nQueueLimitWorkerCores)
                if getattr(queueConfig, "nQueueLimitWorkerMemoryMin", None) is not None and getattr(queueConfig, "nQueueLimitWorkerMemory", None) is not None:
                    queueConfig.nQueueLimitWorkerMemoryMin = min(queueConfig.nQueueLimitWorkerMemoryMin, queueConfig.nQueueLimitWorkerMemory)
                if getattr(queueConfig, "nQueueLimitJob", None) is not None and getattr(queueConfig, "nQueueLimitJobMax", None) is not None:
                    max_queue_jobs = min(queueConfig.nQueueLimitJob, queueConfig.nQueueLimitJobMax)
                    queueConfig.nQueueLimitJob = max_queue_jobs
                    queueConfig.nQueueLimitJobMax = max_queue_jobs
                elif getattr(queueConfig, "nQueueLimitJobMax", None) is not None:
                    queueConfig.nQueueLimitJob = queueConfig.nQueueLimitJobMax
                if getattr(queueConfig, "nQueueLimitJobMin", None) is not None and getattr(queueConfig, "nQueueLimitJob", None) is not None:
                    queueConfig.nQueueLimitJobMin = min(queueConfig.nQueueLimitJobMin, queueConfig.nQueueLimitJob)
                if getattr(queueConfig, "nQueueLimitJobCoresMin", None) is not None and getattr(queueConfig, "nQueueLimitJobCores", None) is not None:
                    queueConfig.nQueueLimitJobCoresMin = min(queueConfig.nQueueLimitJobCoresMin, queueConfig.nQueueLimitJobCores)
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
                        mainLog.error(f"Missing mandatory attributes {','.join(missing_attr_list)} . Omitted {queueConfig.queueName} in queue config")
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
            for queueName, queueConfig in newQueueConfig.items():
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
                    dumpSpec.creationTime = core_utils.naive_utcnow()
                    dumpSpec.configID = self.dbProxy.get_next_seq_number("SEQ_configID")
                    tmpStat = self.dbProxy.add_queue_config_dump(dumpSpec)
                    if not tmpStat:
                        dumpSpec.configID = self.dbProxy.get_config_id_dump(dumpSpec)
                        if dumpSpec.configID is None:
                            mainLog.error(f"failed to get configID for {dumpSpec.dumpUniqueName}")
                            continue
                    queueConfigDumps[dumpSpec.dumpUniqueName] = dumpSpec
                queueConfig.configID = dumpSpec.configID
                # ignore offline
                if queueConfig.queueStatus == "offline":
                    mainLog.debug(f"{queueName} inactive due to offline")
                    continue
                # filter for pilot version
                if (
                    hasattr(harvester_config.qconf, "pilotVersion")
                    and pandaQueueDict.get(queueConfig.siteName) is not None
                    and pandaQueueDict.get(queueConfig.siteName).get("pilot_version") != str(harvester_config.qconf.pilotVersion)
                ):
                    mainLog.debug(f"{queueName} inactive due to unmatched pilot version")
                    continue
                # filter if static queue list
                if (
                    "ALL" not in harvester_config.qconf.queueList
                    and "DYNAMIC" not in harvester_config.qconf.queueList
                    and queueName not in harvester_config.qconf.queueList
                ):
                    mainLog.debug(f"{queueName} inactive due to not in static queue list")
                    continue
                # added to active queues
                activeQueues[queueName] = queueConfig
            self.queueConfig = newQueueConfig.copy()
            self.activeQueues = activeQueues.copy()
            newQueueConfigWithID = dict()
            for dumpSpec in queueConfigDumps.values():
                queueConfig = QueueConfig(dumpSpec.queueName)
                queueConfig.update_attributes(dumpSpec.data)
                queueConfig.configID = dumpSpec.configID
                newQueueConfigWithID[dumpSpec.configID] = queueConfig
            self.queueConfigWithID = newQueueConfigWithID
            # update lastUpdate and lastCheck
            self.lastUpdate = core_utils.naive_utcnow()
            self.lastCheck = self.lastUpdate
        # update database pq_table
        if self.toUpdateDB:
            retVal = self._update_pq_table(cache_time=self.last_cache_ts, refill_table=refill_table)
            if retVal:
                if self.last_cache_ts:
                    mainLog.debug(f"updated pq_table (last cache updated at {str(self.last_cache_ts)})")
                else:
                    mainLog.debug("updated pq_table")
            elif retVal is None:
                mainLog.debug("did not get pq_table_fill lock. Skipped to update pq_table")
            else:
                mainLog.warning("failed to update pq_table. Skipped")
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
        for queue_name, queue_attribs in active_queues.items():
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
