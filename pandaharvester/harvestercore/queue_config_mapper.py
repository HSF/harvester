import copy
import datetime
import importlib
import json
import os
import threading
import time
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


# key in cache table to request all processes to reload the queue config
_reload_request_key = "_qconf_reload_request"

# key in the source signature for modification time of the local config file
_file_mtime_key = "_file_mtime"

# max interval in sec to fill pq_table even if the active queues are unchanged
_pq_table_fill_interval = 3600

# interval in sec to delete old queue config dumps, and their min age in hours
_dump_clean_interval = 3600
_dump_max_age_hours = 24

# age in hours of a dump of a current config, after which its creationTime is set to now so that it is not deleted as old
_dump_touch_age_hours = 12


# source signature as string for logs, with times in ISO format in UTC
def _format_signature(signature):
    if signature is None:
        return "None"
    items = []
    for key, val in signature.items():
        if isinstance(val, datetime.datetime):
            val = val.isoformat()
        items.append(f"{key}={val}")
    return "{" + ", ".join(items) + "}"


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
        self.masterQueue = None

    # whether the queue is a subqueue of a master queue
    @property
    def is_subqueue(self):
        return self.masterQueue is not None

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

    # recursively merge override into base in place; override wins on key conflicts
    @staticmethod
    def _deep_update(base, override):
        for key, val in override.items():
            if isinstance(base.get(key), dict) and isinstance(val, dict):
                QueueConfigMapper._deep_update(base[key], val)
            else:
                base[key] = val
        return base

    # constructor
    def __init__(self, update_db=True):
        self.lock = threading.Lock()
        self.lastUpdate = None
        self.lastCheck = None
        self.sourceSignature = None
        self.resolverCacherKey = None
        self.pqTableFillKey = None
        self.pqTableFillTime = None
        self.queueConfig = dict()
        self.activeQueues = dict()
        self.queueConfigWithID = dict()
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

    # get path of local config file
    @staticmethod
    def _get_config_file_path():
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
        return confFilePath

    # load config from local json file with syntax validation
    @staticmethod
    def _load_config_from_file():
        mainLog = _make_logger(token=f"id={core_utils.get_pid()}", method_name="_load_config_from_file")
        confFilePath = QueueConfigMapper._get_config_file_path()
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

    # get source signature
    def _get_source_signature(self):
        """
        Return a dict which changes when any source of the queue config is updated, or None if failed to get it:
        update times in cache table of the reload request, queues_config_file and resolver data, and modification time of
        the local config file
        """
        keys = [_reload_request_key]
        if self.configFromCacher:
            keys.append("queues_config_file")
        if self.resolverCacherKey:
            keys.append(self.resolverCacherKey)
        update_times = self.dbProxy.get_cache_last_update_times(keys)
        if update_times is None:
            return None
        signature = {key: update_times.get(key) for key in keys}
        try:
            signature[_file_mtime_key] = core_utils.naive_utcfromtimestamp(os.stat(self._get_config_file_path()).st_mtime)
        except OSError:
            signature[_file_mtime_key] = None
        return signature

    # request all processes to reload the queue config at their next check
    def request_reload(self):
        return self.dbProxy.refresh_cache(_reload_request_key, "_universal", f"{core_utils.naive_utc_to_timestamp(core_utils.naive_utcnow()):.3f}")

    # update pq_table with the active queues, and record what it is filled with if succeeded
    def _update_pq_table(self, fill_key, refill_table=False):
        """
        Return True if filled, False if failed, or None if the lock was not got
        """
        got_update_lock = self.dbProxy.get_process_lock("pq_table_fill", "qconf_universal", 120)
        if not got_update_lock:
            return None
        fill_ret_val = self.dbProxy.fill_panda_queue_table(self.activeQueues.keys(), self, refill_table=refill_table)
        self.dbProxy.release_process_lock("pq_table_fill", "qconf_universal")
        if not fill_ret_val:
            return False
        self.pqTableFillKey = fill_key
        self.pqTableFillTime = core_utils.naive_utcnow()
        return True

    # key of what pq_table is filled with
    def _get_pq_table_fill_key(self):
        return {queueName: (queueConfig.configID, queueConfig.queueStatus) for queueName, queueConfig in self.activeQueues.items()}

    # fill pq_table now with the active queues, waiting up to timeout sec for the lock; for admin tools
    def fill_pq_table(self, refill_table=False, timeout=180):
        """
        Return True if filled. Raise RuntimeError if failed, or if the lock was not got within timeout
        """
        self.load_data()
        deadline = time.monotonic() + timeout
        while True:
            retVal = self._update_pq_table(self._get_pq_table_fill_key(), refill_table=refill_table)
            if retVal:
                return True
            if retVal is False:
                raise RuntimeError("failed to fill pq_table")
            if time.monotonic() >= deadline:
                raise RuntimeError(f"did not get pq_table_fill lock within {timeout} sec; another process may be filling pq_table")
            time.sleep(5)

    # delete old queue config dumps, at most every _dump_clean_interval across processes
    def _clean_queue_config_dumps(self, main_log):
        if not self.dbProxy.get_process_lock("qconf_dump_clean", "qconf_universal", _dump_clean_interval):
            return
        # keep dumps of the current configs, which may not be used by workers yet; in addition to their creationTime touched by reloads
        protected_ids = {queueConfig.configID for queueConfig in self.queueConfig.values() if queueConfig.configID is not None}
        deleted_ids = self.dbProxy.clean_queue_config_dumps(_dump_max_age_hours, protected_ids)
        if deleted_ids is None:
            main_log.warning("failed to delete old queue config dumps")
        else:
            main_log.debug(f"deleted {len(deleted_ids)} old queue config dumps")

    # make queue config from dump
    @staticmethod
    def _make_queue_config_from_dump(dump_spec):
        queueConfig = QueueConfig(dump_spec.queueName)
        queueConfig.update_attributes(dump_spec.data)
        queueConfig.configID = dump_spec.configID
        return queueConfig

    # get queue config with configID; cached in the process, otherwise read from DB. None if missing
    def _get_queue_config_with_id(self, config_id):
        queueConfig = self.queueConfigWithID.get(config_id)
        if queueConfig is None:
            dumpSpec = self.dbProxy.get_queue_config_dump(config_id)
            if dumpSpec is not None:
                queueConfig = self._make_queue_config_from_dump(dumpSpec)
                self.queueConfigWithID[config_id] = queueConfig
        return queueConfig

    # load data
    def load_data(self):
        """
        Reload the queue config if any of its sources changed (see _get_source_signature), or if older than updateInterval.
        Sources are checked at most every checkInterval
        """
        mainLog = _make_logger(token=f"id={core_utils.get_pid()}", method_name="load_data")
        call_time = core_utils.naive_utcnow()
        signature = None
        # check if to update; until loaded once, wait for the ongoing first load instead
        with self.lock:
            if self.lastUpdate is not None:
                # skip if lastCheck is fresh (within checkInterval)
                if self.lastCheck is not None and call_time - self.lastCheck < datetime.timedelta(seconds=self.checkInterval):
                    return
                self.lastCheck = call_time
                # skip if no source changed and lastUpdate is fresh (within updateInterval)
                signature = self._get_source_signature()
                if (signature is None or signature == self.sourceSignature) and call_time - self.lastUpdate < datetime.timedelta(seconds=self.updateInterval):
                    return
                if signature is not None and signature != self.sourceSignature:
                    mainLog.debug(f"sources changed: {_format_signature(self.sourceSignature)} -> {_format_signature(signature)}")
        # start
        with self.lock:
            # skip if another thread reloaded while waiting for the lock
            if self.lastUpdate is not None and self.lastUpdate >= call_time:
                return
            # get resolver
            resolver = self._get_resolver()
            if resolver is None:
                mainLog.debug("No resolver is configured")
            resolverCacherKey = getattr(resolver, "cacher_key", None)
            # get the source signature before reading the sources, so that updates meanwhile trigger the next reload; reuse the one just checked
            if signature is None or resolverCacherKey != self.resolverCacherKey:
                self.resolverCacherKey = resolverCacherKey
                signature = self._get_source_signature()
            # refresh resolver data now if updated, rather than after its refresh period
            if hasattr(resolver, "force_refresh") and (
                signature is None or self.sourceSignature is None or signature.get(self.resolverCacherKey) != self.sourceSignature.get(self.resolverCacherKey)
            ):
                mainLog.debug("refresh resolver data")
                resolver.force_refresh()
            # init
            newQueueConfig = dict()
            localTemplatesDict = dict()
            remoteTemplatesDict = dict()
            finalTemplatesDict = dict()
            localQueuesDict = dict()
            remoteQueuesDict = dict()
            dynamicQueuesDict = dict()
            allQueuesNameList = set()
            invalidQueueList = set()
            pandaQueueDict = PandaQueuesDict()
            # load config json from cacher (RT & RQ)
            queueConfigJson_cacher, cache_ts = self._load_config_from_cache()
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
                for queueName in resolver.get_all_queue_names():
                    try:
                        queueDict = dict()
                        # template and default template via workflow
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
                    except Exception:
                        mainLog.error(f"failed to get dynamic queue config of {queueName}. Skipped")
                        core_utils.dump_error_message(mainLog)
            # fill in all queue name list (names of RQ + DQ + LQ)
            allQueuesNameList |= set(remoteQueuesDict)
            allQueuesNameList |= set(dynamicQueuesDict)
            allQueuesNameList |= set(localQueuesDict)
            allQueuesNameList.discard(None)
            # identify subqueues so that their master queues are processed first
            subqueue_set = set()
            referencedMasterSet = set()
            for queueName in allQueuesNameList:
                for queuesDict in [remoteQueuesDict, dynamicQueuesDict, localQueuesDict]:
                    if queueName in queuesDict and queuesDict[queueName].get("masterQueue"):
                        subqueue_set.add(queueName)
                        referencedMasterSet.add(queuesDict[queueName]["masterQueue"])
                        break
            orderedQueueNameList = sorted(allQueuesNameList, key=lambda x: x in subqueue_set)
            # resolved queueDicts of master queues, for subqueues to inherit; queueDict is not modified in place afterwards
            masterQueueDicts = {}
            # set attributes
            for queueName in orderedQueueNameList:
                try:
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
                    # subqueue inherits the resolved config of its master queue
                    masterQueueName = queueDict.get("masterQueue")
                    if masterQueueName:
                        if masterQueueName in subqueue_set:
                            mainLog.error(f"masterQueue {masterQueueName} of subqueue {queueName} is itself a subqueue, which is not allowed. Skipped")
                            invalidQueueList.add(queueName)
                            continue
                        if masterQueueName not in masterQueueDicts or masterQueueName in invalidQueueList:
                            mainLog.error(f"masterQueue {masterQueueName} of subqueue {queueName} is not found or invalid. Skipped")
                            invalidQueueList.add(queueName)
                            continue
                        queueDict = self._deep_update(copy.deepcopy(masterQueueDicts[masterQueueName]), queueDict)
                        mainLog.debug(f"subqueue {queueName} inherits config from master queue {masterQueueName}")
                    elif queueName in referencedMasterSet:
                        masterQueueDicts[queueName] = queueDict
                    # record sources of the queue config and its templates in log
                    if templateQueueName:
                        mainLog.debug(
                            f"queue {queueName} comes from {','.join(queueSourceList)} (with template {templateQueueName} from {','.join(templateSourceList)})"
                        )
                    else:
                        mainLog.debug(f"queue {queueName} comes from {','.join(queueSourceList)}")
                    # prepare queueConfig
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
                        for attName in [
                            "nQueueLimitJob",
                            "nQueueLimitJobRatio",
                            "nQueueLimitJobMax",
                            "nQueueLimitJobMin",
                            "nQueueLimitJobCores",
                            "nQueueLimitJobCoresRatio",
                            "nQueueLimitJobCoresMin",
                        ]:
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
                    if (
                        getattr(queueConfig, "nQueueLimitWorkerMemoryMin", None) is not None
                        and getattr(queueConfig, "nQueueLimitWorkerMemory", None) is not None
                    ):
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
                    missing_attr_list = []
                    for _attr in self.mandatory_attrs:
                        if not hasattr(queueConfig, _attr):
                            invalidQueueList.add(queueConfig.queueName)
                            missing_attr_list.append(_attr)
                    if missing_attr_list:
                        mainLog.error(f"Missing mandatory attributes {','.join(missing_attr_list)} . Omitted {queueConfig.queueName} in queue config")
                except Exception:
                    mainLog.error(f"failed to make queue config of {queueName}. Omitted in queue config")
                    core_utils.dump_error_message(mainLog)
                    invalidQueueList.add(queueName)
            # delete invalid queues
            for invalidQueueName in invalidQueueList:
                if invalidQueueName in newQueueConfig:
                    del newQueueConfig[invalidQueueName]
            # auto blacklisting
            autoBlacklist = False
            if resolver is not None and hasattr(harvester_config.qconf, "autoBlacklist") and harvester_config.qconf.autoBlacklist:
                autoBlacklist = True
            # get configIDs of queue config dumps; keep the previous config if failed
            dumpIDs = self.dbProxy.get_queue_config_dump_ids()
            if dumpIDs is None:
                if self.lastUpdate is None:
                    raise RuntimeError("failed to get configIDs of queue config dumps")
                mainLog.error("failed to get configIDs of queue config dumps. Kept the previous queue config")
                return
            # cache of queue configs with configID; the current ones and the old ones read from DB when needed, while their dumps exist
            liveConfigIDs = {configID for configID, _ in dumpIDs.values()}
            newQueueConfigWithID = {configID: queueConfig for configID, queueConfig in self.queueConfigWithID.items() if configID in liveConfigIDs}
            # dumps of current configs to touch
            touchTimeLimit = core_utils.naive_utcnow() - datetime.timedelta(hours=_dump_touch_age_hours)
            dumpIDsToTouch = set()
            # get active queues
            activeQueues = dict()
            for queueName, queueConfig in newQueueConfig.items():
                try:
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
                    if dumpSpec.dumpUniqueName in dumpIDs:
                        dumpSpec.configID, dumpCreationTime = dumpIDs[dumpSpec.dumpUniqueName]
                        if dumpCreationTime is None or dumpCreationTime < touchTimeLimit:
                            dumpIDsToTouch.add(dumpSpec.configID)
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
                        dumpIDs[dumpSpec.dumpUniqueName] = (dumpSpec.configID, dumpSpec.creationTime)
                    queueConfig.configID = dumpSpec.configID
                    newQueueConfigWithID[dumpSpec.configID] = self._make_queue_config_from_dump(dumpSpec)
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
                except Exception:
                    mainLog.error(f"failed to set active queue config of {queueName}. Skipped")
                    core_utils.dump_error_message(mainLog)
            self.queueConfig = newQueueConfig.copy()
            self.activeQueues = activeQueues.copy()
            self.queueConfigWithID = newQueueConfigWithID
            # what pq_table is filled with
            pq_table_fill_key = self._get_pq_table_fill_key()
            # update lastUpdate and lastCheck
            self.sourceSignature = signature
            self.lastUpdate = core_utils.naive_utcnow()
            self.lastCheck = self.lastUpdate
        # keep dumps of current configs from being deleted as old, also by cleanup in other processes
        if dumpIDsToTouch:
            if self.dbProxy.touch_queue_config_dumps(dumpIDsToTouch) is None:
                mainLog.warning(f"failed to touch {len(dumpIDsToTouch)} queue config dumps")
            else:
                mainLog.debug(f"touched {len(dumpIDsToTouch)} queue config dumps")
        if self.toUpdateDB:
            # update database pq_table if the active queues changed, or not filled for long
            if (
                pq_table_fill_key != self.pqTableFillKey
                or self.pqTableFillTime is None
                or core_utils.naive_utcnow() - self.pqTableFillTime >= datetime.timedelta(seconds=_pq_table_fill_interval)
            ):
                retVal = self._update_pq_table(pq_table_fill_key)
                if retVal:
                    if cache_ts:
                        mainLog.debug(f"updated pq_table (last cache updated at {str(cache_ts)})")
                    else:
                        mainLog.debug("updated pq_table")
                elif retVal is None:
                    mainLog.debug("did not get pq_table_fill lock. Skipped to update pq_table")
                else:
                    mainLog.warning("failed to update pq_table. Skipped")
            else:
                mainLog.debug("active queues unchanged. Skipped to update pq_table")
            # delete old queue config dumps
            self._clean_queue_config_dumps(mainLog)
        # done
        mainLog.debug("done")

    # check if valid queue
    def has_queue(self, queue_name, config_id=None):
        self.load_data()
        if config_id is not None:
            return self._get_queue_config_with_id(config_id) is not None
        return queue_name in self.queueConfig

    # get queue config
    def get_queue(self, queue_name, config_id=None):
        self.load_data()
        if config_id is not None:
            queueConfig = self._get_queue_config_with_id(config_id)
            if queueConfig is not None:
                return queueConfig
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

    # all queues with config IDs, read from DB
    def get_all_queues_with_config_ids(self):
        queueConfigDumps = self.dbProxy.get_queue_config_dumps()
        if queueConfigDumps is None:
            return dict()
        return {dumpSpec.configID: self._make_queue_config_from_dump(dumpSpec) for dumpSpec in queueConfigDumps.values()}
