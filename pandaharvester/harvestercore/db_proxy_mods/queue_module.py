"""
DB proxy module for PanDA queues, worker limits and queue config dumps

"""

import datetime
import math

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_mods.base_module import (
    BaseModule,
    db_error_handler,
    eventTableName,
    fileTableName,
    jobTableName,
    jobWorkerTableName,
    make_logger,
    pandaQueueTableName,
    queueConfigDumpTableName,
    workTableName,
)
from pandaharvester.harvestercore.panda_queue_spec import PandaQueueSpec
from pandaharvester.harvestercore.queue_config_dump_spec import QueueConfigDumpSpec
from pandaharvester.harvestercore.work_spec import WorkSpec

# attributes of queue config to refresh in existing PandaQueue entries
_queue_refresh_attrs = (
    "nQueueLimitJob",
    "nQueueLimitWorker",
    "maxWorkers",
    "nQueueLimitJobRatio",
    "nQueueLimitJobMax",
    "nQueueLimitJobMin",
    "nQueueLimitWorkerRatio",
    "nQueueLimitWorkerMax",
    "nQueueLimitWorkerMin",
    "masterQueue",
)


# get the SQL condition and bind values on jobType and resourceType unless they are the catch-all ANY
def _get_job_resource_type_condition(job_type, resource_type):
    sql = ""
    var_map = dict()
    if job_type != "ANY":
        var_map[":jobType"] = job_type
        sql += "AND jobType=:jobType "
    if resource_type != "ANY":
        var_map[":resourceType"] = resource_type
        sql += "AND resourceType=:resourceType "
    return sql, var_map


# DB proxy module for PanDA queues, worker limits and queue config dumps
class QueueModule(BaseModule):
    # fill panda queue table
    @db_error_handler(default=False)
    def fill_panda_queue_table(self, panda_queue_list, queue_config_mapper, refill_table=False):
        # get logger
        tmpLog = make_logger(method_name="fill_panda_queue_table")
        tmpLog.debug(f"start, refill={refill_table}")
        # sql to get existing queues
        sqlE = f"SELECT queueName FROM {pandaQueueTableName} "
        # sql to delete a queue
        sqlD = f"DELETE FROM {pandaQueueTableName} WHERE queueName=:queueName "
        # sql to check if the catch-all entry of a queue exists
        sqlC = f"SELECT queueName FROM {pandaQueueTableName} WHERE queueName=:queueName AND resourceType=:resourceType AND jobType=:jobType "
        # columns to insert
        pqColumns = PandaQueueSpec.column_names().split(",")
        # get existing queues
        self.execute(sqlE)
        resE = self.cur.fetchall()
        pandaQueueSet = set(panda_queue_list)
        for (queueName,) in resE:
            # delete if not listed in cfg
            if queueName not in pandaQueueSet:
                self.execute(sqlD, {":queueName": queueName})
                # commit
                self.commit()
        # loop over queues
        for queueName in panda_queue_list:
            queueConfig = queue_config_mapper.get_queue(queueName)
            if queueConfig is None:
                continue
            # check if already exist
            varMap = {":queueName": queueName, ":resourceType": PandaQueueSpec.RT_catchall, ":jobType": PandaQueueSpec.JT_catchall}
            self.execute(sqlC, varMap)
            resC = self.cur.fetchone()
            if refill_table:
                self.execute(sqlD, {":queueName": queueName})
            if resC is not None and not refill_table:
                # update limits just in case
                varMap = {f":{qAttr}": getattr(queueConfig, qAttr) for qAttr in _queue_refresh_attrs if hasattr(queueConfig, qAttr)}
                if len(varMap) == 0:
                    continue
                setStr = ",".join(f"{qAttr[1:]}={qAttr}" for qAttr in varMap)
                sqlU = f"UPDATE {pandaQueueTableName} SET {setStr} WHERE queueName=:queueName "
                varMap[":queueName"] = queueName
                self.execute(sqlU, varMap)
            else:
                # insert queue
                varMap = {":queueName": queueName}
                attrName_list = []
                for attrName in pqColumns:
                    if hasattr(queueConfig, attrName):
                        attrName_list.append(attrName)
                        varMap[f":{attrName}"] = getattr(queueConfig, attrName)
                tmpKeys = ",".join(f":{attrName}" for attrName in attrName_list)
                sqlP = f"INSERT IGNORE INTO {pandaQueueTableName} ({','.join(attrName_list)}) VALUES ({tmpKeys}) "
                self.execute(sqlP, varMap)
            # commit
            self.commit()
        tmpLog.debug("done")
        # return
        return True

    # get queues to submit workers
    @db_error_handler(default=({}, None, {}))
    def get_queues_to_submit(self, lookup_interval, locked_by, queue_lock_interval):
        # get logger
        tmpLog = make_logger(method_name="get_queues_to_submit")
        tmpLog.debug("start")
        retMap = dict()
        siteName = None
        resourceMap = dict()
        # sql to get a site
        sql_get_site = (
            f"SELECT siteName FROM {pandaQueueTableName} "
            "WHERE submitTime IS NULL "
            "OR (submitTime<:lockTimeLimit AND lockedBy IS NOT NULL) "
            "OR (submitTime<:lookupTimeLimit AND lockedBy IS NULL) "
            "ORDER BY submitTime "
        )

        # sql to get queues
        sql_get_queues = f"SELECT queueName, jobType, resourceType, nNewWorkers FROM {pandaQueueTableName} WHERE siteName=:siteName "

        # sql to count nQueue
        sql_count_workers = f"SELECT pilotType, status, COUNT(*) cnt FROM {workTableName} WHERE computingSite=:computingSite "

        # sql to count re-fillers
        sql_count_refillers = (
            f"SELECT COUNT(*) cnt FROM {workTableName} "
            "WHERE computingSite=:computingSite AND status=:status "
            "AND nJobsToReFill IS NOT NULL AND nJobsToReFill>0 "
        )

        # sql to update timestamp and lock site
        sql_lock_site = (
            f"UPDATE {pandaQueueTableName} SET submitTime=:submitTime,lockedBy=:lockedBy "
            "WHERE siteName=:siteName "
            "AND (submitTime IS NULL OR submitTime<:timeLimit) "
        )

        # get sites
        timeNow = core_utils.naive_utcnow()
        varMap = {
            ":lockTimeLimit": timeNow - datetime.timedelta(seconds=queue_lock_interval),
            ":lookupTimeLimit": timeNow - datetime.timedelta(seconds=lookup_interval),
        }
        self.execute(sql_get_site, varMap)
        resS = self.cur.fetchall()
        for (tmpSiteName,) in resS:
            # update timestamp to lock the site
            varMap = {
                ":siteName": tmpSiteName,
                ":submitTime": timeNow,
                ":lockedBy": locked_by,
                ":timeLimit": timeNow - datetime.timedelta(seconds=lookup_interval),
            }
            self.execute(sql_lock_site, varMap)
            nRow = self.cur.rowcount
            # commit
            self.commit()
            # skip if not locked
            if nRow == 0:
                continue
            siteName = tmpSiteName
            # get queues
            self.execute(sql_get_queues, {":siteName": siteName})
            resQ = self.cur.fetchall()
            for queueName, jobType, resourceType, nNewWorkers in resQ:
                # conditions on jobType and resourceType shared by the queries below
                sqlJR, varMapJR = _get_job_resource_type_condition(jobType, resourceType)
                # count nQueue
                varMap = {":computingSite": queueName}
                varMap.update(varMapJR)
                self.execute(sql_count_workers + sqlJR + "GROUP BY pilotType, status ", varMap)
                # Fetch worker count results BEFORE executing any other query to preserve cursor state
                resW = self.cur.fetchall()

                # count nFillers once per queue/jobType/resourceType combination
                varMap = {":computingSite": queueName, ":status": WorkSpec.ST_running}
                varMap.update(varMapJR)
                self.execute(sql_count_refillers + sqlJR, varMap)
                (nReFill,) = self.cur.fetchone()

                # Initialize nested dict structure before the loop
                rtMap = retMap.setdefault(queueName, {}).setdefault(jobType, {}).setdefault(resourceType, {})
                anyMap = rtMap.setdefault("ANY", {"nReady": 0, "nRunning": 0, "nQueue": 0, "nNewWorkers": 0})

                for pilotType, workerStatus, tmpNum in resW:
                    if workerStatus in [WorkSpec.ST_submitted, WorkSpec.ST_pending, WorkSpec.ST_idle]:
                        countKey = "nQueue"
                    elif workerStatus == WorkSpec.ST_ready:
                        countKey = "nReady"
                    elif workerStatus == WorkSpec.ST_running:
                        countKey = "nRunning"
                    else:
                        countKey = None
                    # Initialize or update pilot type entry
                    pilotMap = rtMap.setdefault(pilotType, {"nReady": 0, "nRunning": 0, "nQueue": 0, "nNewWorkers": 0})
                    if countKey is not None:
                        pilotMap[countKey] += tmpNum
                        # ANY pilotType
                        anyMap[countKey] += tmpNum

                # Add refiller count once to the ANY pilotType entry
                anyMap["nReady"] += nReFill

                # set nNewWorkers only in ANY pilotType
                anyMap["nNewWorkers"] = nNewWorkers

                resourceMap.setdefault(jobType, {})
                resourceMap[jobType][resourceType] = queueName

            # one locked site is enough
            break
        tmpLog.debug(f"got retMap {str(retMap)}")
        tmpLog.debug(f"got siteName {str(siteName)}")
        tmpLog.debug(f"got resourceMap {str(resourceMap)}")
        return retMap, siteName, resourceMap

    # clone queue
    @db_error_handler(default=False)
    def clone_queue_with_new_job_and_resource_type(self, site_name, queue_name, job_type, resource_type, new_workers):
        # get logger
        tmpLog = make_logger(f"site_name={site_name} queue_name={queue_name}", method_name="clone_queue_with_new_job_and_resource_type")
        tmpLog.debug("start")

        # get the values from one of the existing queues
        sql_select_queue = f"SELECT {PandaQueueSpec.column_names()} FROM {pandaQueueTableName} WHERE siteName=:siteName "
        self.execute(sql_select_queue, {":siteName": site_name})
        queue = self.cur.fetchone()

        if queue:  # a queue to clone was found
            var_map = {}
            attribute_list = []
            attr_binding_list = []
            for attribute, value in zip(PandaQueueSpec.column_names().split(","), queue):
                attr_binding = f":{attribute}"
                if attribute == "resourceType":
                    var_map[attr_binding] = resource_type
                elif attribute == "jobType":
                    var_map[attr_binding] = job_type
                elif attribute == "nNewWorkers":
                    var_map[attr_binding] = new_workers
                elif attribute == "uniqueName":
                    var_map[attr_binding] = core_utils.get_unique_queue_name(queue_name, resource_type, job_type)
                else:
                    var_map[attr_binding] = value
                attribute_list.append(attribute)
                attr_binding_list.append(attr_binding)
            sql_insert = f"INSERT IGNORE INTO {pandaQueueTableName} ({','.join(attribute_list)}) "
            sql_values = f"VALUES ({','.join(attr_binding_list)}) "

            self.execute(sql_insert + sql_values, var_map)
        else:
            tmpLog.debug("Failed to clone the queue")
        self.commit()
        return True

    # set queue limit
    @db_error_handler(default={})
    def set_queue_limit(self, site_name, params):
        # get logger
        tmpLog = make_logger(f"siteName={site_name}", method_name="set_queue_limit")
        tmpLog.debug("start")

        # sql to reset queue limits before setting new command to avoid old values being repeated again and again
        sql_reset = f"UPDATE {pandaQueueTableName} SET nNewWorkers=:zero WHERE siteName=:siteName "

        # sql to get resource types
        sql_get_job_resource = f"SELECT jobType, resourceType FROM {pandaQueueTableName} WHERE siteName=:siteName FOR UPDATE "

        # sql to update nQueueLimit
        sql_update_queue = f"UPDATE {pandaQueueTableName} SET nNewWorkers=:nQueue WHERE siteName=:siteName AND jobType=:jobType AND resourceType=:resourceType "

        # sql to get num of submitted workers
        sql_count_workers = (
            "SELECT COUNT(*) cnt "
            f"FROM {workTableName} wt, {pandaQueueTableName} pq "
            "WHERE pq.siteName=:siteName AND wt.computingSite=pq.queueName AND wt.status=:status "
            "AND pq.jobType=:jobType AND pq.resourceType=:resourceType "
        )

        # reset nqueued for all job & resource types
        self.execute(sql_reset, {":zero": 0, ":siteName": site_name})

        # get job & resource types
        self.execute(sql_get_job_resource, {":siteName": site_name})
        results = self.cur.fetchall()
        job_resource_type_list = {(tmp_job_type, tmp_resource_type) for tmp_job_type, tmp_resource_type in results}

        # set all queues
        nUp = 0
        ret_map = dict()
        queue_name = site_name

        for job_type, job_values in params.items():
            ret_map.setdefault(job_type, {})
            for resource_type, value in job_values.items():
                tmpLog.debug(f"Processing rt {resource_type} -> {value}")

                # get num of submitted workers
                varMap = {":siteName": site_name, ":jobType": job_type, ":resourceType": resource_type, ":status": "submitted"}
                self.execute(sql_count_workers, varMap)
                res = self.cur.fetchone()
                tmpLog.debug(f"{resource_type} has {res} submitted workers")

                if value is None:
                    value = 0
                varMap = {":nQueue": value, ":siteName": site_name, ":jobType": job_type, ":resourceType": resource_type}
                self.execute(sql_update_queue, varMap)
                iUp = self.cur.rowcount

                # iUp is 0 when nQueue is not changed
                if iUp > 0 or (job_type, resource_type) in job_resource_type_list:
                    # a queue was updated, add the values to the map
                    ret_map[job_type][resource_type] = value
                else:
                    # no queue was updated, we need to create a new one for the resource type
                    cloned = self.clone_queue_with_new_job_and_resource_type(site_name, queue_name, job_type, resource_type, value)
                    if cloned:
                        ret_map[job_type][resource_type] = value
                        iUp = 1

                nUp += iUp
                tmpLog.debug(f"set nNewWorkers={value} to {queue_name}:{job_type}:{resource_type} with {iUp}")

        # commit
        self.commit()
        tmpLog.debug(f"updated {nUp} queues")

        return ret_map

    # get worker limits of a queue
    @db_error_handler(default=({}, {}))
    def get_worker_limits(self, site_name: str, queue_config) -> tuple[dict, dict]:
        """
        Evaluate and return the worker limits and worker status of the site, both in dict

        Args:
            site_name (str): name of the site (PanDA queue)
            queue_config (QueueConfig): queue config object of the site

        Returns:
            tuple[dict, dict]: First dict for evaluated values of worker limits and per-resource-type limits, second dict for worker stats queried
        """
        # get logger
        tmpLog = make_logger(token=f"site_name={site_name}", method_name="get_worker_limits")
        tmpLog.debug("start")
        # sql to count resource types
        sqlNRT = f"SELECT COUNT(*) cnt FROM {pandaQueueTableName} WHERE siteName=:siteName AND resourceType!='ANY' "
        # sql to count workers group by status
        sqlNW = (
            f"SELECT status, COUNT(*) cnt, SUM(nCore) corecount, SUM(minRamCount) ramcount FROM {workTableName} "
            "WHERE computingSite=:computingSite AND status IN (:status1, :status2, :status3, :status4, :status5) "
            "GROUP BY status "
        )
        # count resource types
        self.execute(sqlNRT, {":siteName": site_name})
        (cnt,) = self.cur.fetchone()
        nRT = max(1, cnt)
        # count workers by status
        workerStatusList = [WorkSpec.ST_running, WorkSpec.ST_submitted, WorkSpec.ST_idle, WorkSpec.ST_pending, WorkSpec.ST_ready]
        varMap = {":computingSite": site_name}
        for idx, status in enumerate(workerStatusList):
            varMap[f":status{idx + 1}"] = status
        self.execute(sqlNW, varMap)
        resNW = self.cur.fetchall()
        # worker stats
        worker_stats_map = {status: {"n": 0, "core": 0, "mem": 0} for status in workerStatusList}
        worker_stats_map["queue"] = {"n": 0, "core": 0, "mem": 0}
        for status, cnt, corecount, ramcount in resNW:
            # SUM is NULL when all values are NULL
            corecount = corecount or 0
            ramcount = ramcount or 0
            worker_stats_map[status] = {
                "n": cnt,
                "core": corecount,
                "mem": ramcount,
            }
            if status in [WorkSpec.ST_submitted, WorkSpec.ST_idle, WorkSpec.ST_pending]:
                worker_stats_map["queue"]["n"] += cnt
                worker_stats_map["queue"]["core"] += corecount
                worker_stats_map["queue"]["mem"] += ramcount
        # read worker limit attributes from queue config
        maxWorkers = queue_config.maxWorkers
        nQueueLimitWorker = getattr(queue_config, "nQueueLimitWorker", None)
        nQueueLimitWorkerRatio = getattr(queue_config, "nQueueLimitWorkerRatio", None)
        nQueueLimitWorkerMin = getattr(queue_config, "nQueueLimitWorkerMin", None)
        nQueueLimitWorkerCores = getattr(queue_config, "nQueueLimitWorkerCores", None)
        nQueueLimitWorkerCoresRatio = getattr(queue_config, "nQueueLimitWorkerCoresRatio", None)
        nQueueLimitWorkerCoresMin = getattr(queue_config, "nQueueLimitWorkerCoresMin", None)
        nQueueLimitWorkerMemory = getattr(queue_config, "nQueueLimitWorkerMemory", None)
        nQueueLimitWorkerMemoryRatio = getattr(queue_config, "nQueueLimitWorkerMemoryRatio", None)
        nQueueLimitWorkerMemoryMin = getattr(queue_config, "nQueueLimitWorkerMemoryMin", None)
        # initialize
        n_queue_limit_worker_eval = nQueueLimitWorker if nQueueLimitWorker is not None else maxWorkers
        n_queue_limit_worker_per_rt_eval = n_queue_limit_worker_eval
        n_queue_limit_worker_cores_eval = nQueueLimitWorkerCores
        n_queue_limit_worker_cores_min_eval = nQueueLimitWorkerCoresMin
        n_queue_limit_worker_mem_eval = nQueueLimitWorkerMemory
        n_queue_limit_worker_mem_min_eval = nQueueLimitWorkerMemoryMin
        # dynamic n_queue_limit_worker_eval
        if nQueueLimitWorkerRatio is not None:
            n_queue_limit_worker_by_ratio = int(worker_stats_map["running"]["n"] * nQueueLimitWorkerRatio / 100)
            if nQueueLimitWorkerMin is not None and n_queue_limit_worker_by_ratio < nQueueLimitWorkerMin:
                n_queue_limit_worker_eval = min(n_queue_limit_worker_eval, nQueueLimitWorkerMin)
                n_queue_limit_worker_per_rt_eval = min(n_queue_limit_worker_per_rt_eval, math.ceil(nQueueLimitWorkerMin / nRT))
            else:
                n_queue_limit_worker_eval = min(n_queue_limit_worker_eval, n_queue_limit_worker_by_ratio)
                n_queue_limit_worker_per_rt_eval = n_queue_limit_worker_eval
        if nQueueLimitWorkerCoresRatio is not None:
            n_queue_limit_cores_by_ratio = int(worker_stats_map["running"]["core"] * nQueueLimitWorkerCoresRatio / 100)
            if nQueueLimitWorkerMin is not None:
                # get n_queue_limit_worker_cores_min_eval from nQueueLimitWorkerMin if nQueueLimitWorkerCoresMin is not set to ensure the minimum cores (1 core per worker)
                n_queue_limit_worker_cores_min_base = int(nQueueLimitWorkerMin * 1)
                if n_queue_limit_worker_cores_min_eval is None:
                    n_queue_limit_worker_cores_min_eval = n_queue_limit_worker_cores_min_base
                else:
                    n_queue_limit_worker_cores_min_eval = max(n_queue_limit_worker_cores_min_eval, n_queue_limit_worker_cores_min_base)
            if n_queue_limit_worker_cores_min_eval is not None and n_queue_limit_cores_by_ratio < n_queue_limit_worker_cores_min_eval:
                if n_queue_limit_worker_cores_eval is not None:
                    n_queue_limit_worker_cores_eval = min(n_queue_limit_worker_cores_eval, n_queue_limit_worker_cores_min_eval)
                else:
                    n_queue_limit_worker_cores_eval = n_queue_limit_worker_cores_min_eval
            else:
                if n_queue_limit_worker_cores_eval is not None:
                    n_queue_limit_worker_cores_eval = min(n_queue_limit_worker_cores_eval, n_queue_limit_cores_by_ratio)
                else:
                    n_queue_limit_worker_cores_eval = n_queue_limit_cores_by_ratio
        if nQueueLimitWorkerMemoryRatio is not None:
            n_queue_limit_mem_by_ratio = int(worker_stats_map["running"]["mem"] * nQueueLimitWorkerMemoryRatio / 100)
            if nQueueLimitWorkerMin is not None:
                # get n_queue_limit_worker_mem_min_eval from nQueueLimitWorkerMin if nQueueLimitWorkerMemoryMin is not set to ensure the minimum memory (1000 MB per worker)
                n_queue_limit_worker_mem_min_base = int(nQueueLimitWorkerMin * 1000)
                if n_queue_limit_worker_mem_min_eval is None:
                    n_queue_limit_worker_mem_min_eval = n_queue_limit_worker_mem_min_base
                else:
                    n_queue_limit_worker_mem_min_eval = max(n_queue_limit_worker_mem_min_eval, n_queue_limit_worker_mem_min_base)
            if n_queue_limit_worker_mem_min_eval is not None and n_queue_limit_mem_by_ratio < n_queue_limit_worker_mem_min_eval:
                if n_queue_limit_worker_mem_eval is not None:
                    n_queue_limit_worker_mem_eval = min(n_queue_limit_worker_mem_eval, n_queue_limit_worker_mem_min_eval)
                else:
                    n_queue_limit_worker_mem_eval = n_queue_limit_worker_mem_min_eval
            else:
                if n_queue_limit_worker_mem_eval is not None:
                    n_queue_limit_worker_mem_eval = min(n_queue_limit_worker_mem_eval, n_queue_limit_mem_by_ratio)
                else:
                    n_queue_limit_worker_mem_eval = n_queue_limit_mem_by_ratio
        # make map
        worker_limits_dict = {
            "maxWorkers": maxWorkers,
            "nQueueLimitWorker": n_queue_limit_worker_eval,
            "nQueueLimitWorkerPerRT": n_queue_limit_worker_per_rt_eval,
            "nQueueWorkerCores": n_queue_limit_worker_cores_eval,
            "nQueueWorkerMemory": n_queue_limit_worker_mem_eval,
        }
        # commit
        self.commit()
        tmpLog.debug(f"got {str(worker_limits_dict)}")
        return worker_limits_dict, worker_stats_map

    # get queue config dumps
    @db_error_handler(default=None)
    def get_queue_config_dumps(self):
        """
        Return {dumpUniqueName: QueueConfigDumpSpec} of all queue config dumps, or None if failed
        """
        tmpLog = make_logger(method_name="get_queue_config_dumps")
        tmpLog.debug("start")
        sqlQ = f"SELECT {QueueConfigDumpSpec.column_names()} FROM {queueConfigDumpTableName} "
        self.execute(sqlQ)
        resQs = self.cur.fetchall()
        # commit
        self.commit()
        retVal = dict()
        for resQ in resQs:
            dumpSpec = QueueConfigDumpSpec()
            dumpSpec.pack(resQ)
            retVal[dumpSpec.dumpUniqueName] = dumpSpec
        tmpLog.debug(f"got {len(retVal)} dumps")
        return retVal

    # get configIDs of queue config dumps
    @db_error_handler(default=None)
    def get_queue_config_dump_ids(self):
        """
        Return {dumpUniqueName: (configID, creationTime)} of all queue config dumps, or None if failed
        """
        tmpLog = make_logger(method_name="get_queue_config_dump_ids")
        sqlQ = f"SELECT dumpUniqueName,configID,creationTime FROM {queueConfigDumpTableName} "
        self.execute(sqlQ)
        retVal = {dumpUniqueName: (configID, creationTime) for dumpUniqueName, configID, creationTime in self.cur.fetchall()}
        # commit
        self.commit()
        tmpLog.debug(f"got {len(retVal)} configIDs")
        return retVal

    # get a queue config dump
    @db_error_handler(default=None)
    def get_queue_config_dump(self, config_id):
        """
        Return QueueConfigDumpSpec of config_id, or None if missing or failed
        """
        tmpLog = make_logger(f"configID={config_id}", method_name="get_queue_config_dump")
        sqlQ = f"SELECT {QueueConfigDumpSpec.column_names()} FROM {queueConfigDumpTableName} WHERE configID=:configID "
        self.execute(sqlQ, {":configID": config_id})
        resQ = self.cur.fetchone()
        # commit
        self.commit()
        if resQ is None:
            tmpLog.debug("not found")
            return None
        dumpSpec = QueueConfigDumpSpec()
        dumpSpec.pack(resQ)
        tmpLog.debug(f"got {dumpSpec.queueName}")
        return dumpSpec

    # set creationTime of queue config dumps to now, to keep dumps in use as current configs from being deleted as old
    @db_error_handler(default=None)
    def touch_queue_config_dumps(self, config_ids):
        """
        Return the number of updated dumps, or None if failed
        """
        tmpLog = make_logger(method_name="touch_queue_config_dumps")
        config_ids = sorted(config_ids)
        timeNow = core_utils.naive_utcnow()
        nUpdated = 0
        for idx in range(0, len(config_ids), 500):
            varMap = {":creationTime": timeNow}
            bindNameList = []
            for jdx, configID in enumerate(config_ids[idx : idx + 500]):
                bindNameList.append(f":configID{jdx}")
                varMap[bindNameList[-1]] = configID
            bindNames = ",".join(bindNameList)
            sqlU = f"UPDATE {queueConfigDumpTableName} SET creationTime=:creationTime WHERE configID IN ({bindNames}) "
            self.execute(sqlU, varMap)
            nUpdated += self.cur.rowcount
        # commit
        self.commit()
        tmpLog.debug(f"updated {nUpdated} dumps")
        return nUpdated

    # delete queue config dumps unused by jobs and workers, and older than max_age_hours
    @db_error_handler(default=None)
    def clean_queue_config_dumps(self, max_age_hours, protected_ids=()):
        """
        Dumps with configID in protected_ids are kept. Return the list of deleted configIDs, or None if failed
        """
        tmpLog = make_logger(method_name="clean_queue_config_dumps")
        tmpLog.debug("start")
        timeLimit = core_utils.naive_utcnow() - datetime.timedelta(hours=max_age_hours)
        # old dumps
        sqlO = f"SELECT configID FROM {queueConfigDumpTableName} WHERE creationTime<:timeLimit "
        self.execute(sqlO, {":timeLimit": timeLimit})
        candidateIDs = {configID for (configID,) in self.cur.fetchall()} - set(protected_ids)
        deletedIDs = []
        if candidateIDs:
            # configIDs in use
            usedIDs = set()
            for tableName in (jobTableName, workTableName):
                self.execute(f"SELECT DISTINCT configID FROM {tableName} ")
                usedIDs |= {configID for (configID,) in self.cur.fetchall()}
            # delete
            deletedIDs = sorted(candidateIDs - usedIDs)
            if deletedIDs:
                sqlD = f"DELETE FROM {queueConfigDumpTableName} WHERE configID=:configID "
                self.executemany(sqlD, [{":configID": configID} for configID in deletedIDs])
        # commit
        self.commit()
        tmpLog.debug(f"deleted {len(deletedIDs)} dumps")
        return deletedIDs

    # add queue config dump
    @db_error_handler(default=False)
    def add_queue_config_dump(self, dump_spec):
        # sql to insert a dump
        sqlJ = f"INSERT INTO {queueConfigDumpTableName} ({QueueConfigDumpSpec.column_names()}) "
        sqlJ += QueueConfigDumpSpec.bind_values_expression()
        # get logger
        tmpLog = make_logger(method_name="add_queue_config_dump")
        tmpLog.debug(f"start for {dump_spec.dumpUniqueName}")
        varMap = dump_spec.values_list()
        # insert
        self.execute(sqlJ, varMap)
        # commit
        self.commit()
        tmpLog.debug("done")
        # return
        return True

    # get configID for queue config dump
    @db_error_handler(default=None)
    def get_config_id_dump(self, dump_spec):
        # sql to get configID
        sqlJ = f"SELECT configID FROM {queueConfigDumpTableName} "
        sqlJ += "WHERE queueName=:queueName AND dumpUniqueName=:dumpUniqueName "
        # get logger
        tmpLog = make_logger(method_name="get_config_id_dump")
        tmpLog.debug(f"start for {dump_spec.queueName}:{dump_spec.dumpUniqueName}")
        # get
        self.execute(sqlJ, {":queueName": dump_spec.queueName, ":dumpUniqueName": dump_spec.dumpUniqueName})
        resJ = self.cur.fetchone()
        configID = resJ[0] if resJ is not None else None
        # commit
        self.commit()
        tmpLog.debug(f"got configID={configID}")
        # return
        return configID

    # purge a panda queue
    @db_error_handler(default=False)
    def purge_pq(self, queue_name):
        # get logger
        tmpLog = make_logger(f"queueName={queue_name}", method_name="purge_pq")
        tmpLog.debug("start")
        # sql to get jobs
        sqlJ = f"SELECT PandaID FROM {jobTableName} WHERE computingSite=:computingSite "
        # sql to get workers
        sqlW = f"SELECT workerID FROM {workTableName} WHERE computingSite=:computingSite "
        # sql to get queue configs
        sqlQ = f"SELECT configID FROM {queueConfigDumpTableName} WHERE queueName=:queueName "
        # sql to delete jobs, files, events and relations by job
        sqlDJList = [f"DELETE FROM {tableName} WHERE PandaID=:PandaID " for tableName in (jobTableName, fileTableName, eventTableName, jobWorkerTableName)]
        # sql to delete workers and relations by worker
        sqlDWList = [f"DELETE FROM {tableName} WHERE workerID=:workerID " for tableName in (workTableName, jobWorkerTableName)]
        # sql to delete queue config
        sqlDQ = f"DELETE FROM {queueConfigDumpTableName} WHERE configID=:configID "
        # sql to delete panda queue
        sqlDP = f"DELETE FROM {pandaQueueTableName} WHERE queueName=:queueName "
        # get jobs
        self.execute(sqlJ, {":computingSite": queue_name})
        varMaps = [{":PandaID": pandaID} for (pandaID,) in self.cur.fetchall()]
        if varMaps:
            # delete jobs, files, events and relations
            for sqlDJ in sqlDJList:
                self.executemany(sqlDJ, varMaps)
        # get workers
        self.execute(sqlW, {":computingSite": queue_name})
        varMaps = [{":workerID": workerID} for (workerID,) in self.cur.fetchall()]
        if varMaps:
            # delete workers and relations
            for sqlDW in sqlDWList:
                self.executemany(sqlDW, varMaps)
        # get queue configs
        self.execute(sqlQ, {":queueName": queue_name})
        varMaps = [{":configID": configID} for (configID,) in self.cur.fetchall()]
        if varMaps:
            # delete queue configs
            self.executemany(sqlDQ, varMaps)
        # delete panda queue
        self.execute(sqlDP, {":queueName": queue_name})
        # commit
        self.commit()
        tmpLog.debug("done")
        return True

    # update PQ table
    @db_error_handler(default=False)
    def update_panda_queue_attribute(self, key, value, site_name=None, queue_name=None):
        # get logger
        tmpLog = make_logger(f"site={site_name} queue={queue_name}", method_name="update_panda_queue_attribute")
        tmpLog.debug(f"start key={key}")
        # sql to update
        varMap = {f":{key}": value}
        if site_name is not None:
            sqlJ = f"UPDATE {pandaQueueTableName} SET {key}=:{key} WHERE siteName=:siteName "
            varMap[":siteName"] = site_name
        else:
            sqlJ = f"UPDATE {pandaQueueTableName} SET {key}=:{key} WHERE queueName=:queueName "
            varMap[":queueName"] = queue_name
        # update
        self.execute(sqlJ, varMap)
        nRow = self.cur.rowcount
        # commit
        self.commit()
        tmpLog.debug(f"done with {nRow}")
        # return
        return True

    # release a site
    @db_error_handler(default=False)
    def release_site(self, site_name, locked_by):
        # get logger
        tmpLog = make_logger(method_name="release_site")
        tmpLog.debug("start")
        # sql to release site
        sql = f"UPDATE {pandaQueueTableName} SET lockedBy=NULL WHERE siteName=:siteName AND lockedBy=:lockedBy "
        # release site
        self.execute(sql, {":siteName": site_name, ":lockedBy": locked_by})
        n_done = self.cur.rowcount
        # commit
        self.commit()
        if n_done >= 1:
            tmpLog.debug(f"released {site_name}")
        else:
            tmpLog.debug(f"found nothing to release for {site_name}. Skipped")
        # return
        return True
