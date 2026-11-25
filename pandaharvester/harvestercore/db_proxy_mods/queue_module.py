"""
DB proxy module for PanDA queues, worker limits and queue config dumps

"""

import datetime
import math

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_mods.base_module import (
    BaseModule,
    _logger,
    eventTableName,
    fileTableName,
    jobTableName,
    jobWorkerTableName,
    pandaQueueTableName,
    queueConfigDumpTableName,
    workTableName,
)
from pandaharvester.harvestercore.panda_queue_spec import PandaQueueSpec
from pandaharvester.harvestercore.queue_config_dump_spec import QueueConfigDumpSpec
from pandaharvester.harvestercore.work_spec import WorkSpec


# DB proxy module for PanDA queues, worker limits and queue config dumps
class QueueModule(BaseModule):
    # fill panda queue table
    def fill_panda_queue_table(self, panda_queue_list, queue_config_mapper, refill_table=False):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="fill_panda_queue_table")
            tmpLog.debug(f"start, refill={refill_table}")
            # get existing queues
            sqlE = f"SELECT queueName FROM {pandaQueueTableName} "
            varMap = dict()
            self.execute(sqlE, varMap)
            resE = self.cur.fetchall()
            for (queueName,) in resE:
                # delete if not listed in cfg
                if queueName not in panda_queue_list:
                    sqlD = f"DELETE FROM {pandaQueueTableName} "
                    sqlD += "WHERE queueName=:queueName "
                    varMap = dict()
                    varMap[":queueName"] = queueName
                    self.execute(sqlD, varMap)
                    # commit
                    self.commit()
            # loop over queues
            for queueName in panda_queue_list:
                queueConfig = queue_config_mapper.get_queue(queueName)
                if queueConfig is not None:
                    # check if already exist
                    sqlC = f"SELECT * FROM {pandaQueueTableName} "
                    sqlC += "WHERE queueName=:queueName "
                    sqlC += " AND resourceType=:resourceType AND jobType=:jobType "
                    varMap = dict()
                    varMap[":queueName"] = queueName
                    varMap[":resourceType"] = PandaQueueSpec.RT_catchall
                    varMap[":jobType"] = PandaQueueSpec.JT_catchall
                    self.execute(sqlC, varMap)
                    resC = self.cur.fetchone()
                    if refill_table:
                        sqlD = f"DELETE FROM {pandaQueueTableName} "
                        sqlD += "WHERE queueName=:queueName "
                        varMap = dict()
                        varMap[":queueName"] = queueName
                        self.execute(sqlD, varMap)
                    if resC is not None and not refill_table:
                        # update limits just in case
                        varMap = dict()
                        sqlU = f"UPDATE {pandaQueueTableName} SET "
                        for qAttr in [
                            "nQueueLimitJob",
                            "nQueueLimitWorker",
                            "maxWorkers",
                            "nQueueLimitJobRatio",
                            "nQueueLimitJobMax",
                            "nQueueLimitJobMin",
                            "nQueueLimitWorkerRatio",
                            "nQueueLimitWorkerMax",
                            "nQueueLimitWorkerMin",
                        ]:
                            if hasattr(queueConfig, qAttr):
                                sqlU += "{0}=:{0},".format(qAttr)
                                varMap[f":{qAttr}"] = getattr(queueConfig, qAttr)
                        if len(varMap) == 0:
                            continue
                        sqlU = sqlU[:-1]
                        sqlU += " WHERE queueName=:queueName "
                        varMap[":queueName"] = queueName
                        self.execute(sqlU, varMap)
                    else:
                        # insert queue
                        varMap = dict()
                        varMap[":queueName"] = queueName
                        attrName_list = []
                        tmpKey_list = []
                        for attrName in PandaQueueSpec.column_names().split(","):
                            if hasattr(queueConfig, attrName):
                                tmpKey = f":{attrName}"
                                attrName_list.append(attrName)
                                tmpKey_list.append(tmpKey)
                                varMap[tmpKey] = getattr(queueConfig, attrName)
                        sqlP = f"INSERT IGNORE INTO {pandaQueueTableName} ({','.join(attrName_list)}) "
                        sqlS = f"VALUES ({','.join(tmpKey_list)}) "
                        self.execute(sqlP + sqlS, varMap)
                    # commit
                    self.commit()
            tmpLog.debug("done")
            # return
            return True
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # get queues to submit workers
    def get_queues_to_submit(self, lookup_interval, lock_interval, locked_by, queue_lock_interval):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_queues_to_submit")
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

            # sql to get orphaned workers
            sql_get_orphaned_workers = (
                f"SELECT workerID FROM {workTableName} WHERE computingSite=:computingSite AND status=:status AND modificationTime<:timeLimit "
            )

            # sql to delete orphaned workers. Not to use bulk delete to avoid deadlock with 0-record deletion
            sql_delete_orphaned_worker = f"DELETE FROM {workTableName} WHERE workerID=:workerID "

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
            varMap = dict()
            varMap[":lockTimeLimit"] = timeNow - datetime.timedelta(seconds=queue_lock_interval)
            varMap[":lookupTimeLimit"] = timeNow - datetime.timedelta(seconds=lookup_interval)
            self.execute(sql_get_site, varMap)
            resS = self.cur.fetchall()
            for (siteName,) in resS:
                # update timestamp to lock the site
                varMap = dict()
                varMap[":siteName"] = siteName
                varMap[":submitTime"] = timeNow
                varMap[":lockedBy"] = locked_by
                varMap[":timeLimit"] = timeNow - datetime.timedelta(seconds=lookup_interval)
                self.execute(sql_lock_site, varMap)
                nRow = self.cur.rowcount
                # commit
                self.commit()
                # skip if not locked
                if nRow == 0:
                    continue
                # get queues
                varMap = dict()
                varMap[":siteName"] = siteName
                self.execute(sql_get_queues, varMap)
                resQ = self.cur.fetchall()
                for queueName, jobType, resourceType, nNewWorkers in resQ:
                    # delete orphaned workers
                    varMap = dict()
                    varMap[":computingSite"] = queueName
                    varMap[":status"] = WorkSpec.ST_pending
                    varMap[":timeLimit"] = timeNow - datetime.timedelta(seconds=lock_interval)
                    sql_get_orphaned_workers_tmp = sql_get_orphaned_workers
                    if jobType != "ANY":
                        varMap[":jobType"] = jobType
                        sql_get_orphaned_workers_tmp += "AND jobType=:jobType "
                    if resourceType != "ANY":
                        varMap[":resourceType"] = resourceType
                        sql_get_orphaned_workers_tmp += "AND resourceType=:resourceType "
                    self.execute(sql_get_orphaned_workers_tmp, varMap)
                    resO = self.cur.fetchall()
                    for (tmpWorkerID,) in resO:
                        varMap = dict()
                        varMap[":workerID"] = tmpWorkerID
                        self.execute(sql_delete_orphaned_worker, varMap)
                        # commit
                        self.commit()

                    # count nQueue
                    varMap = dict()
                    varMap[":computingSite"] = queueName
                    varMap[":resourceType"] = resourceType
                    sql_count_workers_tmp = sql_count_workers
                    if jobType != "ANY":
                        varMap[":jobType"] = jobType
                        sql_count_workers_tmp += "AND jobType=:jobType "
                    if resourceType != "ANY":
                        varMap[":resourceType"] = resourceType
                        sql_count_workers_tmp += "AND resourceType=:resourceType "
                    sql_count_workers_tmp += "GROUP BY pilotType, status "
                    self.execute(sql_count_workers_tmp, varMap)
                    # Fetch worker count results BEFORE executing any other query to preserve cursor state
                    resW = self.cur.fetchall()

                    # count nFillers once per queue/jobType/resourceType combination
                    varMap = dict()
                    varMap[":computingSite"] = queueName
                    varMap[":status"] = WorkSpec.ST_running
                    sql_count_refillers_tmp = sql_count_refillers
                    if jobType != "ANY":
                        varMap[":jobType"] = jobType
                        sql_count_refillers_tmp += "AND jobType=:jobType "
                    if resourceType != "ANY":
                        varMap[":resourceType"] = resourceType
                        sql_count_refillers_tmp += "AND resourceType=:resourceType "
                    self.execute(sql_count_refillers_tmp, varMap)
                    (nReFill,) = self.cur.fetchone()

                    # Initialize nested dict structure before the loop
                    retMap.setdefault(queueName, {})
                    retMap[queueName].setdefault(jobType, {})
                    retMap[queueName][jobType].setdefault(resourceType, {})
                    retMap[queueName][jobType][resourceType].setdefault("ANY", {"nReady": 0, "nRunning": 0, "nQueue": 0, "nNewWorkers": 0})

                    for pilotType, workerStatus, tmpNum in resW:
                        nQueue = 0
                        nReady = 0
                        nRunning = 0
                        if workerStatus in [WorkSpec.ST_submitted, WorkSpec.ST_pending, WorkSpec.ST_idle]:
                            nQueue += tmpNum
                        elif workerStatus in [WorkSpec.ST_ready]:
                            nReady += tmpNum
                        elif workerStatus in [WorkSpec.ST_running]:
                            nRunning += tmpNum

                        # Initialize or update pilot type entry
                        if pilotType not in retMap[queueName][jobType][resourceType]:
                            retMap[queueName][jobType][resourceType][pilotType] = {
                                "nReady": 0,
                                "nRunning": 0,
                                "nQueue": 0,
                                "nNewWorkers": 0,
                            }
                        retMap[queueName][jobType][resourceType][pilotType]["nReady"] += nReady
                        retMap[queueName][jobType][resourceType][pilotType]["nRunning"] += nRunning
                        retMap[queueName][jobType][resourceType][pilotType]["nQueue"] += nQueue
                        # ANY pilotType
                        retMap[queueName][jobType][resourceType]["ANY"]["nReady"] += nReady
                        retMap[queueName][jobType][resourceType]["ANY"]["nRunning"] += nRunning
                        retMap[queueName][jobType][resourceType]["ANY"]["nQueue"] += nQueue

                    # Add refiller count once to the ANY pilotType entry
                    retMap[queueName][jobType][resourceType]["ANY"]["nReady"] += nReFill

                    # set nNewWorkers only in ANY pilotType
                    retMap[queueName][jobType][resourceType]["ANY"]["nNewWorkers"] = nNewWorkers

                    resourceMap.setdefault(jobType, {})
                    resourceMap[jobType][resourceType] = queueName

                # enough queues
                if len(retMap) >= 0:
                    break
            tmpLog.debug(f"got retMap {str(retMap)}")
            tmpLog.debug(f"got siteName {str(siteName)}")
            tmpLog.debug(f"got resourceMap {str(resourceMap)}")
            return retMap, siteName, resourceMap
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}, None, {}

    # clone queue
    def clone_queue_with_new_job_and_resource_type(self, site_name, queue_name, job_type, resource_type, new_workers):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"site_name={site_name} queue_name={queue_name}", method_name="clone_queue_with_new_job_and_resource_type")
            tmpLog.debug("start")

            # get the values from one of the existing queues
            sql_select_queue = f"SELECT {PandaQueueSpec.column_names()} FROM {pandaQueueTableName} "
            sql_select_queue += "WHERE siteName=:siteName "
            var_map = dict()
            var_map[":siteName"] = site_name
            self.execute(sql_select_queue, var_map)
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
        except Exception:
            self.rollback()
            core_utils.dump_error_message(_logger)
            return False

    # set queue limit
    def set_queue_limit(self, site_name, params):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"siteName={site_name}", method_name="set_queue_limit")
            tmpLog.debug("start")

            # sql to reset queue limits before setting new command to avoid old values being repeated again and again
            sql_reset = f"UPDATE {pandaQueueTableName} SET nNewWorkers=:zero WHERE siteName=:siteName "

            # sql to get resource types
            sql_get_job_resource = f"SELECT jobType, resourceType FROM {pandaQueueTableName} WHERE siteName=:siteName FOR UPDATE "

            # sql to update nQueueLimit
            sql_update_queue = (
                f"UPDATE {pandaQueueTableName} SET nNewWorkers=:nQueue WHERE siteName=:siteName AND jobType=:jobType AND resourceType=:resourceType "
            )

            # sql to get num of submitted workers
            sql_count_workers = (
                "SELECT COUNT(*) cnt "
                f"FROM {workTableName} wt, {pandaQueueTableName} pq "
                "WHERE pq.siteName=:siteName AND wt.computingSite=pq.queueName AND wt.status=:status "
                "AND pq.jobType=:jobType AND pq.resourceType=:resourceType "
            )

            # reset nqueued for all job & resource types
            varMap = dict()
            varMap[":zero"] = 0
            varMap[":siteName"] = site_name
            self.execute(sql_reset, varMap)

            # get job & resource types
            varMap = dict()
            varMap[":siteName"] = site_name
            self.execute(sql_get_job_resource, varMap)
            results = self.cur.fetchall()
            job_resource_type_list = set()
            for tmp_job_type, tmp_resource_type in results:
                job_resource_type_list.add((tmp_job_type, tmp_resource_type))

            # set all queues
            nUp = 0
            ret_map = dict()
            queue_name = site_name

            for job_type, job_values in params.items():
                ret_map.setdefault(job_type, {})
                for resource_type, value in job_values.items():
                    tmpLog.debug(f"Processing rt {resource_type} -> {value}")

                    # get num of submitted workers
                    varMap = dict()
                    varMap[":siteName"] = site_name
                    varMap[":jobType"] = job_type
                    varMap[":resourceType"] = resource_type
                    varMap[":status"] = "submitted"
                    self.execute(sql_count_workers, varMap)
                    res = self.cur.fetchone()
                    tmpLog.debug(f"{resource_type} has {res} submitted workers")

                    if value is None:
                        value = 0
                    varMap = dict()
                    varMap[":nQueue"] = value
                    varMap[":siteName"] = site_name
                    varMap[":jobType"] = job_type
                    varMap[":resourceType"] = resource_type
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
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # get queue status (deprecated)
    def get_worker_limits_old(self, site_name):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, token=f"site_name={site_name}", method_name="get_worker_limits_old")
            tmpLog.debug("start")

            # sql to get queue limits
            sqlQ = "SELECT maxWorkers, nQueueLimitWorker, nQueueLimitWorkerRatio,"
            sqlQ += f"nQueueLimitWorkerMax,nQueueLimitWorkerMin FROM {pandaQueueTableName} "
            sqlQ += "WHERE siteName=:siteName AND resourceType='ANY' AND (jobType='ANY' OR jobType IS NULL) "

            # sql to count resource types
            sqlNT = f"SELECT COUNT(*) cnt FROM {pandaQueueTableName} "
            sqlNT += "WHERE siteName=:siteName AND resourceType!='ANY'"

            # sql to count running workers
            sqlNR = f"SELECT COUNT(*) cnt FROM {workTableName} "
            sqlNR += "WHERE computingSite=:computingSite AND status IN (:status1)"

            # get
            varMap = dict()
            varMap[":siteName"] = site_name
            self.execute(sqlQ, varMap)
            resQ = self.cur.fetchall()
            # count resource types
            varMap = dict()
            varMap[":computingSite"] = site_name
            varMap[":siteName"] = site_name
            self.execute(sqlNT, varMap)
            resNT = self.cur.fetchall()
            # count running workers
            varMap = dict()
            varMap[":computingSite"] = site_name
            varMap[":status1"] = "running"
            self.execute(sqlNR, varMap)
            resNR = self.cur.fetchall()

            # dynamic nQueueLimitWorker
            retMap = dict()
            nRunning = 0
            nRT = 1
            for (cnt,) in resNR:
                nRunning = cnt
            for (cnt,) in resNT:
                nRT = max(nRT, cnt)
            for maxWorkers, nQueueLimitWorker_orig, nQueueLimitWorkerRatio, nQueueLimitWorkerMax, nQueueLimitWorkerMin_orig in resQ:
                if nQueueLimitWorkerRatio is not None and nQueueLimitWorkerRatio > 0:
                    nQueueLimitWorkerByRatio = int(nRunning * nQueueLimitWorkerRatio / 100)
                    nQueueLimitWorkerMin = 1
                    if nQueueLimitWorkerMin_orig is not None:
                        nQueueLimitWorkerMin = nQueueLimitWorkerMin_orig
                    nQueueLimitWorkerMinAllRTs = nQueueLimitWorkerMin * nRT
                    nQueueLimitWorker = max(nQueueLimitWorkerByRatio, nQueueLimitWorkerMinAllRTs)
                    nQueueLimitWorkerPerRT = max(nQueueLimitWorkerByRatio, nQueueLimitWorkerMin)
                    if nQueueLimitWorkerMax is not None:
                        nQueueLimitWorker = min(nQueueLimitWorker, nQueueLimitWorkerMax)
                        nQueueLimitWorkerPerRT = min(nQueueLimitWorkerPerRT, nQueueLimitWorkerMax)
                    if nQueueLimitWorker_orig is not None:
                        nQueueLimitWorker = min(nQueueLimitWorker, nQueueLimitWorker_orig)
                        nQueueLimitWorkerPerRT = min(nQueueLimitWorkerPerRT, nQueueLimitWorker_orig)
                elif nQueueLimitWorker_orig is not None:
                    nQueueLimitWorker = nQueueLimitWorker_orig
                    nQueueLimitWorkerPerRT = nQueueLimitWorker
                else:
                    nQueueLimitWorker = maxWorkers
                    nQueueLimitWorkerPerRT = nQueueLimitWorker
                nQueueLimitWorker = min(nQueueLimitWorker, maxWorkers)
                retMap.update(
                    {
                        "maxWorkers": maxWorkers,
                        "nQueueLimitWorker": nQueueLimitWorker,
                        "nQueueLimitWorkerPerRT": nQueueLimitWorkerPerRT,
                    }
                )
            # commit
            self.commit()
            tmpLog.debug(f"got {str(retMap)}")
            return retMap
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # get worker limits of a queue
    def get_worker_limits(self, site_name: str, queue_config) -> tuple[dict, dict]:
        """
        Evaluate and return the worker limits and worker status of the site, both in dict

        Args:
            site_name (str): name of the site (PanDA queue)
            queue_config (QueueConfig): queue config object of the site

        Returns:
            tuple[dict, dict]: First dict for evaluated values of worker limits and per-resource-type limits, second dict for worker stats queried
        """
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, token=f"site_name={site_name}", method_name="get_worker_limits")
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
            varMap = dict()
            varMap[":computingSite"] = site_name
            varMap[":siteName"] = site_name
            self.execute(sqlNRT, varMap)
            resNT = self.cur.fetchall()
            # count workers by status
            varMap = dict()
            varMap[":computingSite"] = site_name
            varMap[":status1"] = WorkSpec.ST_running
            varMap[":status2"] = WorkSpec.ST_submitted
            varMap[":status3"] = WorkSpec.ST_idle
            varMap[":status4"] = WorkSpec.ST_pending
            varMap[":status5"] = WorkSpec.ST_ready
            self.execute(sqlNW, varMap)
            resNW = self.cur.fetchall()
            # n resource types and worker stats
            nRT = 1
            for (cnt,) in resNT:
                nRT = max(nRT, cnt)
            worker_stats_map = {}
            for status in [WorkSpec.ST_running, WorkSpec.ST_submitted, WorkSpec.ST_idle, WorkSpec.ST_pending, WorkSpec.ST_ready]:
                worker_stats_map.setdefault(status, {"n": 0, "core": 0, "mem": 0})
            worker_stats_map.setdefault("queue", {"n": 0, "core": 0, "mem": 0})
            for status, cnt, corecount, ramcount in resNW:
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
            worker_limits_dict = dict()
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
            # update map
            worker_limits_dict.update(
                {
                    "maxWorkers": maxWorkers,
                    "nQueueLimitWorker": n_queue_limit_worker_eval,
                    "nQueueLimitWorkerPerRT": n_queue_limit_worker_per_rt_eval,
                    "nQueueWorkerCores": n_queue_limit_worker_cores_eval,
                    "nQueueWorkerMemory": n_queue_limit_worker_mem_eval,
                }
            )
            # commit
            self.commit()
            tmpLog.debug(f"got {str(worker_limits_dict)}")
            return worker_limits_dict, worker_stats_map
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}, {}

    # get queue config dumps
    def get_queue_config_dumps(self):
        try:
            retVal = dict()
            configIDs = set()
            # time limit
            timeLimit = core_utils.naive_utcnow() - datetime.timedelta(hours=24)
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_queue_config_dumps")
            tmpLog.debug("start")
            # sql to get used IDs
            sqlIJ = f"SELECT DISTINCT configID FROM {jobTableName} "
            self.execute(sqlIJ)
            resIJ = self.cur.fetchall()
            for (tmpID,) in resIJ:
                configIDs.add(tmpID)
            sqlIW = f"SELECT DISTINCT configID FROM {workTableName} "
            self.execute(sqlIW)
            resIW = self.cur.fetchall()
            for (tmpID,) in resIW:
                configIDs.add(tmpID)
            # sql to delete
            sqlD = f"DELETE FROM {queueConfigDumpTableName} WHERE configID=:configID "
            # sql to get config
            sqlQ = f"SELECT {QueueConfigDumpSpec.column_names()} FROM {queueConfigDumpTableName} "
            sqlQ += "FOR UPDATE "
            self.execute(sqlQ)
            resQs = self.cur.fetchall()
            iDump = 0
            iDel = 0
            for resQ in resQs:
                dumpSpec = QueueConfigDumpSpec()
                dumpSpec.pack(resQ)
                # delete if unused and too old
                if dumpSpec.configID not in configIDs and dumpSpec.creationTime < timeLimit:
                    varMap = dict()
                    varMap[":configID"] = dumpSpec.configID
                    self.execute(sqlD, varMap)
                    iDel += 1
                else:
                    retVal[dumpSpec.dumpUniqueName] = dumpSpec
                    iDump += 1
            # commit
            self.commit()
            tmpLog.debug(f"got {iDump} dumps and delete {iDel} dumps")
            # return
            return retVal
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(tmpLog)
            # return
            return {}

    # add queue config dump
    def add_queue_config_dump(self, dump_spec):
        try:
            # sql to insert a job
            sqlJ = f"INSERT INTO {queueConfigDumpTableName} ({QueueConfigDumpSpec.column_names()}) "
            sqlJ += QueueConfigDumpSpec.bind_values_expression()
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="add_queue_config_dumps")
            tmpLog.debug(f"start for {dump_spec.dumpUniqueName}")
            varMap = dump_spec.values_list()
            # insert
            self.execute(sqlJ, varMap)
            # commit
            self.commit()
            tmpLog.debug("done")
            # return
            return True
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(tmpLog)
            # return
            return False

    # get configID for queue config dump
    def get_config_id_dump(self, dump_spec):
        try:
            # sql to get configID
            sqlJ = f"SELECT configID FROM {queueConfigDumpTableName} "
            sqlJ += "WHERE queueName=:queueName AND dumpUniqueName=:dumpUniqueName "
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_config_id_for_dump")
            tmpLog.debug(f"start for {dump_spec.queueName}:{dump_spec.dumpUniqueName}")
            # get
            varMap = dict()
            varMap[":queueName"] = dump_spec.queueName
            varMap[":dumpUniqueName"] = dump_spec.dumpUniqueName
            self.execute(sqlJ, varMap)
            resJ = self.cur.fetchone()
            if resJ is not None:
                (configID,) = resJ
            else:
                configID = None
            tmpLog.debug(f"got configID={configID}")
            # return
            return configID
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(tmpLog)
            # return
            return None

    # purge a panda queue
    def purge_pq(self, queue_name):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"queueName={queue_name}", method_name="purge_pq")
            tmpLog.debug("start")
            # sql to get jobs
            sqlJ = f"SELECT PandaID FROM {jobTableName} "
            sqlJ += "WHERE computingSite=:computingSite "
            # sql to get workers
            sqlW = f"SELECT workerID FROM {workTableName} "
            sqlW += "WHERE computingSite=:computingSite "
            # sql to get queue configs
            sqlQ = f"SELECT configID FROM {queueConfigDumpTableName} "
            sqlQ += "WHERE queueName=:queueName "
            # sql to delete job
            sqlDJ = f"DELETE FROM {jobTableName} "
            sqlDJ += "WHERE PandaID=:PandaID "
            # sql to delete files
            sqlDF = f"DELETE FROM {fileTableName} "
            sqlDF += "WHERE PandaID=:PandaID "
            # sql to delete events
            sqlDE = f"DELETE FROM {eventTableName} "
            sqlDE += "WHERE PandaID=:PandaID "
            # sql to delete relations by job
            sqlDRJ = f"DELETE FROM {jobWorkerTableName} "
            sqlDRJ += "WHERE PandaID=:PandaID "
            # sql to delete worker
            sqlDW = f"DELETE FROM {workTableName} "
            sqlDW += "WHERE workerID=:workerID "
            # sql to delete relations by worker
            sqlDRW = f"DELETE FROM {jobWorkerTableName} "
            sqlDRW += "WHERE workerID=:workerID "
            # sql to delete queue config
            sqlDQ = f"DELETE FROM {queueConfigDumpTableName} "
            sqlDQ += "WHERE configID=:configID "
            # sql to delete panda queue
            sqlDP = f"DELETE FROM {pandaQueueTableName} "
            sqlDP += "WHERE queueName=:queueName "
            # get jobs
            varMap = dict()
            varMap[":computingSite"] = queue_name
            self.execute(sqlJ, varMap)
            resJ = self.cur.fetchall()
            for (pandaID,) in resJ:
                varMap = dict()
                varMap[":PandaID"] = pandaID
                # delete job
                self.execute(sqlDJ, varMap)
                # delete files
                self.execute(sqlDF, varMap)
                # delete events
                self.execute(sqlDE, varMap)
                # delete relations
                self.execute(sqlDRJ, varMap)
            # get workers
            varMap = dict()
            varMap[":computingSite"] = queue_name
            self.execute(sqlW, varMap)
            resW = self.cur.fetchall()
            for (workerID,) in resW:
                varMap = dict()
                varMap[":workerID"] = workerID
                # delete workers
                self.execute(sqlDW, varMap)
                # delete relations
                self.execute(sqlDRW, varMap)
            # get queue configs
            varMap = dict()
            varMap[":queueName"] = queue_name
            self.execute(sqlQ, varMap)
            resQ = self.cur.fetchall()
            for (configID,) in resQ:
                varMap = dict()
                varMap[":configID"] = configID
                # delete queue configs
                self.execute(sqlDQ, varMap)
            # delete panda queue
            varMap = dict()
            varMap[":queueName"] = queue_name
            self.execute(sqlDP, varMap)
            # commit
            self.commit()
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # update PQ table
    def update_panda_queue_attribute(self, key, value, site_name=None, queue_name=None):
        tmpLog = None
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"site={site_name} queue={queue_name}", method_name="update_panda_queue")
            tmpLog.debug(f"start key={key}")
            # sql to update
            sqlJ = "UPDATE {0} SET {1}=:{1} ".format(pandaQueueTableName, key)
            sqlJ += "WHERE "
            varMap = dict()
            varMap[f":{key}"] = value
            if site_name is not None:
                sqlJ += "siteName=:siteName "
                varMap[":siteName"] = site_name
            else:
                sqlJ += "queueName=:queueName "
                varMap[":queueName"] = queue_name
            # update
            self.execute(sqlJ, varMap)
            nRow = self.cur.rowcount
            # commit
            self.commit()
            tmpLog.debug(f"done with {nRow}")
            # return
            return True
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(tmpLog)
            # return
            return False

    # release a site
    def release_site(self, site_name, locked_by):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="release_site")
            tmpLog.debug("start")
            # sql to release site
            sql = f"UPDATE {pandaQueueTableName} SET lockedBy=NULL "
            sql += "WHERE siteName=:siteName AND lockedBy=:lockedBy "
            # release site
            varMap = dict()
            varMap[":siteName"] = site_name
            varMap[":lockedBy"] = locked_by
            self.execute(sql, varMap)
            n_done = self.cur.rowcount > 0
            # commit
            self.commit()
            if n_done >= 1:
                tmpLog.debug(f"released {site_name}")
            else:
                tmpLog.debug("found nothing to release. Skipped".format(site_name))
            # return
            return True
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False
