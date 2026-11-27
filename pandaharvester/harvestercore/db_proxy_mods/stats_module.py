"""
DB proxy module for worker/job statistics and service metrics

"""

import datetime

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_mods.base_module import (
    BaseModule,
    _bind_in,
    db_error_handler,
    jobTableName,
    make_logger,
    pandaQueueTableName,
    serviceMetricsTableName,
    workTableName,
)
from pandaharvester.harvestercore.resource_type_constants import (
    BASIC_RESOURCE_TYPE_SINGLE_CORE,
)
from pandaharvester.harvestercore.service_metrics_spec import ServiceMetricSpec


# DB proxy module for worker/job statistics and service metrics
class StatsModule(BaseModule):
    # get worker stats
    @db_error_handler(default={})
    def get_worker_stats(self, site_name):
        # get logger
        tmpLog = make_logger(method_name="get_worker_stats")
        tmpLog.debug("start")
        # sql to get nQueueLimit
        sqlQ = f"SELECT queueName, jobType, resourceType, nNewWorkers FROM {pandaQueueTableName} "
        sqlQ += "WHERE siteName=:siteName "
        # get nQueueLimit; summed over the queues of the site
        self.execute(sqlQ, {":siteName": site_name})
        resQ = self.cur.fetchall()
        retMap = dict()
        for computingSite, jobType, resourceType, nNewWorkers in resQ:
            statsMap = retMap.setdefault(jobType, {}).setdefault(resourceType, {"running": 0, "submitted": 0, "finished": 0, "to_submit": 0})
            statsMap["to_submit"] += nNewWorkers or 0
        # sql to get worker stats of the queues of the site, by job and resource types of workers
        sqlW = (
            "SELECT wt.status, wt.jobType, wt.resourceType, COUNT(*) cnt "
            f"FROM {workTableName} wt "
            f"WHERE wt.computingSite IN (SELECT pq.queueName FROM {pandaQueueTableName} pq WHERE pq.siteName=:siteName) "
            "AND wt.status IN (:st1,:st2,:st3) "
            "GROUP BY wt.status, wt.jobType, wt.resourceType "
        )
        # get worker stats
        varMap = {":siteName": site_name, ":st1": "running", ":st2": "submitted", ":st3": "finished"}
        self.execute(sqlW, varMap)
        resW = self.cur.fetchall()
        for workerStatus, jobType, resourceType, cnt in resW:
            # skip workers without resource type
            if resourceType:
                statsMap = retMap.setdefault(jobType, {}).setdefault(resourceType, {"running": 0, "submitted": 0, "finished": 0, "to_submit": 0})
                statsMap[workerStatus] += cnt
        # commit
        self.commit()
        tmpLog.debug(f"got {str(retMap)}")
        return retMap

    # get worker stats
    @db_error_handler(default={})
    def get_worker_stats_bulk(self, active_ups_queues):
        # get logger
        tmpLog = make_logger(method_name="get_worker_stats_bulk")
        tmpLog.debug("start")
        # sql to get nQueueLimit
        sqlQ = f"SELECT queueName, jobType, resourceType, nNewWorkers FROM {pandaQueueTableName} "

        # get nQueueLimit
        self.execute(sqlQ)
        resQ = self.cur.fetchall()
        retMap = dict()
        for computingSite, jobType, resourceType, nNewWorkers in resQ:
            jobTypeMap = retMap.setdefault(computingSite, {}).setdefault(jobType, {})
            if resourceType and resourceType != "ANY":
                jobTypeMap.setdefault(resourceType, {"running": 0, "submitted": 0, "finished": 0, "to_submit": nNewWorkers})

        # sql to get worker stats
        sqlW = (
            "SELECT wt.status, wt.computingSite, wt.jobType, wt.resourceType, COUNT(*) cnt "
            f"FROM {workTableName} wt "
            "WHERE wt.status IN (:st1,:st2,:st3) "
            "GROUP BY wt.status,wt.computingSite, wt.jobType, wt.resourceType "
        )

        # get worker stats
        varMap = {":st1": "running", ":st2": "submitted", ":st3": "finished"}
        self.execute(sqlW, varMap)
        resW = self.cur.fetchall()
        for workerStatus, computingSite, jobType, resourceType, cnt in resW:
            if resourceType and resourceType != "ANY":
                jobTypeMap = retMap.setdefault(computingSite, {}).setdefault(jobType, {})
                jobTypeMap.setdefault(resourceType, {"running": 0, "submitted": 0, "finished": 0, "to_submit": 0})[workerStatus] = cnt

        # if there are no jobs for an active UPS queue, it needs to be initialized so that the pilot streaming
        # on panda server starts processing the queue
        if active_ups_queues:
            for ups_queue in active_ups_queues:
                if ups_queue not in retMap or not retMap[ups_queue] or retMap[ups_queue] == {"ANY": {}}:
                    retMap[ups_queue] = {"managed": {BASIC_RESOURCE_TYPE_SINGLE_CORE: {"running": 0, "submitted": 0, "finished": 0, "to_submit": 0}}}

        # commit
        self.commit()
        tmpLog.debug(f"got {str(retMap)}")
        return retMap

    # get full worker stats
    @db_error_handler(default={})
    def get_worker_stats_full(self, filter_site_list=None):
        # get logger
        tmpLog = make_logger(method_name="get_worker_stats_full")
        tmpLog.debug("start")
        # bind variables of sites, shared by the two queries
        varMap = dict()
        if filter_site_list is not None:
            filter_queue_str = _bind_in("site", filter_site_list, varMap)
        # sql to get nQueueLimit
        sqlQ = f"SELECT queueName, jobType, resourceType, nNewWorkers FROM {pandaQueueTableName} "
        if filter_site_list is not None:
            sqlQ += f"WHERE siteName IN ({filter_queue_str}) "
        # get nQueueLimit
        self.execute(sqlQ, varMap)
        resQ = self.cur.fetchall()
        retMap = dict()
        for computingSite, jobType, resourceType, nNewWorkers in resQ:
            computingSite = str(computingSite)
            jobType = str(jobType)
            resourceType = str(resourceType)
            retMap.setdefault(computingSite, {}).setdefault(jobType, {})[resourceType] = {"running": 0, "submitted": 0, "to_submit": nNewWorkers}
        # sql to get worker stats
        sqlW = f"SELECT wt.status, wt.computingSite, wt.jobType, wt.resourceType, COUNT(*) cnt FROM {workTableName} wt "
        if filter_site_list is not None:
            sqlW += f"WHERE wt.computingSite IN ({filter_queue_str}) "
        sqlW += "GROUP BY wt.status,wt.computingSite, wt.jobType, wt.resourceType "
        # get worker stats
        self.execute(sqlW, varMap)
        resW = self.cur.fetchall()
        for workerStatus, computingSite, jobType, resourceType, cnt in resW:
            workerStatus = str(workerStatus)
            computingSite = str(computingSite)
            jobType = str(jobType)
            resourceType = str(resourceType)
            siteMap = retMap.setdefault(computingSite, {})
            jobTypeMap = siteMap.setdefault(jobType, {})
            siteTotalMap = siteMap.setdefault("_total", {})
            jobTypeMap.setdefault(resourceType, {"running": 0, "submitted": 0, "to_submit": 0})
            jobTypeMap.setdefault("_total", {"running": 0, "submitted": 0, "to_submit": 0})
            siteTotalMap.setdefault(resourceType, {"running": 0, "submitted": 0, "to_submit": 0})
            siteTotalMap.setdefault("_total", {"running": 0, "submitted": 0, "to_submit": 0})
            jobTypeMap[resourceType][workerStatus] = cnt
            for totalMap in (jobTypeMap["_total"], siteTotalMap[resourceType], siteTotalMap["_total"]):
                totalMap[workerStatus] = totalMap.get(workerStatus, 0) + cnt
        # commit
        self.commit()
        tmpLog.debug(f"got {str(retMap)}")
        return retMap

    # get worker CE stats
    @db_error_handler(default={})
    def get_worker_ce_stats(self, site_name):
        # get logger
        tmpLog = make_logger(method_name="get_worker_ce_stats")
        tmpLog.debug("start")
        # get worker CE stats
        sqlW = "SELECT wt.status,wt.computingSite,wt.computingElement,COUNT(*) cnt "
        sqlW += f"FROM {workTableName} wt "
        sqlW += "WHERE wt.computingSite=:siteName AND wt.status IN (:st1,:st2) "
        sqlW += "GROUP BY wt.status,wt.computingElement "
        # get worker CE stats
        varMap = {":siteName": site_name, ":st1": "running", ":st2": "submitted"}
        self.execute(sqlW, varMap)
        resW = self.cur.fetchall()
        retMap = dict()
        for workerStatus, computingSite, computingElement, cnt in resW:
            retMap.setdefault(computingElement, {"running": 0, "submitted": 0})[workerStatus] = cnt
        # commit
        self.commit()
        tmpLog.debug(f"got {str(retMap)}")
        return retMap

    # get worker CE backend throughput
    @db_error_handler(default={})
    def get_worker_ce_backend_throughput(self, site_name, time_window):
        # get logger
        tmpLog = make_logger(method_name="get_worker_ce_backend_throughput")
        tmpLog.debug("start")
        # get worker CE throughput
        sqlW = "SELECT wt.computingElement,wt.status,COUNT(*) cnt "
        sqlW += f"FROM {workTableName} wt "
        sqlW += "WHERE wt.computingSite=:siteName "
        sqlW += "AND wt.status IN (:st1,:st2,:st3) "
        sqlW += "AND wt.creationtime < :timeWindowMiddle "
        sqlW += "AND (wt.starttime is NULL OR "
        sqlW += "(wt.starttime >= :timeWindowStart AND wt.starttime < :timeWindowEnd) ) "
        sqlW += "GROUP BY wt.status,wt.computingElement "
        # time window start and end
        timeWindowEnd = core_utils.naive_utcnow()
        timeWindowStart = timeWindowEnd - datetime.timedelta(seconds=time_window)
        timeWindowMiddle = timeWindowEnd - datetime.timedelta(seconds=time_window / 2)
        # get worker CE throughput
        varMap = {
            ":siteName": site_name,
            ":st1": "submitted",
            ":st2": "running",
            ":st3": "finished",
            ":timeWindowStart": timeWindowStart,
            ":timeWindowEnd": timeWindowEnd,
            ":timeWindowMiddle": timeWindowMiddle,
        }
        self.execute(sqlW, varMap)
        resW = self.cur.fetchall()
        retMap = dict()
        for computingElement, workerStatus, cnt in resW:
            retMap.setdefault(computingElement, {"submitted": 0, "running": 0, "finished": 0})[workerStatus] = cnt
        # commit
        self.commit()
        tmpLog.debug(f"got {str(retMap)} with time_window={time_window} for site {site_name}")
        return retMap

    # insert service metrics
    @db_error_handler(default=False)
    def insert_service_metrics(self, service_metric_spec):
        # get logger
        tmpLog = make_logger(method_name="insert_service_metrics")
        tmpLog.debug("start")
        sql = f"INSERT INTO {serviceMetricsTableName} ({ServiceMetricSpec.column_names()}) "
        sql += ServiceMetricSpec.bind_values_expression()
        self.execute(sql, service_metric_spec.values_list())
        self.commit()
        return True

    # get service metrics
    @db_error_handler(default={})
    def get_service_metrics(self, last_update):
        # get logger
        tmpLog = make_logger(method_name="get_service_metrics")
        tmpLog.debug(f"start with last_update: {last_update}")
        sql = f"SELECT creationTime, hostName, metrics FROM {serviceMetricsTableName} "
        sql += "WHERE creationTime>=:last_update "
        self.execute(sql, {":last_update": last_update})
        res = self.cur.fetchall()

        # change datetime objects to strings for json serialization later
        res_corrected = []
        for entry in res:
            try:
                res_corrected.append([entry[0].strftime("%Y-%m-%d %H:%M:%S.%f"), entry[1], entry[2]])
            except Exception:
                pass

        # commit
        self.commit()
        tmpLog.debug(f"got {str(res)}")
        return res_corrected

    # clean service metrics older than a week
    @db_error_handler(default=False)
    def clean_service_metrics(self):
        tmp_logger = make_logger(method_name="clean_service_metrics")
        tmp_logger.debug("start")
        # time limit in UTC like creationTime, bound rather than NOW() - INTERVAL which SQLite lacks
        sql = f"DELETE FROM {serviceMetricsTableName} WHERE creationTime<:timeLimit "
        self.execute(sql, {":timeLimit": core_utils.naive_utcnow() - datetime.timedelta(weeks=1)})
        self.commit()
        tmp_logger.debug("done")
        return True

    # get full job stats
    @db_error_handler(default=None)
    def get_job_stats_full(self, filter_site_list=None):
        # get logger
        tmpLog = make_logger(method_name="get_job_stats_full")
        tmpLog.debug("start")
        # get job stats
        varMap = dict()
        sqlJ = "SELECT jt.status, jt.computingSite, jt.resourceType, jt.nCore, COUNT(*) cnt "
        sqlJ += f"FROM {jobTableName} jt "
        if filter_site_list is not None:
            sqlJ += f"WHERE jt.computingSite IN ({_bind_in('site', filter_site_list, varMap)}) "
        sqlJ += "GROUP BY jt.status,jt.computingSite, jt.resourceType, jt.nCore "
        self.execute(sqlJ, varMap)
        resJ = self.cur.fetchall()
        # fill return map
        retMap = dict()
        for jobStatus, computingSite, resourceType, nCore, cnt in resJ:
            jobStatus = str(jobStatus)
            computingSite = str(computingSite)
            if resourceType:
                resourceType = str(resourceType)
            else:
                resourceType = "MCORE" if nCore and nCore > 1 else "SCORE"
            if not nCore:
                if resourceType.startswith("MCORE"):
                    # a guess
                    nCore = 8
                else:
                    nCore = 1
            siteMap = retMap.setdefault(computingSite, {})
            siteMap.setdefault(resourceType, {"jobs": {"starting": 0, "running": 0}, "cores": {"starting": 0, "running": 0}})
            siteMap.setdefault("_total", {"jobs": {}, "cores": {}})
            for statsMap in (siteMap[resourceType], siteMap["_total"]):
                statsMap["jobs"][jobStatus] = statsMap["jobs"].get(jobStatus, 0) + cnt
                statsMap["cores"][jobStatus] = statsMap["cores"].get(jobStatus, 0) + nCore * cnt
        # commit
        self.commit()
        tmpLog.debug(f"got {str(retMap)}")
        return retMap
