"""
DB proxy module for worker/job statistics and service metrics

"""

import datetime

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_mods.base_module import (
    BaseModule,
    _logger,
    db_error_handler,
    jobTableName,
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
        tmpLog = core_utils.make_logger(_logger, method_name="get_worker_stats")
        tmpLog.debug("start")
        # sql to get nQueueLimit
        sqlQ = f"SELECT queueName, jobType, resourceType, nNewWorkers FROM {pandaQueueTableName} "
        sqlQ += "WHERE siteName=:siteName "
        # get nQueueLimit
        varMap = dict()
        varMap[":siteName"] = site_name
        self.execute(sqlQ, varMap)
        resQ = self.cur.fetchall()
        retMap = dict()
        for computingSite, jobType, resourceType, nNewWorkers in resQ:
            retMap.setdefault(jobType, {})
            if resourceType not in retMap[jobType]:
                retMap[jobType][resourceType] = {"running": 0, "submitted": 0, "finished": 0, "to_submit": nNewWorkers}

        # get worker stats
        sqlW = (
            "SELECT wt.status, wt.computingSite, pq.jobType, pq.resourceType, COUNT(*) cnt "
            f"FROM {workTableName} wt, {pandaQueueTableName} pq "
            "WHERE pq.siteName=:siteName AND wt.computingSite=pq.queueName AND wt.status IN (:st1,:st2,:st3) "
            "GROUP BY wt.status, wt.computingSite, pq.jobType, pq.resourceType "
        )

        # get worker stats
        varMap = dict()
        varMap[":siteName"] = site_name
        varMap[":st1"] = "running"
        varMap[":st2"] = "submitted"
        varMap[":st3"] = "finished"
        self.execute(sqlW, varMap)
        resW = self.cur.fetchall()
        for workerStatus, computingSite, jobType, resourceType, cnt in resW:
            retMap.setdefault(jobType, {})
            if resourceType not in retMap:
                retMap[jobType][resourceType] = {"running": 0, "submitted": 0, "finished": 0, "to_submit": 0}
            retMap[jobType][resourceType][workerStatus] = cnt
        # commit
        self.commit()
        tmpLog.debug(f"got {str(retMap)}")
        return retMap

    # get worker stats
    @db_error_handler(default={})
    def get_worker_stats_bulk(self, active_ups_queues):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="get_worker_stats_bulk")
        tmpLog.debug("start")
        # sql to get nQueueLimit
        sqlQ = f"SELECT queueName, jobType, resourceType, nNewWorkers FROM {pandaQueueTableName} "

        # get nQueueLimit
        self.execute(sqlQ)
        resQ = self.cur.fetchall()
        retMap = dict()
        for computingSite, jobType, resourceType, nNewWorkers in resQ:
            retMap.setdefault(computingSite, {})
            retMap[computingSite].setdefault(jobType, {})
            if resourceType and resourceType != "ANY" and resourceType not in retMap[computingSite][jobType]:
                retMap[computingSite][jobType][resourceType] = {"running": 0, "submitted": 0, "finished": 0, "to_submit": nNewWorkers}

        # get worker stats
        sqlW = (
            "SELECT wt.status, wt.computingSite, wt.jobType, wt.resourceType, COUNT(*) cnt "
            f"FROM {workTableName} wt "
            "WHERE wt.status IN (:st1,:st2,:st3) "
            "GROUP BY wt.status,wt.computingSite, wt.jobType, wt.resourceType "
        )

        varMap = dict()
        varMap[":st1"] = "running"
        varMap[":st2"] = "submitted"
        varMap[":st3"] = "finished"
        self.execute(sqlW, varMap)
        resW = self.cur.fetchall()
        for workerStatus, computingSite, jobType, resourceType, cnt in resW:
            if resourceType and resourceType != "ANY":
                retMap.setdefault(computingSite, {})
                retMap[computingSite].setdefault(jobType, {})
                retMap[computingSite][jobType].setdefault(resourceType, {"running": 0, "submitted": 0, "finished": 0, "to_submit": 0})
                retMap[computingSite][jobType][resourceType][workerStatus] = cnt

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
        tmpLog = core_utils.make_logger(_logger, method_name="get_worker_stats_full")
        tmpLog.debug("start")
        # sql to get nQueueLimit
        varMap = dict()
        sqlQ = f"SELECT queueName, jobType, resourceType, nNewWorkers FROM {pandaQueueTableName} "
        if filter_site_list is not None:
            site_var_name_list = []
            for j, site in enumerate(filter_site_list):
                site_var_name = f":site{j}"
                site_var_name_list.append(site_var_name)
                varMap[site_var_name] = site
            filter_queue_str = ",".join(site_var_name_list)
            sqlQ += f"WHERE siteName IN ({filter_queue_str}) "
        # get nQueueLimit
        self.execute(sqlQ, varMap)
        resQ = self.cur.fetchall()
        retMap = dict()
        for computingSite, jobType, resourceType, nNewWorkers in resQ:
            computingSite = str(computingSite)
            jobType = str(jobType)
            resourceType = str(resourceType)
            retMap.setdefault(computingSite, {})
            retMap[computingSite].setdefault(jobType, {})
            retMap[computingSite][jobType][resourceType] = {"running": 0, "submitted": 0, "to_submit": nNewWorkers}
        # get worker stats
        varMap = dict()
        sqlW = f"SELECT wt.status, wt.computingSite, wt.jobType, wt.resourceType, COUNT(*) cnt FROM {workTableName} wt "
        if filter_site_list is not None:
            site_var_name_list = []
            for j, site in enumerate(filter_site_list):
                site_var_name = f":site{j}"
                site_var_name_list.append(site_var_name)
                varMap[site_var_name] = site
            filter_queue_str = ",".join(site_var_name_list)
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
            retMap.setdefault(computingSite, {})
            retMap[computingSite].setdefault(jobType, {})
            retMap[computingSite].setdefault("_total", {})
            retMap[computingSite][jobType].setdefault(resourceType, {"running": 0, "submitted": 0, "to_submit": 0})
            retMap[computingSite][jobType].setdefault("_total", {"running": 0, "submitted": 0, "to_submit": 0})
            retMap[computingSite]["_total"].setdefault(resourceType, {"running": 0, "submitted": 0, "to_submit": 0})
            retMap[computingSite]["_total"].setdefault("_total", {"running": 0, "submitted": 0, "to_submit": 0})
            retMap[computingSite][jobType][resourceType][workerStatus] = cnt
            retMap[computingSite][jobType]["_total"].setdefault(workerStatus, 0)
            retMap[computingSite][jobType]["_total"][workerStatus] += cnt
            retMap[computingSite]["_total"][resourceType].setdefault(workerStatus, 0)
            retMap[computingSite]["_total"][resourceType][workerStatus] += cnt
            retMap[computingSite]["_total"]["_total"].setdefault(workerStatus, 0)
            retMap[computingSite]["_total"]["_total"][workerStatus] += cnt
        # commit
        self.commit()
        tmpLog.debug(f"got {str(retMap)}")
        return retMap

    # get worker CE stats
    @db_error_handler(default={})
    def get_worker_ce_stats(self, site_name):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="get_worker_ce_stats")
        tmpLog.debug("start")
        # get worker CE stats
        sqlW = "SELECT wt.status,wt.computingSite,wt.computingElement,COUNT(*) cnt "
        sqlW += f"FROM {workTableName} wt "
        sqlW += "WHERE wt.computingSite=:siteName AND wt.status IN (:st1,:st2) "
        sqlW += "GROUP BY wt.status,wt.computingElement "
        # get worker CE stats
        varMap = dict()
        varMap[":siteName"] = site_name
        varMap[":st1"] = "running"
        varMap[":st2"] = "submitted"
        self.execute(sqlW, varMap)
        resW = self.cur.fetchall()
        retMap = dict()
        for workerStatus, computingSite, computingElement, cnt in resW:
            if computingElement not in retMap:
                retMap[computingElement] = {
                    "running": 0,
                    "submitted": 0,
                }
            retMap[computingElement][workerStatus] = cnt
        # commit
        self.commit()
        tmpLog.debug(f"got {str(retMap)}")
        return retMap

    # get worker CE backend throughput
    @db_error_handler(default={})
    def get_worker_ce_backend_throughput(self, site_name, time_window):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="get_worker_ce_backend_throughput")
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
        varMap = dict()
        varMap[":siteName"] = site_name
        varMap[":st1"] = "submitted"
        varMap[":st2"] = "running"
        varMap[":st3"] = "finished"
        varMap[":timeWindowStart"] = timeWindowStart
        varMap[":timeWindowEnd"] = timeWindowEnd
        varMap[":timeWindowMiddle"] = timeWindowMiddle
        self.execute(sqlW, varMap)
        resW = self.cur.fetchall()
        retMap = dict()
        for computingElement, workerStatus, cnt in resW:
            if computingElement not in retMap:
                retMap[computingElement] = {
                    "submitted": 0,
                    "running": 0,
                    "finished": 0,
                }
            retMap[computingElement][workerStatus] = cnt
        # commit
        self.commit()
        tmpLog.debug(f"got {str(retMap)} with time_window={time_window} for site {site_name}")
        return retMap

    # insert service metrics
    @db_error_handler(default=False)
    def insert_service_metrics(self, service_metric_spec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="insert_service_metrics")
        tmpLog.debug("start")
        sql = f"INSERT INTO {serviceMetricsTableName} ({ServiceMetricSpec.column_names()}) "
        sql += ServiceMetricSpec.bind_values_expression()
        var_map = service_metric_spec.values_list()

        self.execute(sql, var_map)
        self.commit()

        return True

    # get service metrics
    @db_error_handler(default={})
    def get_service_metrics(self, last_update):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="get_service_metrics")
        tmpLog.debug(f"start with last_update: {last_update}")
        sql = f"SELECT creationTime, hostName, metrics FROM {serviceMetricsTableName} "
        sql += "WHERE creationTime>=:last_update "

        var_map = {":last_update": last_update}
        self.execute(sql, var_map)
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

    # clean service metrics older than a month
    @db_error_handler(default=False)
    def clean_service_metrics(self):
        tmp_logger = core_utils.make_logger(_logger, method_name="clean_service_metrics")
        tmp_logger.debug(f"start")
        sql = f"DELETE FROM {serviceMetricsTableName} WHERE creationTime < NOW() - INTERVAL 1 WEEK"
        self.execute(sql)
        self.commit()
        tmp_logger.debug(f"done")
        return True

    # get full job stats
    @db_error_handler(default=None)
    def get_job_stats_full(self, filter_site_list=None):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="get_job_stats_full")
        tmpLog.debug("start")
        # get job stats
        varMap = dict()
        sqlJ = "SELECT jt.status, jt.computingSite, jt.resourceType, jt.nCore, COUNT(*) cnt  "
        sqlJ += f"FROM {jobTableName} jt "
        if filter_site_list is not None:
            site_var_name_list = []
            for j, site in enumerate(filter_site_list):
                site_var_name = f":site{j}"
                site_var_name_list.append(site_var_name)
                varMap[site_var_name] = site
            filter_queue_str = ",".join(site_var_name_list)
            sqlJ += f"WHERE jt.computingSite IN ({filter_queue_str}) "
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
            retMap.setdefault(computingSite, {})
            retMap[computingSite].setdefault(
                resourceType,
                {
                    "jobs": {
                        "starting": 0,
                        "running": 0,
                    },
                    "cores": {
                        "starting": 0,
                        "running": 0,
                    },
                },
            )
            retMap[computingSite].setdefault("_total", {"jobs": {}, "cores": {}})
            retMap[computingSite][resourceType]["jobs"].setdefault(jobStatus, 0)
            retMap[computingSite][resourceType]["cores"].setdefault(jobStatus, 0)
            retMap[computingSite]["_total"]["jobs"].setdefault(jobStatus, 0)
            retMap[computingSite]["_total"]["cores"].setdefault(jobStatus, 0)
            retMap[computingSite][resourceType]["jobs"][jobStatus] += cnt
            retMap[computingSite][resourceType]["cores"][jobStatus] += nCore * cnt
            retMap[computingSite]["_total"]["jobs"][jobStatus] += cnt
            retMap[computingSite]["_total"]["cores"][jobStatus] += nCore * cnt
        # commit
        self.commit()
        tmpLog.debug(f"got {str(retMap)}")
        return retMap
