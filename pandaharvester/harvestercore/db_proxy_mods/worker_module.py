"""
DB proxy module for workers

"""

import copy
import datetime
import random

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_mods.base_module import (
    BaseModule,
    _logger,
    eventTableName,
    fileTableName,
    jobTableName,
    jobWorkerTableName,
    pandaQueueTableName,
    workTableName,
)
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.job_worker_relation_spec import JobWorkerRelationSpec
from pandaharvester.harvestercore.work_spec import WorkSpec


# DB proxy module for workers
class WorkerModule(BaseModule):
    # update worker
    def update_worker(self, workspec, criteria=None):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"workerID={workspec.workerID}", method_name="update_worker")
            tmpLog.debug("start")
            if criteria is None:
                criteria = {}
            # sql to update job
            sql = f"UPDATE {workTableName} SET {workspec.bind_update_changes_expression()} "
            sql += "WHERE workerID=:workerID "
            # update worker
            varMap = workspec.values_map(only_changed=True)
            if len(varMap) > 0:
                for tmpKey, tmpVal in criteria.items():
                    mapKey = f":{tmpKey}_cr"
                    sql += f"AND {tmpKey}={mapKey} "
                    varMap[mapKey] = tmpVal
                varMap[":workerID"] = workspec.workerID
                self.execute(sql, varMap)
                nRow = self.cur.rowcount
                # commit
                self.commit()
                tmpLog.debug(f"done with {nRow}")
            else:
                nRow = None
                tmpLog.debug("skip since no updated attributes")
            # return
            return nRow
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # register a worker
    def register_worker(self, workspec, jobspec_list, locked_by):
        tmpLog = core_utils.make_logger(_logger, f"batchID={workspec.batchID}", method_name="register_worker")
        try:
            tmpLog.debug("start")
            # sql to check if exists
            sqlE = f"SELECT 1 c FROM {workTableName} WHERE workerID=:workerID "
            # sql to insert job and worker relationship
            sqlR = f"INSERT INTO {jobWorkerTableName} ({JobWorkerRelationSpec.column_names()}) "
            sqlR += JobWorkerRelationSpec.bind_values_expression()
            # sql to get number of workers
            sqlNW = f"SELECT DISTINCT t.workerID FROM {jobWorkerTableName} t, {workTableName} w "
            sqlNW += "WHERE t.PandaID=:pandaID AND w.workerID=t.workerID "
            sqlNW += "AND w.status IN (:st_submitted,:st_running,:st_idle) "
            # sql to decrement nNewWorkers
            sqlDN = f"UPDATE {pandaQueueTableName} "
            sqlDN += "SET nNewWorkers=nNewWorkers-1 "
            sqlDN += "WHERE queueName=:queueName AND nNewWorkers IS NOT NULL AND nNewWorkers>0 "
            # insert worker if new
            isNew = False
            if workspec.isNew:
                varMap = dict()
                varMap[":workerID"] = workspec.workerID
                self.execute(sqlE, varMap)
                resE = self.cur.fetchone()
                if resE is None:
                    isNew = True
            if isNew:
                # insert a worker
                sqlI = f"INSERT INTO {workTableName} ({WorkSpec.column_names()}) "
                sqlI += WorkSpec.bind_values_expression()
                varMap = workspec.values_list()
                self.execute(sqlI, varMap)
                # decrement nNewWorkers
                varMap = dict()
                varMap[":queueName"] = workspec.computingSite
                self.execute(sqlDN, varMap)
            else:
                # not update workerID
                workspec.force_not_update("workerID")
                # update a worker
                sqlU = f"UPDATE {workTableName} SET {workspec.bind_update_changes_expression()} "
                sqlU += "WHERE workerID=:workerID "
                varMap = workspec.values_map(only_changed=True)
                varMap[":workerID"] = workspec.workerID
                self.execute(sqlU, varMap)
            # collect values to update jobs or insert job/worker mapping
            varMapsR = []
            if jobspec_list is not None:
                for jobSpec in jobspec_list:
                    # get number of workers for the job
                    varMap = dict()
                    varMap[":pandaID"] = jobSpec.PandaID
                    varMap[":st_submitted"] = WorkSpec.ST_submitted
                    varMap[":st_running"] = WorkSpec.ST_running
                    varMap[":st_idle"] = WorkSpec.ST_idle
                    self.execute(sqlNW, varMap)
                    resNW = self.cur.fetchall()
                    workerIDs = set()
                    workerIDs.add(workspec.workerID)
                    for (tmpWorkerID,) in resNW:
                        workerIDs.add(tmpWorkerID)
                    # update attributes
                    if jobSpec.subStatus in ["submitted", "running"]:
                        jobSpec.nWorkers = len(workerIDs)
                        try:
                            jobSpec.nWorkersInTotal += 1
                        except Exception:
                            jobSpec.nWorkersInTotal = jobSpec.nWorkers
                    elif workspec.hasJob == 1:
                        if workspec.status == WorkSpec.ST_missed:
                            # not update if other workers are active
                            if len(workerIDs) > 1:
                                continue
                            core_utils.update_job_attributes_with_workers(workspec.mapType, [jobSpec], [workspec], {}, {})
                            jobSpec.trigger_propagation()
                        else:
                            jobSpec.subStatus = "submitted"
                        jobSpec.nWorkers = len(workerIDs)
                        try:
                            jobSpec.nWorkersInTotal += 1
                        except Exception:
                            jobSpec.nWorkersInTotal = jobSpec.nWorkers
                    else:
                        if workspec.status == WorkSpec.ST_missed:
                            # not update if other workers are active
                            if len(workerIDs) > 1:
                                continue
                            core_utils.update_job_attributes_with_workers(workspec.mapType, [jobSpec], [workspec], {}, {})
                            jobSpec.trigger_propagation()
                        else:
                            jobSpec.subStatus = "queued"
                    # sql to update job
                    if len(jobSpec.values_map(only_changed=True)) > 0:
                        sqlJ = f"UPDATE {jobTableName} SET {jobSpec.bind_update_changes_expression()} "
                        sqlJ += "WHERE PandaID=:cr_PandaID AND lockedBy=:cr_lockedBy "
                        # update job
                        varMap = jobSpec.values_map(only_changed=True)
                        varMap[":cr_PandaID"] = jobSpec.PandaID
                        varMap[":cr_lockedBy"] = locked_by
                        self.execute(sqlJ, varMap)
                    if jobSpec.subStatus in ["submitted", "running"]:
                        # values for job/worker mapping
                        jwRelation = JobWorkerRelationSpec()
                        jwRelation.PandaID = jobSpec.PandaID
                        jwRelation.workerID = workspec.workerID
                        varMap = jwRelation.values_list()
                        varMapsR.append(varMap)
            # insert job/worker mapping
            if len(varMapsR) > 0:
                self.executemany(sqlR, varMapsR)
            # commit
            self.commit()
            # return
            return True
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(tmpLog)
            # return
            return False

    # insert workers
    def insert_workers(self, workspec_list, locked_by):
        tmpLog = core_utils.make_logger(_logger, f"locked_by={locked_by}", method_name="insert_workers")
        try:
            tmpLog.debug("start")
            timeNow = core_utils.naive_utcnow()
            # sql to insert a worker
            sqlI = f"INSERT INTO {workTableName} ({WorkSpec.column_names()}) "
            sqlI += WorkSpec.bind_values_expression()
            for workSpec in workspec_list:
                tmpWorkSpec = copy.copy(workSpec)
                # insert worker if new
                if not tmpWorkSpec.isNew:
                    continue
                tmpWorkSpec.modificationTime = timeNow
                tmpWorkSpec.status = WorkSpec.ST_pending
                varMap = tmpWorkSpec.values_list()
                self.execute(sqlI, varMap)
            # commit
            self.commit()
            # return
            return True
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(tmpLog)
            # return
            return False

    # get workers to monitor
    def get_workers_to_update(self, max_workers, check_interval, lock_interval, locked_by):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_workers_to_update")
            tmpLog.debug("start")
            # sql to get workers
            sqlW = f"SELECT workerID,configID,mapType FROM {workTableName} "
            sqlW += "WHERE status IN (:st_submitted,:st_running,:st_idle) "
            sqlW += "AND ((modificationTime<:lockTimeLimit AND lockedBy IS NOT NULL) "
            sqlW += "OR (modificationTime<:checkTimeLimit AND lockedBy IS NULL)) "
            sqlW += f"ORDER BY modificationTime LIMIT {max_workers} "
            # sql to lock worker without time check
            sqlL = f"UPDATE {workTableName} SET modificationTime=:timeNow,lockedBy=:lockedBy "
            sqlL += "WHERE workerID=:workerID "
            # sql to update modificationTime
            sqlLM = f"UPDATE {workTableName} SET modificationTime=:timeNow "
            sqlLM += "WHERE workerID=:workerID "
            # sql to lock worker with time check
            sqlLT = f"UPDATE {workTableName} SET modificationTime=:timeNow,lockedBy=:lockedBy "
            sqlLT += "WHERE workerID=:workerID "
            sqlLT += "AND status IN (:st_submitted,:st_running,:st_idle) "
            sqlLT += "AND ((modificationTime<:lockTimeLimit AND lockedBy IS NOT NULL) "
            sqlLT += "OR (modificationTime<:checkTimeLimit AND lockedBy IS NULL)) "
            # sql to get associated workerIDs
            sqlA = "SELECT t.workerID FROM {0} t, {0} s, {1} w ".format(jobWorkerTableName, workTableName)
            sqlA += "WHERE s.PandaID=t.PandaID AND s.workerID=:workerID "
            sqlA += "AND w.workerID=t.workerID AND w.status IN (:st_submitted,:st_running,:st_idle) "
            # sql to get associated workers
            sqlG = f"SELECT {WorkSpec.column_names()} FROM {workTableName} "
            sqlG += "WHERE workerID=:workerID "
            # sql to get associated PandaIDs
            sqlP = f"SELECT PandaID FROM {jobWorkerTableName} "
            sqlP += "WHERE workerID=:workerID "
            # get workerIDs
            timeNow = core_utils.naive_utcnow()
            lockTimeLimit = timeNow - datetime.timedelta(seconds=lock_interval)
            checkTimeLimit = timeNow - datetime.timedelta(seconds=check_interval)
            varMap = dict()
            varMap[":st_submitted"] = WorkSpec.ST_submitted
            varMap[":st_running"] = WorkSpec.ST_running
            varMap[":st_idle"] = WorkSpec.ST_idle
            varMap[":lockTimeLimit"] = lockTimeLimit
            varMap[":checkTimeLimit"] = checkTimeLimit
            self.execute(sqlW, varMap)
            resW = self.cur.fetchall()
            tmpWorkers = set()
            for workerID, configID, mapType in resW:
                # ignore configID
                if not core_utils.dynamic_plugin_change():
                    configID = None
                tmpWorkers.add((workerID, configID, mapType))
            checkedIDs = set()
            retVal = {}
            for workerID, configID, mapType in tmpWorkers:
                # skip
                if workerID in checkedIDs:
                    continue
                # get associated workerIDs
                varMap = dict()
                varMap[":workerID"] = workerID
                varMap[":st_submitted"] = WorkSpec.ST_submitted
                varMap[":st_running"] = WorkSpec.ST_running
                varMap[":st_idle"] = WorkSpec.ST_idle
                self.execute(sqlA, varMap)
                resA = self.cur.fetchall()
                workerIDtoScan = set()
                for (tmpWorkID,) in resA:
                    workerIDtoScan.add(tmpWorkID)
                # add original ID just in case since no relation when job is not yet bound
                workerIDtoScan.add(workerID)
                # use only the largest worker to avoid updating the same worker set concurrently
                if mapType == WorkSpec.MT_MultiWorkers:
                    if workerID != min(workerIDtoScan):
                        # update modification time
                        varMap = dict()
                        varMap[":workerID"] = workerID
                        varMap[":timeNow"] = timeNow
                        self.execute(sqlLM, varMap)
                        # commit
                        self.commit()
                        continue
                # lock worker
                varMap = dict()
                varMap[":workerID"] = workerID
                varMap[":lockedBy"] = locked_by
                varMap[":timeNow"] = timeNow
                varMap[":st_submitted"] = WorkSpec.ST_submitted
                varMap[":st_running"] = WorkSpec.ST_running
                varMap[":st_idle"] = WorkSpec.ST_idle
                varMap[":lockTimeLimit"] = lockTimeLimit
                varMap[":checkTimeLimit"] = checkTimeLimit
                self.execute(sqlLT, varMap)
                nRow = self.cur.rowcount
                # commit
                self.commit()
                # skip if not locked
                if nRow == 0:
                    continue
                # get workers
                queueName = None
                workersList = []
                for tmpWorkID in workerIDtoScan:
                    checkedIDs.add(tmpWorkID)
                    # get worker
                    varMap = dict()
                    varMap[":workerID"] = tmpWorkID
                    self.execute(sqlG, varMap)
                    resG = self.cur.fetchone()
                    workSpec = WorkSpec()
                    workSpec.pack(resG)
                    if queueName is None:
                        queueName = workSpec.computingSite
                    workersList.append(workSpec)
                    # get associated PandaIDs
                    varMap = dict()
                    varMap[":workerID"] = tmpWorkID
                    self.execute(sqlP, varMap)
                    resP = self.cur.fetchall()
                    workSpec.pandaid_list = []
                    for (tmpPandaID,) in resP:
                        workSpec.pandaid_list.append(tmpPandaID)
                    if len(workSpec.pandaid_list) > 0:
                        workSpec.nJobs = len(workSpec.pandaid_list)
                    # lock worker
                    if tmpWorkID != workerID:
                        varMap = dict()
                        varMap[":workerID"] = tmpWorkID
                        varMap[":lockedBy"] = locked_by
                        varMap[":timeNow"] = timeNow
                        self.execute(sqlL, varMap)
                    workSpec.lockedBy = locked_by
                    workSpec.force_not_update("lockedBy")
                # commit
                self.commit()
                # add
                if queueName is not None:
                    retVal.setdefault(queueName, dict())
                    retVal[queueName].setdefault(configID, [])
                    retVal[queueName][configID].append(workersList)
            tmpLog.debug(f"got {str(retVal)}")
            return retVal
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # get workers to propagate
    def get_workers_to_propagate(self, max_workers, check_interval):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_workers_to_propagate")
            tmpLog.debug("start")
            # sql to get worker IDs
            sqlW = f"SELECT workerID FROM {workTableName} "
            sqlW += "WHERE lastUpdate IS NOT NULL AND lastUpdate<:checkTimeLimit "
            sqlW += "ORDER BY lastUpdate "
            # sql to lock worker
            sqlL = f"UPDATE {workTableName} SET lastUpdate=:timeNow "
            sqlL += "WHERE lastUpdate IS NOT NULL AND lastUpdate<:checkTimeLimit "
            sqlL += "AND workerID=:workerID "
            # sql to get associated PandaIDs
            sqlA = f"SELECT PandaID FROM {jobWorkerTableName} "
            sqlA += "WHERE workerID=:workerID "
            # sql to get workers
            sqlG = f"SELECT {WorkSpec.column_names()} FROM {workTableName} "
            sqlG += "WHERE workerID=:workerID "
            timeNow = core_utils.naive_utcnow()
            timeLimit = timeNow - datetime.timedelta(seconds=check_interval)
            # get workerIDs
            varMap = dict()
            varMap[":checkTimeLimit"] = timeLimit
            self.execute(sqlW, varMap)
            resW = self.cur.fetchall()
            tmpWorkers = []
            for (workerID,) in resW:
                tmpWorkers.append(workerID)
            # partially randomize to increase hit rate
            nWorkers = int(max_workers * 0.2)
            subTmpWorkers = list(tmpWorkers[nWorkers:])
            random.shuffle(subTmpWorkers)
            tmpWorkers = tmpWorkers[:nWorkers] + subTmpWorkers
            tmpWorkers = tmpWorkers[:max_workers]
            retVal = []
            for workerID in tmpWorkers:
                # lock worker
                varMap = dict()
                varMap[":workerID"] = workerID
                varMap[":timeNow"] = timeNow
                varMap[":checkTimeLimit"] = timeLimit
                self.execute(sqlL, varMap)
                nRow = self.cur.rowcount
                if nRow > 0:
                    # get worker
                    varMap = dict()
                    varMap[":workerID"] = workerID
                    self.execute(sqlG, varMap)
                    resG = self.cur.fetchone()
                    workSpec = WorkSpec()
                    workSpec.pack(resG)
                    retVal.append(workSpec)
                    # get associated PandaIDs
                    varMap = dict()
                    varMap[":workerID"] = workerID
                    self.execute(sqlA, varMap)
                    resA = self.cur.fetchall()
                    workSpec.pandaid_list = []
                    for (pandaID,) in resA:
                        workSpec.pandaid_list.append(pandaID)
                # commit
                self.commit()
            tmpLog.debug(f"got {len(retVal)} workers")
            return retVal
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # get workers to feed events
    def get_workers_to_feed_events(self, max_workers, lock_interval, locked_by):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_workers_to_feed_events")
            tmpLog.debug("start")
            # sql to get workers
            sqlW = f"SELECT workerID, status FROM {workTableName} "
            sqlW += "WHERE eventsRequest=:eventsRequest AND status IN (:status1,:status2) "
            sqlW += "AND (eventFeedTime IS NULL OR eventFeedTime<:lockTimeLimit) "
            sqlW += f"ORDER BY eventFeedTime LIMIT {max_workers} "
            # sql to lock worker
            sqlL = f"UPDATE {workTableName} SET eventFeedTime=:timeNow,eventFeedLock=:lockedBy "
            sqlL += "WHERE eventsRequest=:eventsRequest AND status=:status "
            sqlL += "AND (eventFeedTime IS NULL OR eventFeedTime<:lockTimeLimit) "
            sqlL += "AND workerID=:workerID "
            # sql to get associated workers
            sqlG = f"SELECT {WorkSpec.column_names()} FROM {workTableName} "
            sqlG += "WHERE workerID=:workerID "
            # get workerIDs
            timeNow = core_utils.naive_utcnow()
            lockTimeLimit = timeNow - datetime.timedelta(seconds=lock_interval)
            varMap = dict()
            varMap[":status1"] = WorkSpec.ST_running
            varMap[":status2"] = WorkSpec.ST_submitted
            varMap[":eventsRequest"] = WorkSpec.EV_requestEvents
            varMap[":lockTimeLimit"] = lockTimeLimit
            self.execute(sqlW, varMap)
            resW = self.cur.fetchall()
            tmpWorkers = dict()
            for tmpWorkerID, tmpWorkStatus in resW:
                tmpWorkers[tmpWorkerID] = tmpWorkStatus
            retVal = {}
            for workerID, workStatus in tmpWorkers.items():
                # lock worker
                varMap = dict()
                varMap[":workerID"] = workerID
                varMap[":timeNow"] = timeNow
                varMap[":status"] = workStatus
                varMap[":eventsRequest"] = WorkSpec.EV_requestEvents
                varMap[":lockTimeLimit"] = lockTimeLimit
                varMap[":lockedBy"] = locked_by
                self.execute(sqlL, varMap)
                nRow = self.cur.rowcount
                # commit
                self.commit()
                # skip if not locked
                if nRow == 0:
                    continue
                # get worker
                varMap = dict()
                varMap[":workerID"] = workerID
                self.execute(sqlG, varMap)
                resG = self.cur.fetchone()
                workSpec = WorkSpec()
                workSpec.pack(resG)
                if workSpec.computingSite not in retVal:
                    retVal[workSpec.computingSite] = []
                retVal[workSpec.computingSite].append(workSpec)
            tmpLog.debug(f"got {len(retVal)} workers")
            return retVal
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # get ready workers
    def get_ready_workers(self, queue_name, n_ready):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"queue={queue_name}", method_name="get_ready_workers")
            tmpLog.debug("start")
            # sql to get workers
            sqlG = f"SELECT {WorkSpec.column_names()} FROM {workTableName} "
            sqlG += "WHERE computingSite=:queueName AND (status=:status_ready OR (status=:status_running "
            sqlG += "AND nJobsToReFill IS NOT NULL AND nJobsToReFill>0)) "
            sqlG += f"ORDER BY modificationTime LIMIT {n_ready} "
            # sql to get associated PandaIDs
            sqlP = f"SELECT COUNT(*) cnt FROM {jobWorkerTableName} "
            sqlP += "WHERE workerID=:workerID "
            # get workers
            varMap = dict()
            varMap[":status_ready"] = WorkSpec.ST_ready
            varMap[":status_running"] = WorkSpec.ST_running
            varMap[":queueName"] = queue_name
            self.execute(sqlG, varMap)
            resList = self.cur.fetchall()
            retVal = []
            for res in resList:
                workSpec = WorkSpec()
                workSpec.pack(res)
                # get number of jobs
                varMap = dict()
                varMap[":workerID"] = workSpec.workerID
                self.execute(sqlP, varMap)
                resP = self.cur.fetchone()
                if resP is not None and resP[0] > 0:
                    workSpec.nJobs = resP[0]
                retVal.append(workSpec)
            # commit
            self.commit()
            tmpLog.debug(f"got {str(retVal)}")
            return retVal
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    # get a worker
    def get_worker_with_id(self, worker_id):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"workerID={worker_id}", method_name="get_worker_with_id")
            tmpLog.debug("start")
            # sql to get a worker
            sqlG = f"SELECT {WorkSpec.column_names()} FROM {workTableName} "
            sqlG += "WHERE workerID=:workerID "
            # get a worker
            varMap = dict()
            varMap[":workerID"] = worker_id
            self.execute(sqlG, varMap)
            res = self.cur.fetchone()
            workSpec = WorkSpec()
            workSpec.pack(res)
            # commit
            self.commit()
            tmpLog.debug("got")
            return workSpec
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # get workers to kill
    def get_workers_to_kill(self, max_workers, check_interval):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_workers_to_kill")
            tmpLog.debug("start")
            # sql to get worker IDs
            sqlW = (
                f"SELECT workerID,status,configID FROM {workTableName} "
                "WHERE killTime IS NOT NULL AND killTime<:checkTimeLimit "
                f"ORDER BY killTime LIMIT {max_workers} "
            )

            # sql to lock or release worker
            sqlL = f"UPDATE {workTableName} SET killTime=:setTime WHERE workerID=:workerID AND killTime IS NOT NULL AND killTime<:checkTimeLimit "

            # sql to get workers
            sqlG = f"SELECT {WorkSpec.column_names()} FROM {workTableName} WHERE workerID=:workerID "
            timeNow = core_utils.naive_utcnow()
            timeLimit = timeNow - datetime.timedelta(seconds=check_interval)

            # get workerIDs
            varMap = dict()
            varMap[":checkTimeLimit"] = timeLimit
            self.execute(sqlW, varMap)
            resW = self.cur.fetchall()
            retVal = dict()
            for workerID, workerStatus, configID in resW:
                # ignore configID
                if not core_utils.dynamic_plugin_change():
                    configID = None
                # lock or release worker
                varMap = dict()
                varMap[":workerID"] = workerID
                varMap[":checkTimeLimit"] = timeLimit
                if workerStatus in (WorkSpec.ST_cancelled, WorkSpec.ST_failed, WorkSpec.ST_finished):
                    # release
                    varMap[":setTime"] = None
                else:
                    # lock
                    varMap[":setTime"] = timeNow
                self.execute(sqlL, varMap)
                # get worker
                nRow = self.cur.rowcount
                if nRow == 1 and varMap[":setTime"] is not None:
                    varMap = dict()
                    varMap[":workerID"] = workerID
                    self.execute(sqlG, varMap)
                    resG = self.cur.fetchone()
                    workSpec = WorkSpec()
                    workSpec.pack(resG)
                    queueName = workSpec.computingSite
                    retVal.setdefault(queueName, dict())
                    retVal[queueName].setdefault(configID, [])
                    retVal[queueName][configID].append(workSpec)
                # commit
                self.commit()
            tmpLog.debug(f"got {len(retVal)} workers")
            return retVal
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # send kill command to workers associated to a job
    def mark_workers_to_kill_by_pandaid(self, panda_id, delay_seconds=None):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"PandaID={panda_id}", method_name="mark_workers_to_kill_by_pandaid")
            tmpLog.debug("start")
            # sql to set killTime
            sqlL = f"UPDATE {workTableName} SET killTime=:setTime WHERE workerID=:workerID AND killTime IS NULL AND NOT status IN (:st1,:st2,:st3) "

            # sql to get associated workers
            sqlA = f"SELECT workerID FROM {jobWorkerTableName} WHERE PandaID=:pandaID "
            # set time to trigger sweeper
            if delay_seconds is None:
                # set a past time to trigger sweeper immediately
                setTime = core_utils.naive_utcnow() - datetime.timedelta(hours=6)
            else:
                # set a future time to delay trigger
                setTime = core_utils.naive_utcnow() + datetime.timedelta(seconds=delay_seconds)
            # get workers
            varMap = dict()
            varMap[":pandaID"] = panda_id
            self.execute(sqlA, varMap)
            resA = self.cur.fetchall()
            nRow = 0
            for (workerID,) in resA:
                # set killTime
                varMap = dict()
                varMap[":workerID"] = workerID
                varMap[":setTime"] = setTime
                varMap[":st1"] = WorkSpec.ST_finished
                varMap[":st2"] = WorkSpec.ST_failed
                varMap[":st3"] = WorkSpec.ST_cancelled
                self.execute(sqlL, varMap)
                nRow += self.cur.rowcount
            # commit
            self.commit()
            tmpLog.debug(f"set killTime to {nRow} workers")
            return nRow
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # send kill command to workers
    def mark_workers_to_kill_by_workerids(self, worker_ids, delay_seconds=None):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="mark_workers_to_kill_by_workerids")
            tmpLog.debug("start")
            # sql to set killTime
            sqlL = f"UPDATE {workTableName} SET killTime=:setTime "
            sqlL += "WHERE workerID=:workerID AND killTime IS NULL AND NOT status IN (:st1,:st2,:st3) "
            # set time to trigger sweeper
            if delay_seconds is None:
                # set a past time to trigger sweeper immediately
                setTime = core_utils.naive_utcnow() - datetime.timedelta(hours=6)
            else:
                # set a future time to delay trigger
                setTime = core_utils.naive_utcnow() + datetime.timedelta(seconds=delay_seconds)
            varMaps = []
            for worker_id in worker_ids:
                varMap = dict()
                varMap[":workerID"] = worker_id
                varMap[":setTime"] = setTime
                varMap[":st1"] = WorkSpec.ST_finished
                varMap[":st2"] = WorkSpec.ST_failed
                varMap[":st3"] = WorkSpec.ST_cancelled
                varMaps.append(varMap)
            self.executemany(sqlL, varMaps)
            nRow = self.cur.rowcount
            # commit
            self.commit()
            tmpLog.debug(f"set killTime with {nRow}")
            return nRow
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # get workers for cleanup
    def get_workers_for_cleanup(self, max_workers, status_timeout_map):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_workers_for_cleanup")
            tmpLog.debug("start")
            # sql to get worker IDs
            timeNow = core_utils.naive_utcnow()
            modTimeLimit = timeNow - datetime.timedelta(minutes=60)
            varMap = dict()
            varMap[":timeLimit"] = modTimeLimit
            sqlW = f"SELECT workerID, configID FROM {workTableName} "
            sqlW += "WHERE lastUpdate IS NULL AND ("
            for tmpStatus, tmpTimeout in status_timeout_map.items():
                tmpStatusKey = f":status_{tmpStatus}"
                tmpTimeoutKey = f":timeLimit_{tmpStatus}"
                sqlW += f"(status={tmpStatusKey} AND endTime<={tmpTimeoutKey}) OR "
                varMap[tmpStatusKey] = tmpStatus
                varMap[tmpTimeoutKey] = timeNow - datetime.timedelta(hours=tmpTimeout)
            sqlW = sqlW[:-4]
            sqlW += ") "
            sqlW += "AND modificationTime<:timeLimit "
            sqlW += f"ORDER BY modificationTime LIMIT {max_workers} "
            # sql to lock or release worker
            sqlL = f"UPDATE {workTableName} SET modificationTime=:setTime "
            sqlL += "WHERE workerID=:workerID AND modificationTime<:timeLimit "
            # sql to check associated jobs
            sqlA = f"SELECT COUNT(*) cnt FROM {jobTableName} j, {jobWorkerTableName} r "
            sqlA += "WHERE j.PandaID=r.PandaID AND r.workerID=:workerID "
            sqlA += "AND propagatorTime IS NOT NULL "
            # sql to get workers
            sqlG = f"SELECT {WorkSpec.column_names()} FROM {workTableName} "
            sqlG += "WHERE workerID=:workerID "
            # sql to get PandaIDs
            sqlP = f"SELECT j.PandaID FROM {jobTableName} j, {jobWorkerTableName} r "
            sqlP += "WHERE j.PandaID=r.PandaID AND r.workerID=:workerID "
            # sql to get jobs
            sqlJ = f"SELECT {JobSpec.column_names()} FROM {jobTableName} "
            sqlJ += "WHERE PandaID=:PandaID "
            # sql to get files
            sqlF = f"SELECT {FileSpec.column_names()} FROM {fileTableName} "
            sqlF += "WHERE PandaID=:PandaID "
            # sql to get files not to be deleted. b.todelete is not used to use index on b.lfn
            sqlD = "SELECT b.lfn,b.todelete  FROM {0} a, {0} b ".format(fileTableName)
            sqlD += "WHERE a.PandaID=:PandaID AND a.fileType IN (:fileType1,:fileType2) AND b.lfn=a.lfn "
            # get workerIDs
            timeNow = core_utils.naive_utcnow()
            self.execute(sqlW, varMap)
            resW = self.cur.fetchall()
            retVal = dict()
            iWorkers = 0
            for workerID, configID in resW:
                # lock worker
                varMap = dict()
                varMap[":workerID"] = workerID
                varMap[":setTime"] = timeNow
                varMap[":timeLimit"] = modTimeLimit
                self.execute(sqlL, varMap)
                # commit
                self.commit()
                if self.cur.rowcount == 0:
                    continue
                # ignore configID
                if not core_utils.dynamic_plugin_change():
                    configID = None
                # check associated jobs
                varMap = dict()
                varMap[":workerID"] = workerID
                self.execute(sqlA, varMap)
                (nActJobs,) = self.cur.fetchone()
                # cleanup when there is no active job
                if nActJobs == 0:
                    # get worker
                    varMap = dict()
                    varMap[":workerID"] = workerID
                    self.execute(sqlG, varMap)
                    resG = self.cur.fetchone()
                    workSpec = WorkSpec()
                    workSpec.pack(resG)
                    queueName = workSpec.computingSite
                    retVal.setdefault(queueName, dict())
                    retVal[queueName].setdefault(configID, [])
                    retVal[queueName][configID].append(workSpec)
                    # get jobs
                    jobSpecs = []
                    checkedLFNs = set()
                    keepLFNs = set()
                    varMap = dict()
                    varMap[":workerID"] = workerID
                    self.execute(sqlP, varMap)
                    resP = self.cur.fetchall()
                    for (pandaID,) in resP:
                        varMap = dict()
                        varMap[":PandaID"] = pandaID
                        self.execute(sqlJ, varMap)
                        resJ = self.cur.fetchone()
                        jobSpec = JobSpec()
                        jobSpec.pack(resJ)
                        jobSpecs.append(jobSpec)
                        # get LFNs not to be deleted
                        varMap = dict()
                        varMap[":PandaID"] = pandaID
                        varMap[":fileType1"] = "input"
                        varMap[":fileType2"] = FileSpec.AUX_INPUT
                        self.execute(sqlD, varMap)
                        resDs = self.cur.fetchall()
                        for tmpLFN, tmpTodelete in resDs:
                            if tmpTodelete == 0:
                                keepLFNs.add(tmpLFN)
                        # get files to be deleted
                        varMap = dict()
                        varMap[":PandaID"] = jobSpec.PandaID
                        self.execute(sqlF, varMap)
                        resFs = self.cur.fetchall()
                        for resF in resFs:
                            fileSpec = FileSpec()
                            fileSpec.pack(resF)
                            # skip if already checked
                            if fileSpec.lfn in checkedLFNs:
                                continue
                            checkedLFNs.add(fileSpec.lfn)
                            # check if it is ready to delete
                            if fileSpec.lfn not in keepLFNs:
                                jobSpec.add_file(fileSpec)
                    workSpec.set_jobspec_list(jobSpecs)
                    iWorkers += 1
            tmpLog.debug(f"got {iWorkers} workers")
            return retVal
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # delete a worker
    def delete_worker(self, worker_id):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"workerID={worker_id}", method_name="delete_worker")
            tmpLog.debug("start")
            # sql to get jobs
            sqlJ = f"SELECT PandaID FROM {jobWorkerTableName} "
            sqlJ += "WHERE workerID=:workerID "
            # sql to delete job
            sqlDJ = f"DELETE FROM {jobTableName} "
            sqlDJ += "WHERE PandaID=:PandaID "
            # sql to delete files
            sqlDF = f"DELETE FROM {fileTableName} "
            sqlDF += "WHERE PandaID=:PandaID "
            # sql to delete events
            sqlDE = f"DELETE FROM {eventTableName} "
            sqlDE += "WHERE PandaID=:PandaID "
            # sql to delete relations
            sqlDR = f"DELETE FROM {jobWorkerTableName} "
            sqlDR += "WHERE PandaID=:PandaID "
            # sql to delete worker
            sqlDW = f"DELETE FROM {workTableName} "
            sqlDW += "WHERE workerID=:workerID "
            # get jobs
            varMap = dict()
            varMap[":workerID"] = worker_id
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
                self.execute(sqlDR, varMap)
            # delete worker
            varMap = dict()
            varMap[":workerID"] = worker_id
            self.execute(sqlDW, varMap)
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

    # get the number of missed worker
    def get_num_missed_workers(self, queue_name, criteria):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"queue={queue_name}", method_name="get_num_missed_workers")
            tmpLog.debug("start")
            # get worker stats
            sqlW = "SELECT COUNT(*) cnt "
            sqlW += f"FROM {workTableName} wt, {pandaQueueTableName} pq "
            sqlW += "WHERE wt.computingSite=pq.queueName AND wt.status=:status "
            # get worker stats
            varMap = dict()
            for attr, val in criteria.items():
                if attr == "timeLimit":
                    sqlW += "AND wt.submitTime>:timeLimit "
                    varMap[":timeLimit"] = val
                elif attr in ["siteName"]:
                    sqlW += "AND pq.{0}=:{0} ".format(attr)
                    varMap[f":{attr}"] = val
                elif attr in ["computingSite", "computingElement"]:
                    sqlW += "AND wt.{0}=:{0} ".format(attr)
                    varMap[f":{attr}"] = val
            varMap[":status"] = "missed"
            self.execute(sqlW, varMap)
            resW = self.cur.fetchone()
            if resW is None:
                nMissed = 0
            else:
                (nMissed,) = resW
            # commit
            self.commit()
            tmpLog.debug(f"got nMissed={nMissed} for {str(criteria)}")
            return nMissed
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return 0

    # get a worker
    def get_workers_with_job_id(self, panda_id, use_commit=True):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"pandaID={panda_id}", method_name="get_workers_with_job_id")
            tmpLog.debug("start")
            # sql to get workerIDs
            sqlW = f"SELECT workerID FROM {jobWorkerTableName} WHERE PandaID=:PandaID "
            sqlW += "ORDER BY workerID "
            # sql to get a worker
            sqlG = f"SELECT {WorkSpec.column_names(slim=True)} FROM {workTableName} "
            sqlG += "WHERE workerID=:workerID "
            # get workerIDs
            varMap = dict()
            varMap[":PandaID"] = panda_id
            self.execute(sqlW, varMap)
            retList = []
            for (worker_id,) in self.cur.fetchall():
                # get a worker
                varMap = dict()
                varMap[":workerID"] = worker_id
                self.execute(sqlG, varMap)
                res = self.cur.fetchone()
                workSpec = WorkSpec()
                workSpec.pack(res, slim=True)
                retList.append(workSpec)
            # commit
            if use_commit:
                self.commit()
            tmpLog.debug(f"got {len(retList)} workers")
            return retList
        except Exception:
            # roll back
            if use_commit:
                self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    # get iterator of active workers to monitor fifo
    def get_active_workers(self, n_workers, seconds_ago=0):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_active_workers")
            tmpLog.debug("start")
            # sql to get workers
            sqlW = f"SELECT {WorkSpec.column_names()} FROM {workTableName} "
            sqlW += "WHERE status IN (:st_submitted,:st_running,:st_idle) "
            sqlW += "AND modificationTime<:timeLimit "
            sqlW += f"ORDER BY modificationTime,computingSite LIMIT {n_workers} "
            # sql to get jobs
            sqlJ = f"SELECT j.{JobSpec.column_names()} FROM {jobWorkerTableName} jw, {jobTableName} j "
            sqlJ += "WHERE j.PandaID=jw.PandaID AND jw.workerID=:workerID "
            # parameter map
            varMap = dict()
            varMap[":timeLimit"] = core_utils.naive_utcnow() - datetime.timedelta(seconds=seconds_ago)
            varMap[":st_submitted"] = WorkSpec.ST_submitted
            varMap[":st_running"] = WorkSpec.ST_running
            varMap[":st_idle"] = WorkSpec.ST_idle
            self.execute(sqlW, varMap)
            resW = self.cur.fetchall()

            def _get_workspec_from_record(rec):
                workspec = WorkSpec()
                workspec.pack(rec)
                jobspec_list = []
                workspec.pandaid_list = []
                varMap = dict()
                varMap[":workerID"] = workspec.workerID
                self.execute(sqlJ, varMap)
                resJ = self.cur.fetchall()
                for one_job in resJ:
                    jobspec = JobSpec()
                    jobspec.pack(one_job)
                    jobspec_list.append(jobspec)
                    workspec.pandaid_list.append(jobspec.PandaID)
                workspec.set_jobspec_list(jobspec_list)
                return workspec

            retVal = map(_get_workspec_from_record, resW)
            tmpLog.debug(f"got {len(resW)} workers")
            return retVal
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # lock workers for specific thread
    def lock_workers(self, worker_id_list, lock_interval):
        try:
            timeNow = core_utils.naive_utcnow()
            lockTimeLimit = timeNow - datetime.timedelta(seconds=lock_interval)
            retVal = True
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="lock_worker")
            tmpLog.debug("start")
            # loop
            for worker_id, attrs in worker_id_list.items():
                varMap = dict()
                varMap[":workerID"] = worker_id
                varMap[":timeNow"] = timeNow
                varMap[":lockTimeLimit"] = lockTimeLimit
                varMap[":st1"] = WorkSpec.ST_cancelled
                varMap[":st2"] = WorkSpec.ST_finished
                varMap[":st3"] = WorkSpec.ST_failed
                varMap[":st4"] = WorkSpec.ST_missed
                # extract lockedBy
                varMap[":lockedBy"] = attrs["lockedBy"]
                if attrs["lockedBy"] is None:
                    del attrs["lockedBy"]
                # sql to lock worker
                sqlL = f"UPDATE {workTableName} SET modificationTime=:timeNow"
                for attrKey, attrVal in attrs.items():
                    sqlL += ",{0}=:{0}".format(attrKey)
                    varMap[f":{attrKey}"] = attrVal
                sqlL += " WHERE workerID=:workerID AND (lockedBy IS NULL "
                sqlL += "OR (modificationTime<:lockTimeLimit AND lockedBy IS NOT NULL)) "
                sqlL += "AND (status NOT IN (:st1,:st2,:st3,:st4)) "
                # lock worker
                self.execute(sqlL, varMap)
                nRow = self.cur.rowcount
                tmpLog.debug(f"done with {nRow}")
                # false if failed to lock
                if nRow == 0:
                    retVal = False
                # commit
                self.commit()
            # return
            return retVal
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # lock worker again to feed events
    def lock_worker_again_to_feed_events(self, worker_id, locked_by):
        try:
            tmpLog = core_utils.make_logger(_logger, f"workerID={worker_id}", method_name="lock_worker_again_to_feed_events")
            tmpLog.debug(f"start id={locked_by}")
            # check lock
            sqlC = f"SELECT eventFeedLock,eventFeedTime FROM {workTableName} "
            sqlC += "WHERE workerID=:workerID "
            sqlC += "FOR UPDATE "
            varMap = dict()
            varMap[":workerID"] = worker_id
            self.execute(sqlC, varMap)
            resC = self.cur.fetchone()
            if resC is None:
                retVal = False
                tmpLog.debug("not found")
            else:
                oldLockedBy, oldLockedTime = resC
                if oldLockedBy != locked_by:
                    tmpLog.debug(f"locked by another {oldLockedBy} at {oldLockedTime}")
                    retVal = False
                else:
                    # update locked time
                    sqlU = f"UPDATE {workTableName} SET eventFeedTime=:timeNow WHERE workerID=:workerID "
                    varMap = dict()
                    varMap[":workerID"] = worker_id
                    varMap[":timeNow"] = core_utils.naive_utcnow()
                    self.execute(sqlU, varMap)
                    retVal = True
            # commit
            self.commit()
            tmpLog.debug(f"done with {retVal}")
            # return
            return retVal
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # get workers via workerID
    def get_workers_from_ids(self, ids):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_workers_from_ids")
            tmpLog.debug("start")
            # sql to get workers
            sqlW = (
                "SELECT workerID,configID,mapType FROM {workTableName} " "WHERE workerID IN ({ids_str}) " "AND status IN (:st_submitted,:st_running,:st_idle) "
            ).format(workTableName=workTableName, ids_str=",".join([str(_) for _ in ids]))
            # sql to get associated workerIDs
            sqlA = (
                "SELECT t.workerID FROM {jobWorkerTableName} t, {jobWorkerTableName} s, {workTableName} w "
                "WHERE s.PandaID=t.PandaID AND s.workerID=:workerID "
                "AND w.workerID=t.workerID AND w.status IN (:st_submitted,:st_running,:st_idle) "
            ).format(jobWorkerTableName=jobWorkerTableName, workTableName=workTableName)
            # sql to get associated workers
            sqlG = f"SELECT {WorkSpec.column_names()} FROM {workTableName} WHERE workerID=:workerID "
            # sql to get associated PandaIDs
            sqlP = f"SELECT PandaID FROM {jobWorkerTableName} WHERE workerID=:workerID "
            # get workerIDs
            timeNow = core_utils.naive_utcnow()
            varMap = dict()
            varMap[":st_submitted"] = WorkSpec.ST_submitted
            varMap[":st_running"] = WorkSpec.ST_running
            varMap[":st_idle"] = WorkSpec.ST_idle
            self.execute(sqlW, varMap)
            resW = self.cur.fetchall()
            tmpWorkers = set()
            for workerID, configID, mapType in resW:
                # ignore configID
                if not core_utils.dynamic_plugin_change():
                    configID = None
                tmpWorkers.add((workerID, configID, mapType))
            checkedIDs = set()
            retVal = {}
            for workerID, configID, mapType in tmpWorkers:
                # skip
                if workerID in checkedIDs:
                    continue
                # get associated workerIDs
                varMap = dict()
                varMap[":workerID"] = workerID
                varMap[":st_submitted"] = WorkSpec.ST_submitted
                varMap[":st_running"] = WorkSpec.ST_running
                varMap[":st_idle"] = WorkSpec.ST_idle
                self.execute(sqlA, varMap)
                resA = self.cur.fetchall()
                workerIDtoScan = set()
                for (tmpWorkID,) in resA:
                    workerIDtoScan.add(tmpWorkID)
                # add original ID just in case since no relation when job is not yet bound
                workerIDtoScan.add(workerID)
                # use only the largest worker to avoid updating the same worker set concurrently
                if mapType == WorkSpec.MT_MultiWorkers:
                    if workerID != min(workerIDtoScan):
                        continue
                # get workers
                queueName = None
                workersList = []
                for tmpWorkID in workerIDtoScan:
                    checkedIDs.add(tmpWorkID)
                    # get worker
                    varMap = dict()
                    varMap[":workerID"] = tmpWorkID
                    self.execute(sqlG, varMap)
                    resG = self.cur.fetchone()
                    workSpec = WorkSpec()
                    workSpec.pack(resG)
                    if queueName is None:
                        queueName = workSpec.computingSite
                    workersList.append(workSpec)
                    # get associated PandaIDs
                    varMap = dict()
                    varMap[":workerID"] = tmpWorkID
                    self.execute(sqlP, varMap)
                    resP = self.cur.fetchall()
                    workSpec.pandaid_list = []
                    for (tmpPandaID,) in resP:
                        workSpec.pandaid_list.append(tmpPandaID)
                    if len(workSpec.pandaid_list) > 0:
                        workSpec.nJobs = len(workSpec.pandaid_list)
                # commit
                self.commit()
                # add
                if queueName is not None:
                    retVal.setdefault(queueName, dict())
                    retVal[queueName].setdefault(configID, [])
                    retVal[queueName][configID].append(workersList)
            tmpLog.debug(f"got {str(retVal)}")
            return retVal
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # send kill command to workers by query
    def mark_workers_to_kill_by_query(self, params, delay_seconds=None):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="mark_workers_to_kill_by_query")
            tmpLog.debug("start")
            # sql to set killTime
            sqlL = f"UPDATE {workTableName} SET killTime=:setTime "
            sqlL += "WHERE workerID=:workerID AND killTime IS NULL AND NOT status IN (:st1,:st2,:st3) "
            # sql to get workers
            constraints_query_string_list = []
            tmp_varMap = {}
            constraint_map = {
                "status": params.get("status", [WorkSpec.ST_submitted]),
                "computingSite": params.get("computingSite", []),
                "computingElement": params.get("computingElement", []),
                "submissionHost": params.get("submissionHost", []),
            }
            tmpLog.debug(f"query {constraint_map}")
            for attribute, match_list in constraint_map.items():
                if match_list == "ALL":
                    pass
                elif not match_list:
                    tmpLog.debug(f"{attribute} constraint is not specified in the query. Skipped")
                    return 0
                else:
                    one_param_list = [f":param_{attribute}_{v_i}" for v_i in range(len(match_list))]
                    tmp_varMap.update(zip(one_param_list, match_list))
                    params_string = "(" + ",".join(one_param_list) + ")"
                    constraints_query_string_list.append(f"{attribute} IN {params_string}")
            constraints_query_string = " AND ".join(constraints_query_string_list)
            sqlW = f"SELECT workerID FROM {workTableName} "
            sqlW += f"WHERE {constraints_query_string} "
            # set time to trigger sweeper
            if delay_seconds is None:
                # set a past time to trigger sweeper immediately
                setTime = core_utils.naive_utcnow() - datetime.timedelta(hours=6)
            else:
                # set a future time to delay trigger
                setTime = core_utils.naive_utcnow() + datetime.timedelta(seconds=delay_seconds)
            # get workers
            varMap = dict()
            varMap.update(tmp_varMap)
            self.execute(sqlW, varMap)
            resW = self.cur.fetchall()
            nRow = 0
            for (workerID,) in resW:
                # set killTime
                varMap = dict()
                varMap[":workerID"] = workerID
                varMap[":setTime"] = setTime
                varMap[":st1"] = WorkSpec.ST_finished
                varMap[":st2"] = WorkSpec.ST_failed
                varMap[":st3"] = WorkSpec.ST_cancelled
                self.execute(sqlL, varMap)
                nRow += self.cur.rowcount
            # commit
            self.commit()
            tmpLog.debug(f"set killTime to {nRow} workers")
            return nRow
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None
