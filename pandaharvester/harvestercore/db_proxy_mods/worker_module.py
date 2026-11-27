"""
DB proxy module for workers

"""

import datetime
import random

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_mods.base_module import (
    BaseModule,
    _logger,
    db_error_handler,
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
from pandaharvester.harvestercore.worker_check_spec import WorkerCheckSpec

# worker statuses not to be killed
_final_status_map = {":st1": WorkSpec.ST_finished, ":st2": WorkSpec.ST_failed, ":st3": WorkSpec.ST_cancelled}


# time to set to killTime: a past time to trigger the sweeper immediately, or a future time to delay the trigger
def _get_kill_time(delay_seconds):
    if delay_seconds is None:
        return core_utils.naive_utcnow() - datetime.timedelta(hours=6)
    return core_utils.naive_utcnow() + datetime.timedelta(seconds=delay_seconds)


# DB proxy module for workers
class WorkerModule(BaseModule):
    # update worker
    @db_error_handler(default=None)
    def update_worker(self, workspec, criteria=None):
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

    # register a worker
    @db_error_handler(default=False)
    def register_worker(self, workspec, jobspec_list, locked_by, first_check_time=None):
        """
        first_check_time: time of the first check by the monitor; when given, the worker check schedule is created
        in the same transaction, unless the worker is in final status or of multi-worker mapping
        """
        tmpLog = core_utils.make_logger(_logger, f"batchID={workspec.batchID}", method_name="register_worker")
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
            self.execute(sqlE, {":workerID": workspec.workerID})
            if self.cur.fetchone() is None:
                isNew = True
        if isNew:
            # insert a worker
            sqlI = f"INSERT INTO {workTableName} ({WorkSpec.column_names()}) "
            sqlI += WorkSpec.bind_values_expression()
            self.execute(sqlI, workspec.values_list())
            # decrement nNewWorkers
            self.execute(sqlDN, {":queueName": workspec.computingSite})
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
                # get active workers for the job, including this worker
                varMap = {":pandaID": jobSpec.PandaID, ":st_submitted": WorkSpec.ST_submitted, ":st_running": WorkSpec.ST_running, ":st_idle": WorkSpec.ST_idle}
                self.execute(sqlNW, varMap)
                workerIDs = {workspec.workerID}
                workerIDs.update(tmpWorkerID for (tmpWorkerID,) in self.cur.fetchall())
                # update attributes
                if jobSpec.subStatus in ["submitted", "running"]:
                    countWorkers = True
                else:
                    countWorkers = workspec.hasJob == 1
                    if workspec.status == WorkSpec.ST_missed:
                        # not update if other workers are active
                        if len(workerIDs) > 1:
                            continue
                        core_utils.update_job_attributes_with_workers(workspec.mapType, [jobSpec], [workspec], {}, {})
                        jobSpec.trigger_propagation()
                    elif countWorkers:
                        jobSpec.subStatus = "submitted"
                    else:
                        jobSpec.subStatus = "queued"
                if countWorkers:
                    jobSpec.nWorkers = len(workerIDs)
                    try:
                        jobSpec.nWorkersInTotal += 1
                    except Exception:
                        jobSpec.nWorkersInTotal = jobSpec.nWorkers
                # update job
                varMap = jobSpec.values_map(only_changed=True)
                if len(varMap) > 0:
                    sqlJ = f"UPDATE {jobTableName} SET {jobSpec.bind_update_changes_expression()} "
                    sqlJ += "WHERE PandaID=:cr_PandaID AND lockedBy=:cr_lockedBy "
                    varMap[":cr_PandaID"] = jobSpec.PandaID
                    varMap[":cr_lockedBy"] = locked_by
                    self.execute(sqlJ, varMap)
                if jobSpec.subStatus in ["submitted", "running"]:
                    # values for job/worker mapping
                    jwRelation = JobWorkerRelationSpec()
                    jwRelation.PandaID = jobSpec.PandaID
                    jwRelation.workerID = workspec.workerID
                    varMapsR.append(jwRelation.values_list())
        # insert job/worker mapping
        if len(varMapsR) > 0:
            self.executemany(sqlR, varMapsR)
        # create the worker check schedule
        if first_check_time is not None and not workspec.is_final_status() and workspec.mapType != WorkSpec.MT_MultiWorkers:
            self._insert_worker_check(workspec.workerID, first_check_time, WorkerCheckSpec.TRIG_submit)
        # commit
        self.commit()
        # return
        return True

    # get workers to propagate
    @db_error_handler(default=[])
    def get_workers_to_propagate(self, max_workers, check_interval):
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
        self.execute(sqlW, {":checkTimeLimit": timeLimit})
        tmpWorkers = [workerID for (workerID,) in self.cur.fetchall()]
        # partially randomize to increase hit rate
        nWorkers = int(max_workers * 0.2)
        subTmpWorkers = tmpWorkers[nWorkers:]
        random.shuffle(subTmpWorkers)
        tmpWorkers = (tmpWorkers[:nWorkers] + subTmpWorkers)[:max_workers]
        retVal = []
        for workerID in tmpWorkers:
            # lock worker
            varMap = {":workerID": workerID, ":timeNow": timeNow, ":checkTimeLimit": timeLimit}
            self.execute(sqlL, varMap)
            if self.cur.rowcount > 0:
                # get worker
                varMap = {":workerID": workerID}
                self.execute(sqlG, varMap)
                workSpec = WorkSpec()
                workSpec.pack(self.cur.fetchone())
                retVal.append(workSpec)
                # get associated PandaIDs
                self.execute(sqlA, varMap)
                workSpec.pandaid_list = [pandaID for (pandaID,) in self.cur.fetchall()]
            # commit
            self.commit()
        tmpLog.debug(f"got {len(retVal)} workers")
        return retVal

    # get workers to feed events
    @db_error_handler(default={})
    def get_workers_to_feed_events(self, max_workers, lock_interval, locked_by):
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
        varMap = {
            ":status1": WorkSpec.ST_running,
            ":status2": WorkSpec.ST_submitted,
            ":eventsRequest": WorkSpec.EV_requestEvents,
            ":lockTimeLimit": lockTimeLimit,
        }
        self.execute(sqlW, varMap)
        resW = self.cur.fetchall()
        retVal = {}
        nWorkers = 0
        for workerID, workStatus in resW:
            # lock worker
            varMap = {
                ":workerID": workerID,
                ":timeNow": timeNow,
                ":status": workStatus,
                ":eventsRequest": WorkSpec.EV_requestEvents,
                ":lockTimeLimit": lockTimeLimit,
                ":lockedBy": locked_by,
            }
            self.execute(sqlL, varMap)
            nRow = self.cur.rowcount
            # commit
            self.commit()
            # skip if not locked
            if nRow == 0:
                continue
            # get worker
            self.execute(sqlG, {":workerID": workerID})
            workSpec = WorkSpec()
            workSpec.pack(self.cur.fetchone())
            retVal.setdefault(workSpec.computingSite, []).append(workSpec)
            nWorkers += 1
        tmpLog.debug(f"got {nWorkers} workers in {len(retVal)} queues")
        return retVal

    # get ready workers
    @db_error_handler(default=[])
    def get_ready_workers(self, queue_name, n_ready):
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
        varMap = {":status_ready": WorkSpec.ST_ready, ":status_running": WorkSpec.ST_running, ":queueName": queue_name}
        self.execute(sqlG, varMap)
        resList = self.cur.fetchall()
        retVal = []
        for res in resList:
            workSpec = WorkSpec()
            workSpec.pack(res)
            # get number of jobs
            self.execute(sqlP, {":workerID": workSpec.workerID})
            resP = self.cur.fetchone()
            if resP is not None and resP[0] > 0:
                workSpec.nJobs = resP[0]
            retVal.append(workSpec)
        # commit
        self.commit()
        tmpLog.debug(f"got {str(retVal)}")
        return retVal

    # get a worker
    @db_error_handler(default=None)
    def get_worker_with_id(self, worker_id):
        # get logger
        tmpLog = core_utils.make_logger(_logger, f"workerID={worker_id}", method_name="get_worker_with_id")
        tmpLog.debug("start")
        # sql to get a worker
        sqlG = f"SELECT {WorkSpec.column_names()} FROM {workTableName} "
        sqlG += "WHERE workerID=:workerID "
        # get a worker
        self.execute(sqlG, {":workerID": worker_id})
        res = self.cur.fetchone()
        # commit
        self.commit()
        if res is None:
            tmpLog.debug("not found")
            return None
        workSpec = WorkSpec()
        workSpec.pack(res)
        tmpLog.debug("got")
        return workSpec

    # get workers to kill
    @db_error_handler(default={})
    def get_workers_to_kill(self, max_workers, check_interval):
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
        self.execute(sqlW, {":checkTimeLimit": timeLimit})
        resW = self.cur.fetchall()
        useConfigID = core_utils.dynamic_plugin_change()
        retVal = dict()
        nWorkers = 0
        for workerID, workerStatus, configID in resW:
            # ignore configID
            if not useConfigID:
                configID = None
            # release worker in final status, or lock it
            toLock = workerStatus not in (WorkSpec.ST_cancelled, WorkSpec.ST_failed, WorkSpec.ST_finished)
            varMap = {":workerID": workerID, ":checkTimeLimit": timeLimit, ":setTime": timeNow if toLock else None}
            self.execute(sqlL, varMap)
            # get worker
            if self.cur.rowcount == 1 and toLock:
                self.execute(sqlG, {":workerID": workerID})
                workSpec = WorkSpec()
                workSpec.pack(self.cur.fetchone())
                retVal.setdefault(workSpec.computingSite, dict()).setdefault(configID, []).append(workSpec)
                nWorkers += 1
            # commit
            self.commit()
        tmpLog.debug(f"got {nWorkers} workers in {len(retVal)} queues")
        return retVal

    # send kill command to workers associated to a job
    @db_error_handler(default=None)
    def mark_workers_to_kill_by_pandaid(self, panda_id, delay_seconds=None):
        # get logger
        tmpLog = core_utils.make_logger(_logger, f"PandaID={panda_id}", method_name="mark_workers_to_kill_by_pandaid")
        tmpLog.debug("start")
        # sql to set killTime
        sqlL = f"UPDATE {workTableName} SET killTime=:setTime WHERE workerID=:workerID AND killTime IS NULL AND NOT status IN (:st1,:st2,:st3) "

        # sql to get associated workers
        sqlA = f"SELECT workerID FROM {jobWorkerTableName} WHERE PandaID=:pandaID "
        # set time to trigger sweeper
        setTime = _get_kill_time(delay_seconds)
        # get workers
        self.execute(sqlA, {":pandaID": panda_id})
        resA = self.cur.fetchall()
        nRow = 0
        for (workerID,) in resA:
            # set killTime
            self.execute(sqlL, {":workerID": workerID, ":setTime": setTime, **_final_status_map})
            nRow += self.cur.rowcount
        # commit
        self.commit()
        tmpLog.debug(f"set killTime to {nRow} workers")
        return nRow

    # send kill command to workers
    @db_error_handler(default=None)
    def mark_workers_to_kill_by_workerids(self, worker_ids, delay_seconds=None):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="mark_workers_to_kill_by_workerids")
        tmpLog.debug("start")
        # sql to set killTime
        sqlL = f"UPDATE {workTableName} SET killTime=:setTime "
        sqlL += "WHERE workerID=:workerID AND killTime IS NULL AND NOT status IN (:st1,:st2,:st3) "
        # set time to trigger sweeper
        setTime = _get_kill_time(delay_seconds)
        varMaps = [{":workerID": worker_id, ":setTime": setTime, **_final_status_map} for worker_id in worker_ids]
        nRow = 0
        if varMaps:
            self.executemany(sqlL, varMaps)
            nRow = self.cur.rowcount
        # commit
        self.commit()
        tmpLog.debug(f"set killTime with {nRow}")
        return nRow

    # get workers for cleanup
    @db_error_handler(default={})
    def get_workers_for_cleanup(self, max_workers, status_timeout_map):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="get_workers_for_cleanup")
        tmpLog.debug("start")
        # sql to get worker IDs
        timeNow = core_utils.naive_utcnow()
        modTimeLimit = timeNow - datetime.timedelta(minutes=60)
        varMap = {":timeLimit": modTimeLimit}
        statusConds = []
        for tmpStatus, tmpTimeout in status_timeout_map.items():
            tmpStatusKey = f":status_{tmpStatus}"
            tmpTimeoutKey = f":timeLimit_{tmpStatus}"
            statusConds.append(f"(status={tmpStatusKey} AND endTime<={tmpTimeoutKey})")
            varMap[tmpStatusKey] = tmpStatus
            varMap[tmpTimeoutKey] = timeNow - datetime.timedelta(hours=tmpTimeout)
        sqlW = f"SELECT workerID, configID FROM {workTableName} "
        sqlW += f"WHERE lastUpdate IS NULL AND ({' OR '.join(statusConds)}) "
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
        sqlD = f"SELECT b.lfn,b.todelete FROM {fileTableName} a, {fileTableName} b "
        sqlD += "WHERE a.PandaID=:PandaID AND a.fileType IN (:fileType1,:fileType2) AND b.lfn=a.lfn "
        # get workerIDs
        self.execute(sqlW, varMap)
        resW = self.cur.fetchall()
        useConfigID = core_utils.dynamic_plugin_change()
        retVal = dict()
        iWorkers = 0
        for workerID, configID in resW:
            # lock worker
            varMap = {":workerID": workerID, ":setTime": timeNow, ":timeLimit": modTimeLimit}
            self.execute(sqlL, varMap)
            nRow = self.cur.rowcount
            # commit
            self.commit()
            if nRow == 0:
                continue
            # ignore configID
            if not useConfigID:
                configID = None
            # check associated jobs
            varMap = {":workerID": workerID}
            self.execute(sqlA, varMap)
            (nActJobs,) = self.cur.fetchone()
            # cleanup when there is no active job
            if nActJobs == 0:
                # get worker
                self.execute(sqlG, varMap)
                workSpec = WorkSpec()
                workSpec.pack(self.cur.fetchone())
                retVal.setdefault(workSpec.computingSite, dict()).setdefault(configID, []).append(workSpec)
                # get jobs
                jobSpecs = []
                checkedLFNs = set()
                keepLFNs = set()
                self.execute(sqlP, varMap)
                resP = self.cur.fetchall()
                for (pandaID,) in resP:
                    self.execute(sqlJ, {":PandaID": pandaID})
                    jobSpec = JobSpec()
                    jobSpec.pack(self.cur.fetchone())
                    jobSpecs.append(jobSpec)
                    # get LFNs not to be deleted
                    self.execute(sqlD, {":PandaID": pandaID, ":fileType1": "input", ":fileType2": FileSpec.AUX_INPUT})
                    for tmpLFN, tmpTodelete in self.cur.fetchall():
                        if tmpTodelete == 0:
                            keepLFNs.add(tmpLFN)
                    # get files to be deleted
                    self.execute(sqlF, {":PandaID": pandaID})
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

    # delete a worker
    @db_error_handler(default=False)
    def delete_worker(self, worker_id):
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
        varMap = {":workerID": worker_id}
        self.execute(sqlJ, varMap)
        varMapsJ = [{":PandaID": pandaID} for (pandaID,) in self.cur.fetchall()]
        if varMapsJ:
            # delete jobs, files, events and relations
            for sqlD in (sqlDJ, sqlDF, sqlDE, sqlDR):
                self.executemany(sqlD, varMapsJ)
        # delete worker
        self.execute(sqlDW, varMap)
        # commit
        self.commit()
        tmpLog.debug("done")
        return True

    # get the number of missed worker
    @db_error_handler(default=0)
    def get_num_missed_workers(self, queue_name, criteria):
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
                sqlW += f"AND pq.{attr}=:{attr} "
                varMap[f":{attr}"] = val
            elif attr in ["computingSite", "computingElement"]:
                sqlW += f"AND wt.{attr}=:{attr} "
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
            self.execute(sqlW, {":PandaID": panda_id})
            retList = []
            for (worker_id,) in self.cur.fetchall():
                # get a worker
                self.execute(sqlG, {":workerID": worker_id})
                workSpec = WorkSpec()
                workSpec.pack(self.cur.fetchone(), slim=True)
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

    # lock worker again to feed events
    @db_error_handler(default=False)
    def lock_worker_again_to_feed_events(self, worker_id, locked_by):
        tmpLog = core_utils.make_logger(_logger, f"workerID={worker_id}", method_name="lock_worker_again_to_feed_events")
        tmpLog.debug(f"start id={locked_by}")
        # check lock
        sqlC = f"SELECT eventFeedLock,eventFeedTime FROM {workTableName} "
        sqlC += "WHERE workerID=:workerID "
        sqlC += "FOR UPDATE "
        self.execute(sqlC, {":workerID": worker_id})
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
                self.execute(sqlU, {":workerID": worker_id, ":timeNow": core_utils.naive_utcnow()})
                retVal = True
        # commit
        self.commit()
        tmpLog.debug(f"done with {retVal}")
        # return
        return retVal

    # send kill command to workers by query
    @db_error_handler(default=None)
    def mark_workers_to_kill_by_query(self, params, delay_seconds=None):
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
        setTime = _get_kill_time(delay_seconds)
        # get workers
        self.execute(sqlW, tmp_varMap)
        resW = self.cur.fetchall()
        nRow = 0
        for (workerID,) in resW:
            # set killTime
            self.execute(sqlL, {":workerID": workerID, ":setTime": setTime, **_final_status_map})
            nRow += self.cur.rowcount
        # commit
        self.commit()
        tmpLog.debug(f"set killTime to {nRow} workers")
        return nRow
