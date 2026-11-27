"""
DB proxy module for jobs and their files

"""

import datetime
import random

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
)
from pandaharvester.harvestercore.event_spec import EventSpec
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.job_spec import JobSpec

# sql to delete a job, and its files, events and job-worker relations
_sqlDeleteJob = f"DELETE FROM {jobTableName} WHERE PandaID=:PandaID "
_sqlDeleteJobRelated = [f"DELETE FROM {tableName} WHERE PandaID=:PandaID " for tableName in (fileTableName, eventTableName, jobWorkerTableName)]


# DB proxy module for jobs and their files
class JobModule(BaseModule):
    # delete a job and, if it existed, its files, events and relations without commit; return the number of deleted jobs
    def _delete_job_and_related(self, panda_id):
        varMap = {":PandaID": panda_id}
        self.execute(_sqlDeleteJob, varMap)
        iDel = self.cur.rowcount
        if iDel <= 0:
            return 0
        for sqlD in _sqlDeleteJobRelated:
            self.execute(sqlD, varMap)
        return iDel

    # insert jobs
    @db_error_handler(default=False)
    def insert_jobs(self, jobspec_list):
        # get logger
        tmpLog = make_logger(method_name="insert_jobs")
        tmpLog.debug(f"{len(jobspec_list)} jobs")
        # sql to insert a job
        sqlJ = f"INSERT INTO {jobTableName} ({JobSpec.column_names()}) "
        sqlJ += JobSpec.bind_values_expression()
        # sql to insert a file
        sqlF = f"INSERT INTO {fileTableName} ({FileSpec.column_names()}) "
        sqlF += FileSpec.bind_values_expression()
        # loop over all jobs
        varMapsJ = []
        varMapsF = []
        for jobSpec in jobspec_list:
            # delete job just in case
            self._delete_job_and_related(jobSpec.PandaID)
            # commit
            self.commit()
            # insert job and files
            varMapsJ.append(jobSpec.values_list())
            for fileSpec in jobSpec.inFiles:
                varMapsF.append(fileSpec.values_list())
        # insert
        self.executemany(sqlJ, varMapsJ)
        self.executemany(sqlF, varMapsF)
        # commit
        self.commit()
        # return
        return True

    # get job
    @db_error_handler(default=None)
    def get_job(self, panda_id):
        # get logger
        tmpLog = make_logger(f"PandaID={panda_id}", method_name="get_job")
        tmpLog.debug("start")
        # sql to get job
        sql = f"SELECT {JobSpec.column_names()} FROM {jobTableName} "
        sql += "WHERE PandaID=:pandaID "
        # get job
        self.execute(sql, {":pandaID": panda_id})
        resJ = self.cur.fetchone()
        if resJ is None:
            jobSpec = None
        else:
            # make job
            jobSpec = JobSpec()
            jobSpec.pack(resJ)
            # get files
            sqlF = f"SELECT {FileSpec.column_names()} FROM {fileTableName} "
            sqlF += "WHERE PandaID=:PandaID "
            self.execute(sqlF, {":PandaID": panda_id})
            for resFile in self.cur.fetchall():
                fileSpec = FileSpec()
                fileSpec.pack(resFile)
                jobSpec.add_file(fileSpec)
        # commit
        self.commit()
        tmpLog.debug("done")
        # return
        return jobSpec

    # get all jobs (fetch entire jobTable)
    @db_error_handler(default=None)
    def get_jobs(self):
        # get logger
        tmpLog = make_logger(method_name="get_jobs")
        tmpLog.debug("start")
        # sql to get job
        sql = f"SELECT {JobSpec.column_names()} FROM {jobTableName} "
        sql += "WHERE PandaID IS NOT NULL"
        # get jobs
        self.execute(sql)
        jobSpecList = []
        # make jobs list
        for resJ in self.cur.fetchall():
            jobSpec = JobSpec()
            jobSpec.pack(resJ)
            jobSpecList.append(jobSpec)
        tmpLog.debug("done")
        # return
        return jobSpecList

    # update job
    @db_error_handler(default=None)
    def update_job(self, jobspec, criteria=None, update_in_file=False, update_out_file=False):
        # get logger
        tmpLog = make_logger(f"PandaID={jobspec.PandaID} subStatus={jobspec.subStatus}", method_name="update_job")
        tmpLog.debug("start")
        if criteria is None:
            criteria = {}
        # sql to update job
        sql = f"UPDATE {jobTableName} SET {jobspec.bind_update_changes_expression()} "
        sql += "WHERE PandaID=:PandaID "
        # update job
        varMap = jobspec.values_map(only_changed=True)
        for tmpKey, tmpVal in criteria.items():
            mapKey = f":{tmpKey}_cr"
            sql += f"AND {tmpKey}={mapKey} "
            varMap[mapKey] = tmpVal
        varMap[":PandaID"] = jobspec.PandaID
        self.execute(sql, varMap)
        nRow = self.cur.rowcount
        if nRow > 0:
            # update events
            for eventSpec in jobspec.events:
                varMap = eventSpec.values_map(only_changed=True)
                if varMap != {}:
                    sqlE = f"UPDATE {eventTableName} SET {eventSpec.bind_update_changes_expression()} "
                    sqlE += "WHERE eventRangeID=:eventRangeID "
                    varMap[":eventRangeID"] = eventSpec.eventRangeID
                    self.execute(sqlE, varMap)
            # set file status to done if jobs are done, unless input files are updated individually
            if not update_in_file and jobspec.is_final_status():
                sqlF = f"UPDATE {fileTableName} SET status=:status "
                sqlF += "WHERE PandaID=:PandaID AND fileType IN (:type1,:type2) "
                varMap = {":PandaID": jobspec.PandaID, ":type1": "input", ":type2": FileSpec.AUX_INPUT, ":status": "done"}
                self.execute(sqlF, varMap)
            # update changed input and/or output files
            filesToUpdate = []
            if update_in_file:
                filesToUpdate += jobspec.inFiles
            if update_out_file:
                filesToUpdate += jobspec.outFiles
            for fileSpec in filesToUpdate:
                varMap = fileSpec.values_map(only_changed=True)
                if varMap != {}:
                    sqlF = f"UPDATE {fileTableName} SET {fileSpec.bind_update_changes_expression()} "
                    sqlF += "WHERE fileID=:fileID "
                    varMap[":fileID"] = fileSpec.fileID
                    self.execute(sqlF, varMap)
            # set to_delete flag
            if jobspec.subStatus == "done":
                sqlD = f"UPDATE {fileTableName} SET todelete=:to_delete "
                sqlD += "WHERE PandaID=:PandaID "
                self.execute(sqlD, {":PandaID": jobspec.PandaID, ":to_delete": 1})
        # commit
        self.commit()
        tmpLog.debug(f"done with {nRow}")
        # return
        return nRow

    # insert output files into database
    @db_error_handler(default=False)
    def insert_files(self, jobspec_list):
        # get logger
        tmpLog = make_logger(method_name="insert_files")
        tmpLog.debug(f"{len(jobspec_list)} jobs")
        # sql to insert a file
        sqlF = f"INSERT INTO {fileTableName} ({FileSpec.column_names()}) "
        sqlF += FileSpec.bind_values_expression()
        # loop over all jobs
        varMapsF = [fileSpec.values_list() for jobSpec in jobspec_list for fileSpec in jobSpec.outFiles]
        # insert
        self.executemany(sqlF, varMapsF)
        # commit
        self.commit()
        # return
        return True

    # get number of jobs to fetch
    @db_error_handler(default={})
    def get_num_jobs_to_fetch(self, n_queues: int, interval: int, queue_config_mapper) -> dict:
        """
        Evaluate and return the map of n jobs to fetch of all queues queried

        Args:
            n_queues (int): max number of queues to query
            interval (int): check interval in seconds; only pick up queues with jobFetchTime before than interval seconds ago
            queue_config_mapper (QueueConfigMapper): queue config mapper object for all panda queues

        Returns:
            dict: map in the form {"queue1": N1_jobs_to_fetch, ...} for all queues queried
        """
        # get logger
        tmpLog = make_logger(method_name="get_num_jobs_to_fetch")
        tmpLog.debug("start")
        ret_map = {}
        # sql to get queues
        sqlQ = "SELECT queueName "
        sqlQ += f"FROM {pandaQueueTableName} "
        sqlQ += "WHERE jobFetchTime IS NULL OR jobFetchTime<:timeLimit "
        sqlQ += "ORDER BY jobFetchTime "
        # sql to count jobs group by status
        sqlN = f"SELECT status, COUNT(*) cnt, SUM(nCore) corecount FROM {jobTableName} "
        sqlN += "WHERE computingSite=:computingSite AND status IN (:status1,:status2) "
        sqlN += "GROUP BY status "
        # sql to update timestamp
        sqlU = f"UPDATE {pandaQueueTableName} SET jobFetchTime=:jobFetchTime "
        sqlU += "WHERE queueName=:queueName "
        sqlU += "AND (jobFetchTime IS NULL OR jobFetchTime<:timeLimit) "
        # get queues
        timeNow = core_utils.naive_utcnow()
        timeLimit = timeNow - datetime.timedelta(seconds=interval)
        self.execute(sqlQ, {":timeLimit": timeLimit})
        resQ = self.cur.fetchall()
        i_queues = 0
        for (queue_name,) in resQ:
            # update timestamp to lock the queue
            varMap = {":queueName": queue_name, ":jobFetchTime": timeNow, ":timeLimit": timeLimit}
            self.execute(sqlU, varMap)
            nRow = self.cur.rowcount
            # commit
            self.commit()
            # skip if not locked
            if nRow == 0:
                continue
            # count nQueue
            varMap = {":computingSite": queue_name, ":status1": "starting", ":status2": "running"}
            self.execute(sqlN, varMap)
            job_stats_map = {status: {"n": 0, "core": 0} for status in ("starting", "running")}
            for status, cnt, corecount in self.cur.fetchall():
                job_stats_map[status]["n"] = cnt
                # SUM is NULL when nCore is NULL for all jobs
                job_stats_map[status]["core"] = corecount if corecount is not None else 0
            # get job limit attributes from queue config
            queue_config = queue_config_mapper.get_queue(queue_name)
            nQueueLimitJob = getattr(queue_config, "nQueueLimitJob", None)
            nQueueLimitJobRatio = getattr(queue_config, "nQueueLimitJobRatio", None)
            nQueueLimitJobMin = getattr(queue_config, "nQueueLimitJobMin", None)
            nQueueLimitJobCores = getattr(queue_config, "nQueueLimitJobCores", None)
            nQueueLimitJobCoresRatio = getattr(queue_config, "nQueueLimitJobCoresRatio", None)
            nQueueLimitJobCoresMin = getattr(queue_config, "nQueueLimitJobCoresMin", None)
            # skip the queue if nQueueLimitJob is None (not PUSH)
            if nQueueLimitJob is None:
                continue
            # initialize
            n_queue_limit_job_eval = nQueueLimitJob
            n_queue_limit_job_cores_eval = nQueueLimitJobCores
            n_queue_limit_job_cores_min_eval = nQueueLimitJobCoresMin
            # dynamic nQueueLimitJob
            if nQueueLimitJobRatio is not None:
                n_queue_limit_job_by_ratio = int(job_stats_map["running"]["n"] * nQueueLimitJobRatio / 100)
                nQueueLimitJobMin_default = nQueueLimitJobMin if nQueueLimitJobMin is not None else min(1, nQueueLimitJob)
                if n_queue_limit_job_by_ratio < nQueueLimitJobMin_default:
                    n_queue_limit_job_eval = min(n_queue_limit_job_eval, nQueueLimitJobMin_default)
                else:
                    n_queue_limit_job_eval = min(n_queue_limit_job_eval, n_queue_limit_job_by_ratio)
            if nQueueLimitJobCoresRatio is not None:
                n_queue_limit_cores_by_ratio = int(job_stats_map["running"]["core"] * nQueueLimitJobCoresRatio / 100)
                if nQueueLimitJobMin is not None:
                    # get n_queue_limit_job_cores_min_eval from nQueueLimitJobMin if nQueueLimitJobCoresMin is not set to ensure the minimum cores (1 core per job)
                    n_queue_limit_job_cores_min_base = nQueueLimitJobMin * 1
                    if n_queue_limit_job_cores_min_eval is None:
                        n_queue_limit_job_cores_min_eval = n_queue_limit_job_cores_min_base
                    else:
                        n_queue_limit_job_cores_min_eval = max(n_queue_limit_job_cores_min_eval, n_queue_limit_job_cores_min_base)
                if n_queue_limit_job_cores_min_eval is not None and n_queue_limit_cores_by_ratio < n_queue_limit_job_cores_min_eval:
                    if n_queue_limit_job_cores_eval is not None:
                        n_queue_limit_job_cores_eval = min(n_queue_limit_job_cores_eval, n_queue_limit_job_cores_min_eval)
                    else:
                        n_queue_limit_job_cores_eval = n_queue_limit_job_cores_min_eval
                else:
                    if n_queue_limit_job_cores_eval is not None:
                        n_queue_limit_job_cores_eval = min(n_queue_limit_job_cores_eval, n_queue_limit_cores_by_ratio)
                    else:
                        n_queue_limit_job_cores_eval = n_queue_limit_cores_by_ratio
            # more jobs need to be queued
            n_queue = job_stats_map["starting"]["n"]
            corecount_queue = job_stats_map["starting"]["core"]
            if n_queue >= n_queue_limit_job_eval:
                tmpLog.debug(f"{queue_name} has n_queue({n_queue}) >= n_queue_limit_job({n_queue_limit_job_eval}) ; skipped ")
                continue
            elif n_queue_limit_job_cores_eval is not None and corecount_queue >= n_queue_limit_job_cores_eval:
                tmpLog.debug(f"{queue_name} has corecount_queue({corecount_queue}) >= n_queue_limit_job_cores({n_queue_limit_job_cores_eval}) ; skipped ")
                continue
            else:
                ret_map[queue_name] = {
                    "jobs": n_queue_limit_job_eval,
                    "cores": n_queue_limit_job_cores_eval,
                }
            # enough queues
            i_queues += 1
            if i_queues >= n_queues:
                break
        tmpLog.debug(f"got {str(ret_map)}")
        return ret_map

    # get jobs to propagate checkpoints
    @db_error_handler(default=[])
    def get_jobs_to_propagate(self, max_jobs, lock_interval, update_interval, locked_by):
        # get logger
        tmpLog = make_logger(f"by={locked_by}", method_name="get_jobs_to_propagate")
        tmpLog.debug("start")
        # sql to get jobs
        sql = f"SELECT PandaID FROM {jobTableName} "
        sql += "WHERE propagatorTime IS NOT NULL "
        sql += "AND ((propagatorTime<:lockTimeLimit AND propagatorLock IS NOT NULL) "
        sql += "OR (propagatorTime<:updateTimeLimit AND propagatorLock IS NULL)) "
        sql += f"ORDER BY propagatorTime LIMIT {max_jobs} "
        # sql to get jobs
        sqlJ = f"SELECT {JobSpec.column_names()} FROM {jobTableName} "
        sqlJ += "WHERE PandaID=:PandaID "
        # sql to lock job
        sqlL = f"UPDATE {jobTableName} SET propagatorTime=:timeNow,propagatorLock=:lockedBy "
        sqlL += "WHERE PandaID=:PandaID "
        sqlL += "AND ((propagatorTime<:lockTimeLimit AND propagatorLock IS NOT NULL) "
        sqlL += "OR (propagatorTime<:updateTimeLimit AND propagatorLock IS NULL)) "
        # sql to get events
        sqlE = f"SELECT {EventSpec.column_names()} FROM {eventTableName} "
        sqlE += "WHERE PandaID=:PandaID AND subStatus IN (:statusFinished,:statusFailed) "
        # sql to get file
        sqlF = "SELECT DISTINCT {0} FROM {1} f, {2} e, {1} f2 ".format(FileSpec.column_names("f2"), fileTableName, eventTableName)
        sqlF += "WHERE e.PandaID=:PandaID AND e.fileID=f.fileID "
        sqlF += "AND e.subStatus IN (:statusFinished,:statusFailed) "
        sqlF += "AND f2.fileID=f.zipFileID "
        # sql to get fileID of zip
        sqlZ = f"SELECT e.fileID,f.zipFileID FROM {fileTableName} f, {eventTableName} e "
        sqlZ += "WHERE e.PandaID=:PandaID AND e.fileID=f.fileID "
        sqlZ += "AND e.subStatus IN (:statusFinished,:statusFailed) "
        # sql to get checkpoint files
        sqlC = f"SELECT {FileSpec.column_names()} FROM {fileTableName} "
        sqlC += "WHERE PandaID=:PandaID AND fileType=:type AND status=:status "
        # get jobs
        timeNow = core_utils.naive_utcnow()
        lockTimeLimit = timeNow - datetime.timedelta(seconds=lock_interval)
        updateTimeLimit = timeNow - datetime.timedelta(seconds=update_interval)
        self.execute(sql, {":lockTimeLimit": lockTimeLimit, ":updateTimeLimit": updateTimeLimit})
        pandaIDs = [pandaID for (pandaID,) in self.cur.fetchall()]
        # partially randomise to increase success rate for lock
        nJobs = int(max_jobs * 0.2)
        subPandaIDs = pandaIDs[nJobs:]
        random.shuffle(subPandaIDs)
        pandaIDs = (pandaIDs[:nJobs] + subPandaIDs)[:max_jobs]
        jobSpecList = []
        iEvents = 0
        for pandaID in pandaIDs:
            # avoid a bulk update for many jobs with too many events
            if iEvents > 10000:
                break
            # lock job
            varMap = {":PandaID": pandaID, ":timeNow": timeNow, ":lockedBy": locked_by, ":lockTimeLimit": lockTimeLimit, ":updateTimeLimit": updateTimeLimit}
            self.execute(sqlL, varMap)
            nRow = self.cur.rowcount
            # commit
            self.commit()
            if nRow > 0:
                # read job
                self.execute(sqlJ, {":PandaID": pandaID})
                res = self.cur.fetchone()
                # make job
                jobSpec = JobSpec()
                jobSpec.pack(res)
                jobSpec.propagatorLock = locked_by
                # parameters to get finished/failed events and their files
                varMapE = {":PandaID": jobSpec.PandaID, ":statusFinished": "finished", ":statusFailed": "failed"}
                # get zipIDs
                self.execute(sqlZ, varMapE)
                zipIdMap = {tmpFileID: tmpZipFileID for tmpFileID, tmpZipFileID in self.cur.fetchall()}
                # get zip files
                zipFiles = {}
                self.execute(sqlF, varMapE)
                for resF in self.cur.fetchall():
                    fileSpec = FileSpec()
                    fileSpec.pack(resF)
                    zipFiles[fileSpec.fileID] = fileSpec
                # read events
                self.execute(sqlE, varMapE)
                for resE in self.cur.fetchall():
                    eventSpec = EventSpec()
                    eventSpec.pack(resE)
                    zipFileSpec = None
                    # get associated zip file if any
                    if eventSpec.fileID is not None:
                        if eventSpec.fileID not in zipIdMap:
                            continue
                        zipFileID = zipIdMap[eventSpec.fileID]
                        if zipFileID is not None:
                            # skip like events without file if the zip file is gone
                            if zipFileID not in zipFiles:
                                continue
                            zipFileSpec = zipFiles[zipFileID]
                    jobSpec.add_event(eventSpec, zipFileSpec)
                    iEvents += 1
                # read checkpoint files
                self.execute(sqlC, {":PandaID": pandaID, ":type": "checkpoint", ":status": "renewed"})
                for resFile in self.cur.fetchall():
                    fileSpec = FileSpec()
                    fileSpec.pack(resFile)
                    jobSpec.add_out_file(fileSpec)
                # add to job list
                jobSpecList.append(jobSpec)
        tmpLog.debug(f"got {len(jobSpecList)} jobs")
        return jobSpecList

    # get jobs in sub status
    @db_error_handler(default=[])
    def get_jobs_in_sub_status(
        self,
        sub_status,
        max_jobs,
        time_column=None,
        lock_column=None,
        interval_without_lock=None,
        interval_with_lock=None,
        locked_by=None,
        new_sub_status=None,
        max_files_per_job=None,
        ng_file_status_list=None,
    ):
        # get logger
        msgPfx = None if locked_by is None else f"id={locked_by}"
        tmpLog = make_logger(msgPfx, method_name="get_jobs_in_sub_status")
        tmpLog.debug(f"start subStatus={sub_status} timeColumn={time_column}")
        timeNow = core_utils.naive_utcnow()
        # time limits for lock
        timeLimitMap = {}
        if interval_with_lock is not None:
            timeLimitMap[":lockTimeLimit"] = timeNow - datetime.timedelta(seconds=interval_with_lock)
        if interval_without_lock is not None:
            timeLimitMap[":updateTimeLimit"] = timeNow - datetime.timedelta(seconds=interval_without_lock)
        # sql to count jobs being processed
        sqlC = f"SELECT COUNT(*) cnt FROM {jobTableName} "
        sqlC += f"WHERE ({lock_column} IS NOT NULL AND subStatus=:subStatus "
        if time_column is not None and interval_with_lock is not None:
            sqlC += "AND ({0} IS NOT NULL AND {0}>:lockTimeLimit) ".format(time_column)
        sqlC += ") OR subStatus=:newSubStatus "
        # count jobs
        if max_jobs > 0 and new_sub_status is not None:
            varMap = {":subStatus": sub_status, ":newSubStatus": new_sub_status}
            if time_column is not None and interval_with_lock is not None:
                varMap[":lockTimeLimit"] = timeLimitMap[":lockTimeLimit"]
            self.execute(sqlC, varMap)
            (nProcessing,) = self.cur.fetchone()
            if nProcessing >= max_jobs:
                # commit
                self.commit()
                tmpLog.debug(f"enough jobs {nProcessing} are being processed in {new_sub_status} state")
                return []
            max_jobs -= nProcessing
        # sql to get job IDs
        sql = f"SELECT PandaID FROM {jobTableName} "
        sql += "WHERE subStatus=:subStatus "
        # condition on lock time, shared with sql to lock job
        sqlTimeCond = ""
        if time_column is not None:
            if interval_with_lock is not None:
                sqlTimeCond += f"OR ({time_column}<:lockTimeLimit AND {lock_column} IS NOT NULL) "
            if interval_without_lock is not None:
                sqlTimeCond += f"OR ({time_column}<:updateTimeLimit AND {lock_column} IS NULL) "
            sql += f"AND ({time_column} IS NULL {sqlTimeCond}) "
            sql += f"ORDER BY {time_column} "
        # sql to lock job
        sqlL = f"UPDATE {jobTableName} SET {time_column}=:timeNow,{lock_column}=:lockedBy "
        sqlL += "WHERE PandaID=:PandaID AND subStatus=:subStatus "
        if time_column is not None:
            sqlL += f"AND ({time_column} IS NULL {sqlTimeCond}) "
        # sql to get jobs
        sqlGJ = f"SELECT {JobSpec.column_names()} FROM {jobTableName} "
        sqlGJ += "WHERE PandaID=:PandaID "
        # sql to get file
        sqlGF = f"SELECT {FileSpec.column_names()} FROM {fileTableName} "
        sqlGF += "WHERE PandaID=:PandaID AND fileType=:type "
        ngStatusMap = {}
        if ng_file_status_list:
            ngStatusMap = {f":status_{tmpStatus}": tmpStatus for tmpStatus in ng_file_status_list}
            sqlGF += f"AND status NOT IN ({','.join(ngStatusMap)}) "
        if max_files_per_job is not None and max_files_per_job > 0:
            sqlGF += f"LIMIT {max_files_per_job} "
        # get jobs
        self.execute(sql, {":subStatus": sub_status, **timeLimitMap})
        pandaIDs = [pandaID for (pandaID,) in self.cur.fetchall()]
        # partially randomise to increase success rate for lock
        nJobs = int(max_jobs * 0.2)
        subPandaIDs = pandaIDs[nJobs:]
        random.shuffle(subPandaIDs)
        pandaIDs = (pandaIDs[:nJobs] + subPandaIDs)[:max_jobs]
        jobSpecList = []
        for pandaID in pandaIDs:
            # lock job
            if locked_by is not None:
                varMap = {":PandaID": pandaID, ":timeNow": timeNow, ":lockedBy": locked_by, ":subStatus": sub_status, **timeLimitMap}
                self.execute(sqlL, varMap)
                nRow = self.cur.rowcount
                # commit
                self.commit()
            else:
                nRow = 1
            if nRow > 0:
                # get job
                self.execute(sqlGJ, {":PandaID": pandaID})
                resGJ = self.cur.fetchone()
                # make job
                jobSpec = JobSpec()
                jobSpec.pack(resGJ)
                if locked_by is not None:
                    jobSpec.lockedBy = locked_by
                    setattr(jobSpec, time_column, timeNow)
                # get files
                if jobSpec.auxInput in [None, JobSpec.AUX_hasAuxInput, JobSpec.AUX_allTriggered]:
                    fileType = "input"
                else:
                    fileType = FileSpec.AUX_INPUT
                self.execute(sqlGF, {":PandaID": jobSpec.PandaID, ":type": fileType, **ngStatusMap})
                for resFile in self.cur.fetchall():
                    fileSpec = FileSpec()
                    fileSpec.pack(resFile)
                    jobSpec.add_in_file(fileSpec)
                # append
                jobSpecList.append(jobSpec)
        tmpLog.debug(f"got {len(jobSpecList)} jobs")
        return jobSpecList

    # release jobs
    @db_error_handler(default=False)
    def release_jobs(self, panda_ids, locked_by):
        # get logger
        tmpLog = make_logger(method_name="release_jobs")
        tmpLog.debug(f"start for {len(panda_ids)} jobs")
        # sql to release job
        sql = f"UPDATE {jobTableName} SET lockedBy=NULL "
        sql += "WHERE PandaID=:pandaID AND lockedBy=:lockedBy "
        nJobs = 0
        for pandaID in panda_ids:
            self.execute(sql, {":pandaID": pandaID, ":lockedBy": locked_by})
            if self.cur.rowcount > 0:
                nJobs += 1
        # commit
        self.commit()
        tmpLog.debug(f"released {nJobs} jobs")
        # return
        return True

    # lock job again
    @db_error_handler(default=False)
    def lock_job_again(self, panda_id, time_column, lock_column, locked_by):
        tmpLog = make_logger(f"PandaID={panda_id}", method_name="lock_job_again")
        tmpLog.debug(f"start column={lock_column} id={locked_by}")
        # check lock
        sqlC = f"SELECT {lock_column},{time_column} FROM {jobTableName} "
        sqlC += "WHERE PandaID=:pandaID "
        sqlC += "FOR UPDATE "
        self.execute(sqlC, {":pandaID": panda_id})
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
                sqlU = f"UPDATE {jobTableName} SET {time_column}=:timeNow WHERE pandaID=:pandaID "
                self.execute(sqlU, {":pandaID": panda_id, ":timeNow": core_utils.naive_utcnow()})
                retVal = True
        # commit
        self.commit()
        tmpLog.debug(f"done with {retVal}")
        # return
        return retVal

    # increment submission attempt
    @db_error_handler(default=False)
    def increment_submission_attempt(self, panda_id, new_number):
        # get logger
        tmpLog = make_logger(f"pandaID={panda_id}", method_name="increment_submission_attempt")
        tmpLog.debug(f"start with newNum={new_number}")
        # sql to update attempt number
        sqlL = f"UPDATE {jobTableName} SET submissionAttempts=:newNum "
        sqlL += "WHERE PandaID=:PandaID "
        self.execute(sqlL, {":PandaID": panda_id, ":newNum": new_number})
        # commit
        self.commit()
        tmpLog.debug("done")
        return True

    # delete old jobs
    @db_error_handler(default=False)
    def delete_old_jobs(self, timeout):
        # get logger
        tmpLog = make_logger(f"timeout={timeout}", method_name="delete_old_jobs")
        tmpLog.debug("start")
        # sql to get old jobs to be deleted
        sqlGJ = f"SELECT PandaID FROM {jobTableName} "
        sqlGJ += "WHERE subStatus=:subStatus AND propagatorTime IS NULL "
        sqlGJ += "AND ((modificationTime IS NOT NULL AND modificationTime<:timeLimit1) "
        sqlGJ += "OR (modificationTime IS NULL AND creationTime<:timeLimit2)) "
        # get jobs
        timeNow = core_utils.naive_utcnow()
        varMap = {
            ":subStatus": "done",
            ":timeLimit1": timeNow - datetime.timedelta(hours=timeout),
            ":timeLimit2": timeNow - datetime.timedelta(hours=timeout * 2),
        }
        self.execute(sqlGJ, varMap)
        resGJ = self.cur.fetchall()
        nDel = 0
        for (pandaID,) in resGJ:
            # delete job, its files, events and relations
            nDel += self._delete_job_and_related(pandaID)
            # commit
            self.commit()
        tmpLog.debug(f"deleted {nDel} jobs")
        return True

    # disable multi workers
    @db_error_handler(default=None)
    def disable_multi_workers(self, panda_id):
        # get logger
        tmpLog = make_logger(f"PandaID={panda_id}", method_name="disable_multi_workers")
        tmpLog.debug("start")
        # sql to update flag
        sqlJ = f"UPDATE {jobTableName} SET moreWorkers=0 "
        sqlJ += "WHERE PandaID=:pandaID AND nWorkers IS NOT NULL AND nWorkersLimit IS NOT NULL "
        sqlJ += "AND nWorkers>0 "
        # set flag
        self.execute(sqlJ, {":pandaID": panda_id})
        nRow = self.cur.rowcount
        # commit
        self.commit()
        tmpLog.debug(f"done with {nRow}")
        # return
        return nRow

    # delete orphaned job info
    @db_error_handler(default=False)
    def delete_orphaned_job_info(self):
        # get logger
        tmpLog = make_logger(method_name="delete_orphaned_job_info")
        tmpLog.debug("start")
        # loop over all tables
        for tableName, sqlDJ in zip((fileTableName, eventTableName, jobWorkerTableName), _sqlDeleteJobRelated):
            # get job info to be deleted; DISTINCT since tables have many rows per PandaID and one delete removes them all
            sqlGJ = f"SELECT DISTINCT PandaID FROM {tableName} "
            sqlGJ += f"WHERE PandaID NOT IN (SELECT PandaID FROM {jobTableName}) "
            self.execute(sqlGJ)
            resGJ = self.cur.fetchall()
            nDel = 0
            for (pandaID,) in resGJ:
                # delete
                self.execute(sqlDJ, {":PandaID": pandaID})
                iDel = self.cur.rowcount
                if iDel > 0:
                    nDel += iDel
                # commit
                self.commit()
            tmpLog.debug(f"deleted {nDel} records from {tableName}")
        return True
