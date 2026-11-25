"""
DB proxy module for jobs and their files

"""

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
)
from pandaharvester.harvestercore.event_spec import EventSpec
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.job_spec import JobSpec


# DB proxy module for jobs and their files
class JobModule(BaseModule):
    # insert jobs
    def insert_jobs(self, jobspec_list):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="insert_jobs")
        tmpLog.debug(f"{len(jobspec_list)} jobs")
        try:
            # sql to insert a job
            sqlJ = f"INSERT INTO {jobTableName} ({JobSpec.column_names()}) "
            sqlJ += JobSpec.bind_values_expression()
            # sql to insert a file
            sqlF = f"INSERT INTO {fileTableName} ({FileSpec.column_names()}) "
            sqlF += FileSpec.bind_values_expression()
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
            # loop over all jobs
            varMapsJ = []
            varMapsF = []
            for jobSpec in jobspec_list:
                # delete job just in case
                varMap = dict()
                varMap[":PandaID"] = jobSpec.PandaID
                self.execute(sqlDJ, varMap)
                iDel = self.cur.rowcount
                if iDel > 0:
                    # delete files
                    self.execute(sqlDF, varMap)
                    # delete events
                    self.execute(sqlDE, varMap)
                    # delete relations
                    self.execute(sqlDR, varMap)
                # commit
                self.commit()
                # insert job and files
                varMap = jobSpec.values_list()
                varMapsJ.append(varMap)
                for fileSpec in jobSpec.inFiles:
                    varMap = fileSpec.values_list()
                    varMapsF.append(varMap)
            # insert
            self.executemany(sqlJ, varMapsJ)
            self.executemany(sqlF, varMapsF)
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

    # get job
    def get_job(self, panda_id):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"PandaID={panda_id}", method_name="get_job")
            tmpLog.debug("start")
            # sql to get job
            sql = f"SELECT {JobSpec.column_names()} FROM {jobTableName} "
            sql += "WHERE PandaID=:pandaID "
            # get job
            varMap = dict()
            varMap[":pandaID"] = panda_id
            self.execute(sql, varMap)
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
                varMap = dict()
                varMap[":PandaID"] = panda_id
                self.execute(sqlF, varMap)
                resFileList = self.cur.fetchall()
                for resFile in resFileList:
                    fileSpec = FileSpec()
                    fileSpec.pack(resFile)
                    jobSpec.add_file(fileSpec)
            # commit
            self.commit()
            tmpLog.debug("done")
            # return
            return jobSpec
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # get all jobs (fetch entire jobTable)
    def get_jobs(self):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_jobs")
            tmpLog.debug("start")
            # sql to get job
            sql = f"SELECT {JobSpec.column_names()} FROM {jobTableName} "
            sql += "WHERE PandaID IS NOT NULL"
            # get jobs
            varMap = None
            self.execute(sql, varMap)
            resJobs = self.cur.fetchall()
            if resJobs is None:
                return None
            jobSpecList = []
            # make jobs list
            for resJ in resJobs:
                jobSpec = JobSpec()
                jobSpec.pack(resJ)
                jobSpecList.append(jobSpec)
            tmpLog.debug("done")
            # return
            return jobSpecList
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # update job
    def update_job(self, jobspec, criteria=None, update_in_file=False, update_out_file=False):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"PandaID={jobspec.PandaID} subStatus={jobspec.subStatus}", method_name="update_job")
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
                # update input file
                if update_in_file:
                    for fileSpec in jobspec.inFiles:
                        varMap = fileSpec.values_map(only_changed=True)
                        if varMap != {}:
                            sqlF = f"UPDATE {fileTableName} SET {fileSpec.bind_update_changes_expression()} "
                            sqlF += "WHERE fileID=:fileID "
                            varMap[":fileID"] = fileSpec.fileID
                            self.execute(sqlF, varMap)
                else:
                    # set file status to done if jobs are done
                    if jobspec.is_final_status():
                        varMap = dict()
                        varMap[":PandaID"] = jobspec.PandaID
                        varMap[":type1"] = "input"
                        varMap[":type2"] = FileSpec.AUX_INPUT
                        varMap[":status"] = "done"
                        sqlF = f"UPDATE {fileTableName} SET status=:status "
                        sqlF += "WHERE PandaID=:PandaID AND fileType IN (:type1,:type2) "
                        self.execute(sqlF, varMap)
                # update output file
                if update_out_file:
                    for fileSpec in jobspec.outFiles:
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
                    varMap = dict()
                    varMap[":PandaID"] = jobspec.PandaID
                    varMap[":to_delete"] = 1
                    self.execute(sqlD, varMap)
            # commit
            self.commit()
            tmpLog.debug(f"done with {nRow}")
            # return
            return nRow
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # insert output files into database
    def insert_files(self, jobspec_list):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="insert_files")
        tmpLog.debug(f"{len(jobspec_list)} jobs")
        try:
            # sql to insert a file
            sqlF = f"INSERT INTO {fileTableName} ({FileSpec.column_names()}) "
            sqlF += FileSpec.bind_values_expression()
            # loop over all jobs
            varMapsF = []
            for jobSpec in jobspec_list:
                for fileSpec in jobSpec.outFiles:
                    varMap = fileSpec.values_list()
                    varMapsF.append(varMap)
            # insert
            self.executemany(sqlF, varMapsF)
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

    # get number of jobs to fetch (deprecated)
    def get_num_jobs_to_fetch_old(self, n_queues, interval):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="get_num_jobs_to_fetch_old")
        try:
            tmpLog.debug("start")
            retMap = {}
            # sql to get queues
            sqlQ = "SELECT queueName,nQueueLimitJob,nQueueLimitJobRatio,nQueueLimitJobMax,nQueueLimitJobMin "
            sqlQ += f"FROM {pandaQueueTableName} "
            sqlQ += "WHERE jobFetchTime IS NULL OR jobFetchTime<:timeLimit "
            sqlQ += "ORDER BY jobFetchTime "
            # sql to count nQueue
            sqlN = f"SELECT COUNT(*) cnt,status FROM {jobTableName} "
            sqlN += "WHERE computingSite=:computingSite AND status IN (:status1,:status2) "
            sqlN += "GROUP BY status "
            # sql to update timestamp
            sqlU = f"UPDATE {pandaQueueTableName} SET jobFetchTime=:jobFetchTime "
            sqlU += "WHERE queueName=:queueName "
            sqlU += "AND (jobFetchTime IS NULL OR jobFetchTime<:timeLimit) "
            # get queues
            timeNow = core_utils.naive_utcnow()
            varMap = dict()
            varMap[":timeLimit"] = timeNow - datetime.timedelta(seconds=interval)
            self.execute(sqlQ, varMap)
            resQ = self.cur.fetchall()
            iQueues = 0
            for queueName, nQueueLimitJob, nQueueLimitJobRatio, nQueueLimitJobMax, nQueueLimitJobMin in resQ:
                # update timestamp to lock the queue
                varMap = dict()
                varMap[":queueName"] = queueName
                varMap[":jobFetchTime"] = timeNow
                varMap[":timeLimit"] = timeNow - datetime.timedelta(seconds=interval)
                self.execute(sqlU, varMap)
                nRow = self.cur.rowcount
                # commit
                self.commit()
                # skip if not locked
                if nRow == 0:
                    continue
                # count nQueue
                varMap = dict()
                varMap[":computingSite"] = queueName
                varMap[":status1"] = "starting"
                varMap[":status2"] = "running"
                self.execute(sqlN, varMap)
                resN = self.cur.fetchall()
                nsMap = dict()
                for tmpN, tmpStatus in resN:
                    nsMap[tmpStatus] = tmpN
                # get num of queued jobs
                try:
                    nQueue = nsMap["starting"]
                except Exception:
                    nQueue = 0
                # dynamic nQueueLimitJob
                if nQueueLimitJobRatio is not None and nQueueLimitJobRatio > 0:
                    try:
                        nRunning = nsMap["running"]
                    except Exception:
                        nRunning = 0
                    nQueueLimitJob = int(nRunning * nQueueLimitJobRatio / 100)
                    if nQueueLimitJobMin is None:
                        nQueueLimitJobMin = 1
                    nQueueLimitJob = max(nQueueLimitJob, nQueueLimitJobMin)
                    if nQueueLimitJobMax is not None:
                        nQueueLimitJob = min(nQueueLimitJob, nQueueLimitJobMax)
                # more jobs need to be queued
                if nQueueLimitJob is not None and nQueue < nQueueLimitJob:
                    retMap[queueName] = nQueueLimitJob - nQueue
                # enough queues
                iQueues += 1
                if iQueues >= n_queues:
                    break
            tmpLog.debug(f"got {str(retMap)}")
            return retMap
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(tmpLog)
            # return
            return {}

    # get number of jobs to fetch
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
        tmpLog = core_utils.make_logger(_logger, method_name="get_num_jobs_to_fetch")
        try:
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
            varMap = dict()
            varMap[":timeLimit"] = timeNow - datetime.timedelta(seconds=interval)
            self.execute(sqlQ, varMap)
            resQ = self.cur.fetchall()
            i_queues = 0
            for (queue_name,) in resQ:
                # update timestamp to lock the queue
                varMap = dict()
                varMap[":queueName"] = queue_name
                varMap[":jobFetchTime"] = timeNow
                varMap[":timeLimit"] = timeNow - datetime.timedelta(seconds=interval)
                self.execute(sqlU, varMap)
                nRow = self.cur.rowcount
                # commit
                self.commit()
                # skip if not locked
                if nRow == 0:
                    continue
                # count nQueue
                varMap = dict()
                varMap[":computingSite"] = queue_name
                varMap[":status1"] = "starting"
                varMap[":status2"] = "running"
                self.execute(sqlN, varMap)
                resN = self.cur.fetchall()
                job_stats_map = dict()
                for status in ["starting", "running"]:
                    job_stats_map.setdefault(status, {"n": 0, "core": 0})
                for status, cnt, corecount in resN:
                    job_stats_map[status]["n"] = cnt
                    job_stats_map[status]["core"] = corecount
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
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(tmpLog)
            # return
            return {}

    # get jobs to propagate checkpoints
    def get_jobs_to_propagate(self, max_jobs, lock_interval, update_interval, locked_by):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"thr={locked_by}", method_name="get_jobs_to_propagate")
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
            varMap = dict()
            varMap[":lockTimeLimit"] = lockTimeLimit
            varMap[":updateTimeLimit"] = updateTimeLimit
            self.execute(sql, varMap)
            resList = self.cur.fetchall()
            pandaIDs = []
            for (pandaID,) in resList:
                pandaIDs.append(pandaID)
            # partially randomise to increase success rate for lock
            nJobs = int(max_jobs * 0.2)
            subPandaIDs = list(pandaIDs[nJobs:])
            random.shuffle(subPandaIDs)
            pandaIDs = pandaIDs[:nJobs] + subPandaIDs
            pandaIDs = pandaIDs[:max_jobs]
            jobSpecList = []
            iEvents = 0
            for pandaID in pandaIDs:
                # avoid a bulk update for many jobs with too many events
                if iEvents > 10000:
                    break
                # lock job
                varMap = dict()
                varMap[":PandaID"] = pandaID
                varMap[":timeNow"] = timeNow
                varMap[":lockedBy"] = locked_by
                varMap[":lockTimeLimit"] = lockTimeLimit
                varMap[":updateTimeLimit"] = updateTimeLimit
                self.execute(sqlL, varMap)
                nRow = self.cur.rowcount
                # commit
                self.commit()
                if nRow > 0:
                    # read job
                    varMap = dict()
                    varMap[":PandaID"] = pandaID
                    self.execute(sqlJ, varMap)
                    res = self.cur.fetchone()
                    # make job
                    jobSpec = JobSpec()
                    jobSpec.pack(res)
                    jobSpec.propagatorLock = locked_by
                    zipFiles = {}
                    zipIdMap = dict()
                    # get zipIDs
                    varMap = dict()
                    varMap[":PandaID"] = jobSpec.PandaID
                    varMap[":statusFinished"] = "finished"
                    varMap[":statusFailed"] = "failed"
                    self.execute(sqlZ, varMap)
                    resZ = self.cur.fetchall()
                    for tmpFileID, tmpZipFileID in resZ:
                        zipIdMap[tmpFileID] = tmpZipFileID
                    # get zip files
                    varMap = dict()
                    varMap[":PandaID"] = jobSpec.PandaID
                    varMap[":statusFinished"] = "finished"
                    varMap[":statusFailed"] = "failed"
                    self.execute(sqlF, varMap)
                    resFs = self.cur.fetchall()
                    for resF in resFs:
                        fileSpec = FileSpec()
                        fileSpec.pack(resF)
                        zipFiles[fileSpec.fileID] = fileSpec
                    # read events
                    varMap = dict()
                    varMap[":PandaID"] = jobSpec.PandaID
                    varMap[":statusFinished"] = "finished"
                    varMap[":statusFailed"] = "failed"
                    self.execute(sqlE, varMap)
                    resEs = self.cur.fetchall()
                    for resE in resEs:
                        eventSpec = EventSpec()
                        eventSpec.pack(resE)
                        zipFileSpec = None
                        # get associated zip file if any
                        if eventSpec.fileID is not None:
                            if eventSpec.fileID not in zipIdMap:
                                continue
                            zipFileID = zipIdMap[eventSpec.fileID]
                            if zipFileID is not None:
                                zipFileSpec = zipFiles[zipFileID]
                        jobSpec.add_event(eventSpec, zipFileSpec)
                        iEvents += 1
                    # read checkpoint files
                    varMap = dict()
                    varMap[":PandaID"] = pandaID
                    varMap[":type"] = "checkpoint"
                    varMap[":status"] = "renewed"
                    self.execute(sqlC, varMap)
                    resC = self.cur.fetchall()
                    for resFile in resC:
                        fileSpec = FileSpec()
                        fileSpec.pack(resFile)
                        jobSpec.add_out_file(fileSpec)
                    # add to job list
                    jobSpecList.append(jobSpec)
            tmpLog.debug(f"got {len(jobSpecList)} jobs")
            return jobSpecList
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    # get jobs in sub status
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
        try:
            # get logger
            if locked_by is None:
                msgPfx = None
            else:
                msgPfx = f"id={locked_by}"
            tmpLog = core_utils.make_logger(_logger, msgPfx, method_name="get_jobs_in_sub_status")
            tmpLog.debug(f"start subStatus={sub_status} timeColumn={time_column}")
            timeNow = core_utils.naive_utcnow()
            # sql to count jobs being processed
            sqlC = f"SELECT COUNT(*) cnt FROM {jobTableName} "
            sqlC += f"WHERE ({lock_column} IS NOT NULL AND subStatus=:subStatus "
            if time_column is not None and interval_with_lock is not None:
                sqlC += "AND ({0} IS NOT NULL AND {0}>:lockTimeLimit) ".format(time_column)
            sqlC += ") OR subStatus=:newSubStatus "
            # count jobs
            if max_jobs > 0 and new_sub_status is not None:
                varMap = dict()
                varMap[":subStatus"] = sub_status
                varMap[":newSubStatus"] = new_sub_status
                if time_column is not None and interval_with_lock is not None:
                    varMap[":lockTimeLimit"] = timeNow - datetime.timedelta(seconds=interval_with_lock)
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
            if time_column is not None:
                sql += f"AND ({time_column} IS NULL "
                if interval_with_lock is not None:
                    sql += f"OR ({time_column}<:lockTimeLimit AND {lock_column} IS NOT NULL) "
                if interval_without_lock is not None:
                    sql += f"OR ({time_column}<:updateTimeLimit AND {lock_column} IS NULL) "
                sql += ") "
                sql += f"ORDER BY {time_column} "
            # sql to lock job
            sqlL = f"UPDATE {jobTableName} SET {time_column}=:timeNow,{lock_column}=:lockedBy "
            sqlL += "WHERE PandaID=:PandaID AND subStatus=:subStatus "
            if time_column is not None:
                sqlL += f"AND ({time_column} IS NULL "
                if interval_with_lock is not None:
                    sqlL += f"OR ({time_column}<:lockTimeLimit AND {lock_column} IS NOT NULL) "
                if interval_without_lock is not None:
                    sqlL += f"OR ({time_column}<:updateTimeLimit AND {lock_column} IS NULL) "
                sqlL += ") "
            # sql to get jobs
            sqlGJ = f"SELECT {JobSpec.column_names()} FROM {jobTableName} "
            sqlGJ += "WHERE PandaID=:PandaID "
            # sql to get file
            sqlGF = f"SELECT {FileSpec.column_names()} FROM {fileTableName} "
            sqlGF += "WHERE PandaID=:PandaID AND fileType=:type "
            if ng_file_status_list is not None:
                sqlGF += "AND status NOT IN ("
                for tmpStatus in ng_file_status_list:
                    tmpKey = f":status_{tmpStatus}"
                    sqlGF += f"{tmpKey},"
                sqlGF = sqlGF[:-1]
                sqlGF += ") "
            if max_files_per_job is not None and max_files_per_job > 0:
                sqlGF += f"LIMIT {max_files_per_job} "
            # get jobs
            varMap = dict()
            varMap[":subStatus"] = sub_status
            if interval_with_lock is not None:
                varMap[":lockTimeLimit"] = timeNow - datetime.timedelta(seconds=interval_with_lock)
            if interval_without_lock is not None:
                varMap[":updateTimeLimit"] = timeNow - datetime.timedelta(seconds=interval_without_lock)
            self.execute(sql, varMap)
            resList = self.cur.fetchall()
            pandaIDs = []
            for (pandaID,) in resList:
                pandaIDs.append(pandaID)
            # partially randomise to increase success rate for lock
            nJobs = int(max_jobs * 0.2)
            subPandaIDs = list(pandaIDs[nJobs:])
            random.shuffle(subPandaIDs)
            pandaIDs = pandaIDs[:nJobs] + subPandaIDs
            pandaIDs = pandaIDs[:max_jobs]
            jobSpecList = []
            for pandaID in pandaIDs:
                # lock job
                if locked_by is not None:
                    varMap = dict()
                    varMap[":PandaID"] = pandaID
                    varMap[":timeNow"] = timeNow
                    varMap[":lockedBy"] = locked_by
                    varMap[":subStatus"] = sub_status
                    if interval_with_lock is not None:
                        varMap[":lockTimeLimit"] = timeNow - datetime.timedelta(seconds=interval_with_lock)
                    if interval_without_lock is not None:
                        varMap[":updateTimeLimit"] = timeNow - datetime.timedelta(seconds=interval_without_lock)
                    self.execute(sqlL, varMap)
                    nRow = self.cur.rowcount
                    # commit
                    self.commit()
                else:
                    nRow = 1
                if nRow > 0:
                    # get job
                    varMap = dict()
                    varMap[":PandaID"] = pandaID
                    self.execute(sqlGJ, varMap)
                    resGJ = self.cur.fetchone()
                    # make job
                    jobSpec = JobSpec()
                    jobSpec.pack(resGJ)
                    if locked_by is not None:
                        jobSpec.lockedBy = locked_by
                        setattr(jobSpec, time_column, timeNow)
                    # get files
                    varMap = dict()
                    varMap[":PandaID"] = jobSpec.PandaID
                    if jobSpec.auxInput in [None, JobSpec.AUX_hasAuxInput, JobSpec.AUX_allTriggered]:
                        varMap[":type"] = "input"
                    else:
                        varMap[":type"] = FileSpec.AUX_INPUT
                    if ng_file_status_list is not None:
                        for tmpStatus in ng_file_status_list:
                            tmpKey = f":status_{tmpStatus}"
                            varMap[tmpKey] = tmpStatus
                    self.execute(sqlGF, varMap)
                    resGF = self.cur.fetchall()
                    for resFile in resGF:
                        fileSpec = FileSpec()
                        fileSpec.pack(resFile)
                        jobSpec.add_in_file(fileSpec)
                    # append
                    jobSpecList.append(jobSpec)
            tmpLog.debug(f"got {len(jobSpecList)} jobs")
            return jobSpecList
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    # release jobs
    def release_jobs(self, panda_ids, locked_by):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="release_jobs")
            tmpLog.debug(f"start for {len(panda_ids)} jobs")
            # sql to release job
            sql = f"UPDATE {jobTableName} SET lockedBy=NULL "
            sql += "WHERE PandaID=:pandaID AND lockedBy=:lockedBy "
            nJobs = 0
            for pandaID in panda_ids:
                varMap = dict()
                varMap[":pandaID"] = pandaID
                varMap[":lockedBy"] = locked_by
                self.execute(sql, varMap)
                if self.cur.rowcount > 0:
                    nJobs += 1
            # commit
            self.commit()
            tmpLog.debug(f"released {nJobs} jobs")
            # return
            return True
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # lock job again
    def lock_job_again(self, panda_id, time_column, lock_column, locked_by):
        try:
            tmpLog = core_utils.make_logger(_logger, f"PandaID={panda_id}", method_name="lock_job_again")
            tmpLog.debug(f"start column={lock_column} id={locked_by}")
            # check lock
            sqlC = f"SELECT {lock_column},{time_column} FROM {jobTableName} "
            sqlC += "WHERE PandaID=:pandaID "
            sqlC += "FOR UPDATE "
            varMap = dict()
            varMap[":pandaID"] = panda_id
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
                    sqlU = f"UPDATE {jobTableName} SET {time_column}=:timeNow WHERE pandaID=:pandaID "
                    varMap = dict()
                    varMap[":pandaID"] = panda_id
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

    # increment submission attempt
    def increment_submission_attempt(self, panda_id, new_number):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"pandaID={panda_id}", method_name="increment_submission_attempt")
            tmpLog.debug(f"start with newNum={new_number}")
            # sql to update attempt number
            sqlL = f"UPDATE {jobTableName} SET submissionAttempts=:newNum "
            sqlL += "WHERE PandaID=:PandaID "
            varMap = dict()
            varMap[":PandaID"] = panda_id
            varMap[":newNum"] = new_number
            self.execute(sqlL, varMap)
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

    # delete old jobs
    def delete_old_jobs(self, timeout):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"timeout={timeout}", method_name="delete_old_jobs")
            tmpLog.debug("start")
            # sql to get old jobs to be deleted
            sqlGJ = f"SELECT PandaID FROM {jobTableName} "
            sqlGJ += "WHERE subStatus=:subStatus AND propagatorTime IS NULL "
            sqlGJ += "AND ((modificationTime IS NOT NULL AND modificationTime<:timeLimit1) "
            sqlGJ += "OR (modificationTime IS NULL AND creationTime<:timeLimit2)) "
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
            # get jobs
            varMap = dict()
            varMap[":subStatus"] = "done"
            varMap[":timeLimit1"] = core_utils.naive_utcnow() - datetime.timedelta(hours=timeout)
            varMap[":timeLimit2"] = core_utils.naive_utcnow() - datetime.timedelta(hours=timeout * 2)
            self.execute(sqlGJ, varMap)
            resGJ = self.cur.fetchall()
            nDel = 0
            for (pandaID,) in resGJ:
                varMap = dict()
                varMap[":PandaID"] = pandaID
                # delete job
                self.execute(sqlDJ, varMap)
                iDel = self.cur.rowcount
                if iDel > 0:
                    nDel += iDel
                    # delete files
                    self.execute(sqlDF, varMap)
                    # delete events
                    self.execute(sqlDE, varMap)
                    # delete relations
                    self.execute(sqlDR, varMap)
                # commit
                self.commit()
            tmpLog.debug(f"deleted {nDel} jobs")
            return True
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # disable multi workers
    def disable_multi_workers(self, panda_id):
        tmpLog = None
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"PandaID={panda_id}", method_name="disable_multi_workers")
            tmpLog.debug("start")
            # sql to update flag
            sqlJ = f"UPDATE {jobTableName} SET moreWorkers=0 "
            sqlJ += "WHERE PandaID=:pandaID AND nWorkers IS NOT NULL AND nWorkersLimit IS NOT NULL "
            sqlJ += "AND nWorkers>0 "
            # set flag
            varMap = dict()
            varMap[":pandaID"] = panda_id
            self.execute(sqlJ, varMap)
            nRow = self.cur.rowcount
            # commit
            self.commit()
            tmpLog.debug(f"done with {nRow}")
            # return
            return nRow
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(tmpLog)
            # return
            return None

    # delete orphaned job info
    def delete_orphaned_job_info(self):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="delete_orphaned_job_info")
            tmpLog.debug("start")
            # sql to get job info to be deleted
            sqlGJ = "SELECT PandaID FROM {0} "
            sqlGJ += "WHERE PandaID NOT IN ("
            sqlGJ += "SELECT PandaID FROM {1}) "
            # sql to delete job info
            sqlDJ = "DELETE FROM {0} "
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
            # loop over all tables
            for tableName in [fileTableName, eventTableName, jobWorkerTableName]:
                # get job info
                self.execute(sqlGJ.format(tableName, jobTableName))
                resGJ = self.cur.fetchall()
                nDel = 0
                for (pandaID,) in resGJ:
                    # delete
                    varMap = dict()
                    varMap[":PandaID"] = pandaID
                    self.execute(sqlDJ.format(tableName), varMap)
                    iDel = self.cur.rowcount
                    if iDel > 0:
                        nDel += iDel
                    # commit
                    self.commit()
                tmpLog.debug(f"deleted {nDel} records from {tableName}")
            return True
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False
