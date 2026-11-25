"""
DB proxy module for operations spanning jobs and workers together

"""

import datetime

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_mods.base_module import (
    BaseModule,
    _logger,
    eventTableName,
    fileTableName,
    jobTableName,
    jobWorkerTableName,
    workTableName,
)
from pandaharvester.harvestercore.event_spec import EventSpec
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.job_worker_relation_spec import JobWorkerRelationSpec
from pandaharvester.harvestercore.work_spec import WorkSpec


# DB proxy module for operations spanning jobs and workers together
class JobWorkerModule(BaseModule):
    # get job chunks to make workers
    def get_job_chunks_for_workers(
        self,
        queue_name,
        n_workers,
        n_ready,
        n_jobs_per_worker,
        n_workers_per_job,
        use_job_late_binding,
        check_interval,
        lock_interval,
        locked_by,
        allow_job_mixture=False,
        max_workers_per_job_in_total=None,
        max_workers_per_job_per_cycle=None,
    ):
        toCommit = False
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"queue={queue_name}", method_name="get_job_chunks_for_workers")
            tmpLog.debug("start")
            # define maxJobs
            if n_jobs_per_worker is not None:
                maxJobs = (n_workers + n_ready) * n_jobs_per_worker
            else:
                maxJobs = -(-(n_workers + n_ready) // n_workers_per_job)
            # core part of sql
            # submitted and running are for multi-workers
            sqlCore = "WHERE (subStatus IN (:subStat1,:subStat2) OR (subStatus IN (:subStat3,:subStat4) "
            sqlCore += "AND nWorkers IS NOT NULL AND nWorkersLimit IS NOT NULL AND nWorkers<nWorkersLimit "
            sqlCore += "AND moreWorkers IS NULL AND (maxWorkersInTotal IS NULL OR nWorkersInTotal IS NULL "
            sqlCore += "OR nWorkersInTotal<maxWorkersInTotal))) "
            sqlCore += "AND (submitterTime IS NULL "
            sqlCore += "OR (submitterTime<:lockTimeLimit AND lockedBy IS NOT NULL) "
            sqlCore += "OR (submitterTime<:checkTimeLimit AND lockedBy IS NULL)) "
            sqlCore += "AND computingSite=:queueName "
            # sql to get job IDs
            sqlP = f"SELECT PandaID FROM {jobTableName} "
            sqlP += sqlCore
            sqlP += "ORDER BY currentPriority DESC,taskID,PandaID "
            # sql to get job
            sqlJ = f"SELECT {JobSpec.column_names()} FROM {jobTableName} "
            sqlJ += "WHERE PandaID=:PandaID "
            # sql to lock job
            sqlL = f"UPDATE {jobTableName} SET submitterTime=:timeNow,lockedBy=:lockedBy "
            sqlL += sqlCore
            sqlL += "AND PandaID=:PandaID "
            timeNow = core_utils.naive_utcnow()
            lockTimeLimit = timeNow - datetime.timedelta(seconds=lock_interval)
            checkTimeLimit = timeNow - datetime.timedelta(seconds=check_interval)
            # sql to get file
            sqlGF = f"SELECT {FileSpec.column_names()} FROM {fileTableName} "
            sqlGF += "WHERE PandaID=:PandaID AND fileType IN (:type1,:type2) "
            jobChunkList = []
            # count jobs for nJobsPerWorker>1
            nAvailableJobs = None
            if n_jobs_per_worker is not None and n_jobs_per_worker > 1:
                toCommit = True
                # sql to count jobs
                sqlC = f"SELECT COUNT(*) cnt FROM {jobTableName} "
                sqlC += sqlCore
                # count jobs
                varMap = dict()
                varMap[":subStat1"] = "prepared"
                varMap[":subStat2"] = "queued"
                varMap[":subStat3"] = "submitted"
                varMap[":subStat4"] = "running"
                varMap[":queueName"] = queue_name
                varMap[":lockTimeLimit"] = lockTimeLimit
                varMap[":checkTimeLimit"] = checkTimeLimit
                self.execute(sqlC, varMap)
                (nAvailableJobs,) = self.cur.fetchone()
                maxJobs = min(maxJobs, nAvailableJobs)
                # only full chunks unless there are not enough jobs for even one chunk
                if maxJobs >= n_jobs_per_worker:
                    maxJobs = maxJobs // n_jobs_per_worker * n_jobs_per_worker
            tmpStr = f"n_workers={n_workers} n_ready={n_ready} "
            tmpStr += f"n_jobs_per_worker={n_jobs_per_worker} n_workers_per_job={n_workers_per_job} "
            tmpStr += f"n_ava_jobs={nAvailableJobs}"
            tmpLog.debug(tmpStr)
            if maxJobs == 0:
                tmpStr = "skip due to maxJobs=0"
                tmpLog.debug(tmpStr)
            else:
                # get job IDs
                varMap = dict()
                varMap[":subStat1"] = "prepared"
                varMap[":subStat2"] = "queued"
                varMap[":subStat3"] = "submitted"
                varMap[":subStat4"] = "running"
                varMap[":queueName"] = queue_name
                varMap[":lockTimeLimit"] = lockTimeLimit
                varMap[":checkTimeLimit"] = checkTimeLimit
                self.execute(sqlP, varMap)
                resP = self.cur.fetchall()
                tmpStr = f"fetched {len(resP)} jobs"
                tmpLog.debug(tmpStr)
                jobChunk = []
                iJobs = 0
                for (pandaID,) in resP:
                    toCommit = True
                    toEscape = False
                    # lock job
                    varMap = dict()
                    varMap[":subStat1"] = "prepared"
                    varMap[":subStat2"] = "queued"
                    varMap[":subStat3"] = "submitted"
                    varMap[":subStat4"] = "running"
                    varMap[":queueName"] = queue_name
                    varMap[":lockTimeLimit"] = lockTimeLimit
                    varMap[":checkTimeLimit"] = checkTimeLimit
                    varMap[":PandaID"] = pandaID
                    varMap[":timeNow"] = timeNow
                    varMap[":lockedBy"] = locked_by
                    self.execute(sqlL, varMap)
                    nRow = self.cur.rowcount
                    if nRow > 0:
                        iJobs += 1
                        # get job
                        varMap = dict()
                        varMap[":PandaID"] = pandaID
                        self.execute(sqlJ, varMap)
                        resJ = self.cur.fetchone()
                        # make job
                        jobSpec = JobSpec()
                        jobSpec.pack(resJ)
                        jobSpec.lockedBy = locked_by
                        # get files
                        varMap = dict()
                        varMap[":PandaID"] = pandaID
                        varMap[":type1"] = "input"
                        varMap[":type2"] = FileSpec.AUX_INPUT
                        self.execute(sqlGF, varMap)
                        resGF = self.cur.fetchall()
                        for resFile in resGF:
                            fileSpec = FileSpec()
                            fileSpec.pack(resFile)
                            jobSpec.add_in_file(fileSpec)
                        # new chunk
                        if len(jobChunk) > 0 and jobChunk[0].taskID != jobSpec.taskID and not allow_job_mixture:
                            tmpLog.debug(f"new chunk with {len(jobChunk)} jobs due to taskID change")
                            jobChunkList.append(jobChunk)
                            jobChunk = []
                        # only prepared for new worker
                        if len(jobChunkList) >= n_ready and jobSpec.subStatus == "queued":
                            toCommit = False
                        else:
                            jobChunk.append(jobSpec)
                            # enough jobs in chunk
                            if n_jobs_per_worker is not None and len(jobChunk) >= n_jobs_per_worker:
                                tmpLog.debug(f"new chunk with {len(jobChunk)} jobs due to n_jobs_per_worker")
                                jobChunkList.append(jobChunk)
                                jobChunk = []
                            # one job per multiple workers
                            elif n_workers_per_job is not None:
                                if jobSpec.nWorkersLimit is None:
                                    jobSpec.nWorkersLimit = n_workers_per_job
                                if max_workers_per_job_in_total is not None:
                                    jobSpec.maxWorkersInTotal = max_workers_per_job_in_total
                                nMultiWorkers = min(jobSpec.nWorkersLimit - jobSpec.nWorkers, n_workers - len(jobChunkList))
                                if jobSpec.maxWorkersInTotal is not None and jobSpec.nWorkersInTotal is not None:
                                    nMultiWorkers = min(nMultiWorkers, jobSpec.maxWorkersInTotal - jobSpec.nWorkersInTotal)
                                if max_workers_per_job_per_cycle is not None:
                                    nMultiWorkers = min(nMultiWorkers, max_workers_per_job_per_cycle)
                                if nMultiWorkers < 0:
                                    nMultiWorkers = 0
                                tmpLog.debug(f"new {nMultiWorkers} chunks with {len(jobChunk)} jobs due to n_workers_per_job")
                                for i in range(nMultiWorkers):
                                    jobChunkList.append(jobChunk)
                                jobChunk = []
                            # enough job chunks
                            if len(jobChunkList) >= n_workers:
                                toEscape = True
                    if toCommit:
                        self.commit()
                    else:
                        self.rollback()
                    if toEscape or iJobs >= maxJobs:
                        break
                # at least one chunk even if not enough jobs to fill it
                if len(jobChunk) > 0 and len(jobChunkList) == 0:
                    tmpLog.debug(f"new chunk with {len(jobChunk)} jobs due to lack of jobs")
                    jobChunkList.append(jobChunk)
            tmpLog.debug(f"got {len(jobChunkList)} job chunks")
            return jobChunkList
        except Exception:
            # roll back
            if toCommit:
                self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    # update jobs and workers
    def update_jobs_workers(self, jobspec_list, workspec_list, locked_by, panda_ids_list=None):
        try:
            timeNow = core_utils.naive_utcnow()
            # sql to check job
            sqlCJ = f"SELECT status FROM {jobTableName} WHERE PandaID=:PandaID FOR UPDATE "
            # sql to check file
            sqlFC = f"SELECT {FileSpec.column_names()} FROM {fileTableName} "
            sqlFC += "WHERE PandaID=:PandaID AND lfn=:lfn "
            # sql to get all LFNs
            sqlFL = f"SELECT lfn,fileID FROM {fileTableName} "
            sqlFL += "WHERE PandaID=:PandaID AND fileType<>:type "
            # sql to check file with eventRangeID
            sqlFE = f"SELECT 1 c FROM {fileTableName} "
            sqlFE += "WHERE PandaID=:PandaID AND lfn=:lfn AND eventRangeID=:eventRangeID ".format(fileTableName)
            # sql to insert file
            sqlFI = f"INSERT INTO {fileTableName} ({FileSpec.column_names()}) "
            sqlFI += FileSpec.bind_values_expression()
            # sql to get pending files
            sqlFP = f"SELECT fileID,fsize,lfn FROM {fileTableName} "
            sqlFP += "WHERE PandaID=:PandaID AND status=:status AND fileType<>:type "
            # sql to get provenanceID,workerID for pending files
            sqlPW = f"SELECT SUM(fsize),provenanceID,workerID FROM {fileTableName} "
            sqlPW += "WHERE PandaID=:PandaID AND status=:status AND fileType<>:type "
            sqlPW += "GROUP BY provenanceID,workerID "
            # sql to update pending files
            sqlFU = f"UPDATE {fileTableName} "
            sqlFU += "SET status=:status,zipFileID=:zipFileID "
            sqlFU += "WHERE fileID=:fileID "
            # sql to check event
            sqlEC = f"SELECT eventRangeID,eventStatus FROM {eventTableName} "
            sqlEC += "WHERE PandaID=:PandaID AND eventRangeID IS NOT NULL "
            # sql to check associated file
            sqlEF = f"SELECT eventRangeID,status FROM {fileTableName} "
            sqlEF += "WHERE PandaID=:PandaID AND eventRangeID IS NOT NULL "
            # sql to insert event
            sqlEI = f"INSERT INTO {eventTableName} ({EventSpec.column_names()}) "
            sqlEI += EventSpec.bind_values_expression()
            # sql to update event
            sqlEU = f"UPDATE {eventTableName} "
            sqlEU += "SET eventStatus=:eventStatus,subStatus=:subStatus "
            sqlEU += "WHERE PandaID=:PandaID AND eventRangeID=:eventRangeID "
            # sql to check if relationship is already available
            sqlCR = f"SELECT 1 c FROM {jobWorkerTableName} WHERE PandaID=:PandaID AND workerID=:workerID "
            # sql to insert job and worker relationship
            sqlIR = f"INSERT INTO {jobWorkerTableName} ({JobWorkerRelationSpec.column_names()}) "
            sqlIR += JobWorkerRelationSpec.bind_values_expression()
            # count number of workers
            sqlNW = f"SELECT DISTINCT t.workerID FROM {jobWorkerTableName} t, {workTableName} w "
            sqlNW += "WHERE t.PandaID=:PandaID AND w.workerID=t.workerID "
            sqlNW += "AND w.status IN (:st_submitted,:st_running,:st_idle) "
            # update job
            if jobspec_list is not None:
                if len(workspec_list) > 0 and workspec_list[0].mapType == WorkSpec.MT_MultiWorkers:
                    isMultiWorkers = True
                else:
                    isMultiWorkers = False
                for jobSpec in jobspec_list:
                    tmpLog = core_utils.make_logger(_logger, f"PandaID={jobSpec.PandaID} by {locked_by}", method_name="update_jobs_workers")
                    # check job
                    varMap = dict()
                    varMap[":PandaID"] = jobSpec.PandaID
                    self.execute(sqlCJ, varMap)
                    resCJ = self.cur.fetchone()
                    (tmpJobStatus,) = resCJ
                    # don't update cancelled jobs
                    if tmpJobStatus == ["cancelled"]:
                        pass
                    else:
                        # get nWorkers
                        tmpLog.debug("start")
                        activeWorkers = set()
                        if isMultiWorkers:
                            varMap = dict()
                            varMap[":PandaID"] = jobSpec.PandaID
                            varMap[":st_submitted"] = WorkSpec.ST_submitted
                            varMap[":st_running"] = WorkSpec.ST_running
                            varMap[":st_idle"] = WorkSpec.ST_idle
                            self.execute(sqlNW, varMap)
                            resNW = self.cur.fetchall()
                            for (tmpWorkerID,) in resNW:
                                activeWorkers.add(tmpWorkerID)
                            jobSpec.nWorkers = len(activeWorkers)
                        # get all LFNs
                        allLFNs = dict()
                        varMap = dict()
                        varMap[":PandaID"] = jobSpec.PandaID
                        varMap[":type"] = "input"
                        self.execute(sqlFL, varMap)
                        resFL = self.cur.fetchall()
                        for tmpLFN, tmpFileID in resFL:
                            allLFNs[tmpLFN] = tmpFileID
                        # insert files
                        nFiles = 0
                        fileIdMap = {}
                        zipFileRes = dict()
                        for fileSpec in jobSpec.outFiles:
                            # insert file
                            if fileSpec.lfn not in allLFNs:
                                if jobSpec.zipPerMB is None or fileSpec.isZip in [0, 1]:
                                    if fileSpec.fileType != "checkpoint":
                                        fileSpec.status = "defined"
                                        jobSpec.hasOutFile = JobSpec.HO_hasOutput
                                    else:
                                        fileSpec.status = "renewed"
                                else:
                                    fileSpec.status = "pending"
                                varMap = fileSpec.values_list()
                                self.execute(sqlFI, varMap)
                                fileSpec.fileID = self.cur.lastrowid
                                nFiles += 1
                                # mapping between event range ID and file ID
                                if fileSpec.eventRangeID is not None:
                                    fileIdMap[fileSpec.eventRangeID] = fileSpec.fileID
                                # associate to itself
                                if fileSpec.isZip == 1:
                                    varMap = dict()
                                    varMap[":status"] = fileSpec.status
                                    varMap[":fileID"] = fileSpec.fileID
                                    varMap[":zipFileID"] = fileSpec.fileID
                                    self.execute(sqlFU, varMap)
                            elif fileSpec.isZip == 1 and fileSpec.eventRangeID is not None:
                                # add a fake file with eventRangeID which has the same lfn/zipFileID as zip file
                                varMap = dict()
                                varMap[":PandaID"] = fileSpec.PandaID
                                varMap[":lfn"] = fileSpec.lfn
                                varMap[":eventRangeID"] = fileSpec.eventRangeID
                                self.execute(sqlFE, varMap)
                                resFE = self.cur.fetchone()
                                if resFE is None:
                                    if fileSpec.lfn not in zipFileRes:
                                        # get file
                                        varMap = dict()
                                        varMap[":PandaID"] = fileSpec.PandaID
                                        varMap[":lfn"] = fileSpec.lfn
                                        self.execute(sqlFC, varMap)
                                        resFC = self.cur.fetchone()
                                        zipFileRes[fileSpec.lfn] = resFC
                                    # associate to existing zip
                                    resFC = zipFileRes[fileSpec.lfn]
                                    zipFileSpec = FileSpec()
                                    zipFileSpec.pack(resFC)
                                    fileSpec.status = "zipped"
                                    fileSpec.zipFileID = zipFileSpec.zipFileID
                                    varMap = fileSpec.values_list()
                                    self.execute(sqlFI, varMap)
                                    nFiles += 1
                                    # mapping between event range ID and file ID
                                    fileIdMap[fileSpec.eventRangeID] = self.cur.lastrowid
                            elif fileSpec.fileType == "checkpoint":
                                # reset status of checkpoint to be uploaded again
                                varMap = dict()
                                varMap[":status"] = "renewed"
                                varMap[":fileID"] = allLFNs[fileSpec.lfn]
                                varMap[":zipFileID"] = None
                                self.execute(sqlFU, varMap)
                        if nFiles > 0:
                            tmpLog.debug(f"inserted {nFiles} files")
                        # check pending files
                        if jobSpec.zipPerMB is not None and not (jobSpec.zipPerMB == 0 and jobSpec.subStatus != "to_transfer"):
                            # get workerID and provenanceID of pending files
                            zippedFileIDs = []
                            varMap = dict()
                            varMap[":PandaID"] = jobSpec.PandaID
                            varMap[":status"] = "pending"
                            varMap[":type"] = "input"
                            self.execute(sqlPW, varMap)
                            resPW = self.cur.fetchall()
                            for subTotalSize, tmpProvenanceID, tmpWorkerID in resPW:
                                if (
                                    jobSpec.subStatus == "to_transfer"
                                    or (jobSpec.zipPerMB > 0 and subTotalSize > jobSpec.zipPerMB * 1024 * 1024)
                                    or (tmpWorkerID is not None and tmpWorkerID not in activeWorkers)
                                ):
                                    sqlFPx = sqlFP
                                    varMap = dict()
                                    varMap[":PandaID"] = jobSpec.PandaID
                                    varMap[":status"] = "pending"
                                    varMap[":type"] = "input"
                                    if tmpProvenanceID is None:
                                        sqlFPx += "AND provenanceID IS NULL "
                                    else:
                                        varMap[":provenanceID"] = tmpProvenanceID
                                        sqlFPx += "AND provenanceID=:provenanceID "
                                    if tmpWorkerID is None:
                                        sqlFPx += "AND workerID IS NULL "
                                    else:
                                        varMap[":workerID"] = tmpWorkerID
                                        sqlFPx += "AND workerID=:workerID"
                                    # get pending files
                                    self.execute(sqlFPx, varMap)
                                    resFP = self.cur.fetchall()
                                    tmpLog.debug(f"got {len(resFP)} pending files for workerID={tmpWorkerID} provenanceID={tmpProvenanceID}")
                                    # make subsets
                                    subTotalSize = 0
                                    subFileIDs = []
                                    for tmpFileID, tmpFsize, tmpLFN in resFP:
                                        if jobSpec.zipPerMB > 0 and subTotalSize > 0 and (subTotalSize + tmpFsize > jobSpec.zipPerMB * 1024 * 1024):
                                            zippedFileIDs.append(subFileIDs)
                                            subFileIDs = []
                                            subTotalSize = 0
                                        subTotalSize += tmpFsize
                                        subFileIDs.append((tmpFileID, tmpLFN))
                                    if (
                                        jobSpec.subStatus == "to_transfer"
                                        or (jobSpec.zipPerMB > 0 and subTotalSize > jobSpec.zipPerMB * 1024 * 1024)
                                        or (tmpWorkerID is not None and tmpWorkerID not in activeWorkers)
                                    ) and len(subFileIDs) > 0:
                                        zippedFileIDs.append(subFileIDs)
                            # make zip files
                            for subFileIDs in zippedFileIDs:
                                # insert zip file
                                fileSpec = FileSpec()
                                fileSpec.status = "zipping"
                                fileSpec.lfn = "panda." + subFileIDs[0][-1] + ".zip"
                                fileSpec.scope = "panda"
                                fileSpec.fileType = "zip_output"
                                fileSpec.PandaID = jobSpec.PandaID
                                fileSpec.taskID = jobSpec.taskID
                                fileSpec.isZip = 1
                                varMap = fileSpec.values_list()
                                self.execute(sqlFI, varMap)
                                # update pending files
                                varMaps = []
                                for tmpFileID, tmpLFN in subFileIDs:
                                    varMap = dict()
                                    varMap[":status"] = "zipped"
                                    varMap[":fileID"] = tmpFileID
                                    varMap[":zipFileID"] = self.cur.lastrowid
                                    varMaps.append(varMap)
                                self.executemany(sqlFU, varMaps)
                            # set zip output flag
                            if len(zippedFileIDs) > 0:
                                jobSpec.hasOutFile = JobSpec.HO_hasZipOutput
                        # get event ranges and file stat
                        eventFileStat = dict()
                        eventRangesSet = set()
                        doneEventRangesSet = set()
                        if len(jobSpec.events) > 0:
                            # get event ranges
                            varMap = dict()
                            varMap[":PandaID"] = jobSpec.PandaID
                            self.execute(sqlEC, varMap)
                            resEC = self.cur.fetchall()
                            for tmpEventRangeID, tmpEventStatus in resEC:
                                if tmpEventStatus in ["running"]:
                                    eventRangesSet.add(tmpEventRangeID)
                                else:
                                    doneEventRangesSet.add(tmpEventRangeID)
                            # check associated file
                            varMap = dict()
                            varMap[":PandaID"] = jobSpec.PandaID
                            self.execute(sqlEF, varMap)
                            resEF = self.cur.fetchall()
                            for tmpEventRangeID, tmpStat in resEF:
                                eventFileStat[tmpEventRangeID] = tmpStat
                        # insert or update events
                        varMapsEI = []
                        varMapsEU = []
                        for eventSpec in jobSpec.events:
                            # already done
                            if eventSpec.eventRangeID in doneEventRangesSet:
                                continue
                            # set subStatus
                            if eventSpec.eventStatus == "finished":
                                # check associated file
                                if eventSpec.eventRangeID not in eventFileStat or eventFileStat[eventSpec.eventRangeID] == "finished":
                                    eventSpec.subStatus = "finished"
                                elif eventFileStat[eventSpec.eventRangeID] == "failed":
                                    eventSpec.eventStatus = "failed"
                                    eventSpec.subStatus = "failed"
                                else:
                                    eventSpec.subStatus = "transferring"
                            else:
                                eventSpec.subStatus = eventSpec.eventStatus
                            # set fileID
                            if eventSpec.eventRangeID in fileIdMap:
                                eventSpec.fileID = fileIdMap[eventSpec.eventRangeID]
                            # insert or update event
                            if eventSpec.eventRangeID not in eventRangesSet:
                                varMap = eventSpec.values_list()
                                varMapsEI.append(varMap)
                            else:
                                varMap = dict()
                                varMap[":PandaID"] = jobSpec.PandaID
                                varMap[":eventRangeID"] = eventSpec.eventRangeID
                                varMap[":eventStatus"] = eventSpec.eventStatus
                                varMap[":subStatus"] = eventSpec.subStatus
                                varMapsEU.append(varMap)
                        if len(varMapsEI) > 0:
                            self.executemany(sqlEI, varMapsEI)
                            tmpLog.debug(f"inserted {len(varMapsEI)} event")
                        if len(varMapsEU) > 0:
                            self.executemany(sqlEU, varMapsEU)
                            tmpLog.debug(f"updated {len(varMapsEU)} event")
                        # update job
                        varMap = jobSpec.values_map(only_changed=True)
                        if len(varMap) > 0:
                            tmpLog.debug("update job")
                            # sql to update job
                            sqlJ = f"UPDATE {jobTableName} SET {jobSpec.bind_update_changes_expression()} "
                            sqlJ += "WHERE PandaID=:PandaID "
                            jobSpec.lockedBy = None
                            jobSpec.modificationTime = timeNow
                            varMap = jobSpec.values_map(only_changed=True)
                            varMap[":PandaID"] = jobSpec.PandaID
                            self.execute(sqlJ, varMap)
                            nRow = self.cur.rowcount
                            tmpLog.debug(f"done with {nRow}")
                        tmpLog.debug("all done for job")
                    # commit
                    self.commit()
            # update worker
            retVal = True
            for idxW, workSpec in enumerate(workspec_list):
                tmpLog = core_utils.make_logger(_logger, f"workerID={workSpec.workerID}", method_name="update_jobs_workers")
                tmpLog.debug("update worker")
                workSpec.lockedBy = None
                if workSpec.status == WorkSpec.ST_running and workSpec.startTime is None:
                    workSpec.startTime = timeNow
                elif workSpec.is_final_status():
                    if workSpec.startTime is None:
                        workSpec.startTime = timeNow
                    if workSpec.endTime is None:
                        workSpec.endTime = timeNow
                if not workSpec.nextLookup:
                    if workSpec.has_updated_attributes():
                        workSpec.modificationTime = timeNow
                else:
                    workSpec.nextLookup = False
                # sql to update worker
                sqlW = f"UPDATE {workTableName} SET {workSpec.bind_update_changes_expression()} "
                sqlW += "WHERE workerID=:workerID AND lockedBy=:cr_lockedBy "
                sqlW += "AND (status NOT IN (:st1,:st2,:st3,:st4)) "
                varMap = workSpec.values_map(only_changed=True)
                if len(varMap) > 0:
                    varMap[":workerID"] = workSpec.workerID
                    varMap[":cr_lockedBy"] = locked_by
                    varMap[":st1"] = WorkSpec.ST_cancelled
                    varMap[":st2"] = WorkSpec.ST_finished
                    varMap[":st3"] = WorkSpec.ST_failed
                    varMap[":st4"] = WorkSpec.ST_missed
                    self.execute(sqlW, varMap)
                    nRow = self.cur.rowcount
                    tmpLog.debug(f"done with {nRow}")
                    if nRow == 0:
                        retVal = False
                # insert relationship if necessary
                if panda_ids_list is not None and len(panda_ids_list) > idxW:
                    varMapsIR = []
                    for pandaID in panda_ids_list[idxW]:
                        varMap = dict()
                        varMap[":PandaID"] = pandaID
                        varMap[":workerID"] = workSpec.workerID
                        self.execute(sqlCR, varMap)
                        resCR = self.cur.fetchone()
                        if resCR is None:
                            jwRelation = JobWorkerRelationSpec()
                            jwRelation.PandaID = pandaID
                            jwRelation.workerID = workSpec.workerID
                            varMap = jwRelation.values_list()
                            varMapsIR.append(varMap)
                    if len(varMapsIR) > 0:
                        self.executemany(sqlIR, varMapsIR)
                tmpLog.debug("all done for worker")
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

    # get jobs with workerID
    def get_jobs_with_worker_id(self, worker_id, locked_by, with_file=False, only_running=False, slim=False):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"workerID={worker_id}", method_name="get_jobs_with_worker_id")
            tmpLog.debug("start")
            # sql to get PandaIDs
            sqlP = f"SELECT PandaID FROM {jobWorkerTableName} "
            sqlP += "WHERE workerID=:workerID "
            # sql to get jobs
            sqlJ = f"SELECT {JobSpec.column_names(slim=slim)} FROM {jobTableName} "
            sqlJ += "WHERE PandaID=:PandaID "
            # sql to get job parameters
            sqlJJ = f"SELECT jobParams FROM {jobTableName} "
            sqlJJ += "WHERE PandaID=:PandaID "
            # sql to lock job
            sqlL = f"UPDATE {jobTableName} SET modificationTime=:timeNow,lockedBy=:lockedBy "
            sqlL += "WHERE PandaID=:PandaID "
            # sql to get files
            sqlF = f"SELECT {FileSpec.column_names()} FROM {fileTableName} "
            sqlF += "WHERE PandaID=:PandaID AND zipFileID IS NULL "
            # get jobs
            jobChunkList = []
            timeNow = core_utils.naive_utcnow()
            varMap = dict()
            varMap[":workerID"] = worker_id
            self.execute(sqlP, varMap)
            resW = self.cur.fetchall()
            for (pandaID,) in resW:
                # get job
                varMap = dict()
                varMap[":PandaID"] = pandaID
                self.execute(sqlJ, varMap)
                resJ = self.cur.fetchone()
                # make job
                jobSpec = JobSpec()
                jobSpec.pack(resJ, slim=slim)
                if only_running and jobSpec.subStatus not in ["running", "submitted", "queued", "idle"]:
                    continue
                jobSpec.lockedBy = locked_by
                # for old jobs without extractions
                if jobSpec.jobParamsExtForLog is None:
                    varMap = dict()
                    varMap[":PandaID"] = pandaID
                    self.execute(sqlJJ, varMap)
                    resJJ = self.cur.fetchone()
                    jobSpec.set_blob_attribute("jobParams", resJJ[0])
                    jobSpec.get_output_file_attributes()
                    jobSpec.get_logfile_info()
                # lock job
                if locked_by is not None:
                    varMap = dict()
                    varMap[":PandaID"] = pandaID
                    varMap[":lockedBy"] = locked_by
                    varMap[":timeNow"] = timeNow
                    self.execute(sqlL, varMap)
                # get files
                if with_file:
                    varMap = dict()
                    varMap[":PandaID"] = pandaID
                    self.execute(sqlF, varMap)
                    resFileList = self.cur.fetchall()
                    for resFile in resFileList:
                        fileSpec = FileSpec()
                        fileSpec.pack(resFile)
                        jobSpec.add_file(fileSpec)
                # append
                jobChunkList.append(jobSpec)
            # commit
            self.commit()
            tmpLog.debug(f"got {len(jobChunkList)} job chunks")
            return jobChunkList
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    # get jobs to trigger or check output transfer or zip output
    def get_jobs_for_stage_out(
        self,
        max_jobs,
        interval_without_lock,
        interval_with_lock,
        locked_by,
        sub_status,
        has_out_file_flag,
        bad_has_out_file_flag_list=None,
        max_files_per_job=None,
    ):
        try:
            # get logger
            msgPfx = f"thr={locked_by}"
            tmpLog = core_utils.make_logger(_logger, msgPfx, method_name="get_jobs_for_stage_out")
            tmpLog.debug("start")
            # sql to get PandaIDs without FOR UPDATE which causes deadlock in MariaDB
            sql = f"SELECT PandaID FROM {jobTableName} "
            sql += "WHERE "
            sql += "(subStatus=:subStatus OR hasOutFile=:hasOutFile) "
            if bad_has_out_file_flag_list is not None:
                sql += "AND (hasOutFile IS NULL OR hasOutFile NOT IN ("
                for badFlag in bad_has_out_file_flag_list:
                    tmpKey = f":badHasOutFile{badFlag}"
                    sql += f"{tmpKey},"
                sql = sql[:-1]
                sql += ")) "
            sql += "AND (stagerTime IS NULL "
            sql += "OR (stagerTime<:lockTimeLimit AND stagerLock IS NOT NULL) "
            sql += "OR (stagerTime<:updateTimeLimit AND stagerLock IS NULL) "
            sql += ") "
            sql += "ORDER BY stagerTime "
            sql += f"LIMIT {max_jobs} "
            # sql to lock job
            sqlL = f"UPDATE {jobTableName} SET stagerTime=:timeNow,stagerLock=:lockedBy "
            sqlL += "WHERE PandaID=:PandaID AND "
            sqlL += "(subStatus=:subStatus OR hasOutFile=:hasOutFile) "
            if bad_has_out_file_flag_list is not None:
                sqlL += "AND (hasOutFile IS NULL OR hasOutFile NOT IN ("
                for badFlag in bad_has_out_file_flag_list:
                    tmpKey = f":badHasOutFile{badFlag}"
                    sqlL += f"{tmpKey},"
                sqlL = sqlL[:-1]
                sqlL += ")) "
            sqlL += "AND (stagerTime IS NULL "
            sqlL += "OR (stagerTime<:lockTimeLimit AND stagerLock IS NOT NULL) "
            sqlL += "OR (stagerTime<:updateTimeLimit AND stagerLock IS NULL) "
            sqlL += ") "
            # sql to get job
            sqlJ = f"SELECT {JobSpec.column_names(slim=True)} FROM {jobTableName} "
            sqlJ += "WHERE PandaID=:PandaID "
            # sql to get job parameters
            sqlJJ = f"SELECT jobParams FROM {jobTableName} "
            sqlJJ += "WHERE PandaID=:PandaID "
            # sql to get files
            sqlF = f"SELECT {FileSpec.column_names()} FROM {fileTableName} "
            sqlF += "WHERE PandaID=:PandaID AND status=:status AND fileType NOT IN (:type1,:type2,:type3) "
            if max_files_per_job is not None and max_files_per_job > 0:
                sqlF += f"LIMIT {max_files_per_job} "
            # sql to get associated files
            sqlAF = f"SELECT {FileSpec.column_names()} FROM {fileTableName} "
            sqlAF += "WHERE PandaID=:PandaID AND zipFileID=:zipFileID "
            sqlAF += "AND fileType NOT IN (:type1,:type2,:type3) "
            # sql to increment attempt number
            sqlFU = f"UPDATE {fileTableName} SET attemptNr=attemptNr+1 WHERE fileID=:fileID "
            # get jobs
            timeNow = core_utils.naive_utcnow()
            lockTimeLimit = timeNow - datetime.timedelta(seconds=interval_with_lock)
            updateTimeLimit = timeNow - datetime.timedelta(seconds=interval_without_lock)
            varMap = dict()
            varMap[":subStatus"] = sub_status
            varMap[":hasOutFile"] = has_out_file_flag
            if bad_has_out_file_flag_list is not None:
                for badFlag in bad_has_out_file_flag_list:
                    tmpKey = f":badHasOutFile{badFlag}"
                    varMap[tmpKey] = badFlag
            varMap[":lockTimeLimit"] = lockTimeLimit
            varMap[":updateTimeLimit"] = updateTimeLimit
            self.execute(sql, varMap)
            resList = self.cur.fetchall()
            jobSpecList = []
            for (pandaID,) in resList:
                # lock job
                varMap = dict()
                varMap[":PandaID"] = pandaID
                varMap[":timeNow"] = timeNow
                varMap[":lockedBy"] = locked_by
                varMap[":lockTimeLimit"] = lockTimeLimit
                varMap[":updateTimeLimit"] = updateTimeLimit
                varMap[":subStatus"] = sub_status
                varMap[":hasOutFile"] = has_out_file_flag
                if bad_has_out_file_flag_list is not None:
                    for badFlag in bad_has_out_file_flag_list:
                        tmpKey = f":badHasOutFile{badFlag}"
                        varMap[tmpKey] = badFlag
                self.execute(sqlL, varMap)
                nRow = self.cur.rowcount
                # commit
                self.commit()
                if nRow > 0:
                    # get job
                    varMap = dict()
                    varMap[":PandaID"] = pandaID
                    self.execute(sqlJ, varMap)
                    resJ = self.cur.fetchone()
                    # make job
                    jobSpec = JobSpec()
                    jobSpec.pack(resJ, slim=True)
                    jobSpec.stagerLock = locked_by
                    jobSpec.stagerTime = timeNow
                    # for old jobs without extractions
                    if jobSpec.jobParamsExtForLog is None:
                        varMap = dict()
                        varMap[":PandaID"] = pandaID
                        self.execute(sqlJJ, varMap)
                        resJJ = self.cur.fetchone()
                        jobSpec.set_blob_attribute("jobParams", resJJ[0])
                        jobSpec.get_output_file_attributes()
                        jobSpec.get_logfile_info()
                    # get files
                    varMap = dict()
                    varMap[":PandaID"] = jobSpec.PandaID
                    varMap[":type1"] = "input"
                    varMap[":type2"] = FileSpec.AUX_INPUT
                    varMap[":type3"] = "checkpoint"
                    if has_out_file_flag == JobSpec.HO_hasOutput:
                        varMap[":status"] = "defined"
                    elif has_out_file_flag == JobSpec.HO_hasZipOutput:
                        varMap[":status"] = "zipping"
                    elif has_out_file_flag == JobSpec.HO_hasPostZipOutput:
                        varMap[":status"] = "post_zipping"
                    else:
                        varMap[":status"] = "transferring"
                    self.execute(sqlF, varMap)
                    resFileList = self.cur.fetchall()
                    for resFile in resFileList:
                        fileSpec = FileSpec()
                        fileSpec.pack(resFile)
                        fileSpec.attemptNr += 1
                        jobSpec.add_out_file(fileSpec)
                        # increment attempt number
                        varMap = dict()
                        varMap[":fileID"] = fileSpec.fileID
                        self.execute(sqlFU, varMap)
                    jobSpecList.append(jobSpec)
                    # commit
                    if len(resFileList) > 0:
                        self.commit()
                    # get associated files
                    if has_out_file_flag in [JobSpec.HO_hasZipOutput, JobSpec.HO_hasPostZipOutput]:
                        for fileSpec in jobSpec.outFiles:
                            varMap = dict()
                            varMap[":PandaID"] = fileSpec.PandaID
                            varMap[":zipFileID"] = fileSpec.fileID
                            varMap[":type1"] = "input"
                            varMap[":type2"] = FileSpec.AUX_INPUT
                            varMap[":type3"] = "checkpoint"
                            self.execute(sqlAF, varMap)
                            resAFs = self.cur.fetchall()
                            for resAF in resAFs:
                                assFileSpec = FileSpec()
                                assFileSpec.pack(resAF)
                                fileSpec.add_associated_file(assFileSpec)
                    # get associated workers
                    tmpWorkers = self.get_workers_with_job_id(jobSpec.PandaID, use_commit=False)
                    jobSpec.add_workspec_list(tmpWorkers)
            tmpLog.debug(f"got {len(jobSpecList)} jobs")
            return jobSpecList
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    # update job for stage-out
    def update_job_for_stage_out(self, jobspec, update_event_status, locked_by):
        try:
            # get logger
            tmpLog = core_utils.make_logger(
                _logger, f"PandaID={jobspec.PandaID} subStatus={jobspec.subStatus} thr={locked_by}", method_name="update_job_for_stage_out"
            )
            tmpLog.debug("start")
            # sql to update event
            sqlEU = f"UPDATE {eventTableName} "
            sqlEU += "SET eventStatus=:eventStatus,subStatus=:subStatus "
            sqlEU += "WHERE eventRangeID=:eventRangeID "
            sqlEU += "AND eventStatus<>:statusFailed AND subStatus<>:statusDone "
            # sql to update associated events
            sqlAE1 = f"SELECT eventRangeID FROM {fileTableName} "
            sqlAE1 += "WHERE PandaID=:PandaID AND zipFileID=:zipFileID "
            sqlAE = f"UPDATE {eventTableName} "
            sqlAE += "SET eventStatus=:eventStatus,subStatus=:subStatus "
            sqlAE += "WHERE eventRangeID=:eventRangeID "
            sqlAE += "AND eventStatus<>:statusFailed AND subStatus<>:statusDone "
            # sql to lock job again
            sqlLJ = f"UPDATE {jobTableName} SET stagerTime=:timeNow "
            sqlLJ += "WHERE PandaID=:PandaID AND stagerLock=:lockedBy "
            # sql to check lock
            sqlLC = f"SELECT stagerLock FROM {jobTableName} "
            sqlLC += "WHERE PandaID=:PandaID "
            # lock
            varMap = dict()
            varMap[":PandaID"] = jobspec.PandaID
            varMap[":lockedBy"] = locked_by
            varMap[":timeNow"] = core_utils.naive_utcnow()
            self.execute(sqlLJ, varMap)
            nRow = self.cur.rowcount
            # check just in case since nRow can be 0 if two lock actions are too close in time
            if nRow == 0:
                varMap = dict()
                varMap[":PandaID"] = jobspec.PandaID
                self.execute(sqlLC, varMap)
                resLC = self.cur.fetchone()
                if resLC is not None and resLC[0] == locked_by:
                    nRow = 1
            # commit
            self.commit()
            if nRow == 0:
                tmpLog.debug("skip since locked by another")
                return None
            # update files
            tmpLog.debug(f"update {len(jobspec.outFiles)} files")
            for fileSpec in jobspec.outFiles:
                # sql to update file
                sqlF = f"UPDATE {fileTableName} SET {fileSpec.bind_update_changes_expression()} "
                sqlF += "WHERE PandaID=:PandaID AND fileID=:fileID "
                varMap = fileSpec.values_map(only_changed=True)
                updated = False
                if len(varMap) > 0:
                    varMap[":PandaID"] = fileSpec.PandaID
                    varMap[":fileID"] = fileSpec.fileID
                    self.execute(sqlF, varMap)
                    updated = True
                # update event status
                if update_event_status:
                    if fileSpec.eventRangeID is not None:
                        varMap = dict()
                        varMap[":eventRangeID"] = fileSpec.eventRangeID
                        varMap[":eventStatus"] = fileSpec.status
                        varMap[":subStatus"] = fileSpec.status
                        varMap[":statusFailed"] = "failed"
                        varMap[":statusDone"] = "done"
                        self.execute(sqlEU, varMap)
                        updated = True
                    if fileSpec.isZip == 1:
                        # update files associated with zip file
                        varMap = dict()
                        varMap[":PandaID"] = fileSpec.PandaID
                        varMap[":zipFileID"] = fileSpec.fileID
                        self.execute(sqlAE1, varMap)
                        resAE1 = self.cur.fetchall()
                        for (eventRangeID,) in resAE1:
                            varMap = dict()
                            varMap[":eventRangeID"] = eventRangeID
                            varMap[":eventStatus"] = fileSpec.status
                            varMap[":subStatus"] = fileSpec.status
                            varMap[":statusFailed"] = "failed"
                            varMap[":statusDone"] = "done"
                            self.execute(sqlAE, varMap)
                        updated = True
                        nRow = self.cur.rowcount
                        tmpLog.debug(f"updated {nRow} events")
                if updated:
                    # lock job again
                    varMap = dict()
                    varMap[":PandaID"] = jobspec.PandaID
                    varMap[":lockedBy"] = locked_by
                    varMap[":timeNow"] = core_utils.naive_utcnow()
                    self.execute(sqlLJ, varMap)
                    # commit
                    self.commit()
                    nRow = self.cur.rowcount
                    # check just in case since nRow can be 0 if two lock actions are too close in time
                    if nRow == 0:
                        varMap = dict()
                        varMap[":PandaID"] = jobspec.PandaID
                        self.execute(sqlLC, varMap)
                        resLC = self.cur.fetchone()
                        if resLC is not None and resLC[0] == locked_by:
                            nRow = 1
                    if nRow == 0:
                        tmpLog.debug("skip since locked by another")
                        return None
            # count files
            sqlC = f"SELECT COUNT(*) cnt,status FROM {fileTableName} "
            sqlC += "WHERE PandaID=:PandaID GROUP BY status "
            varMap = dict()
            varMap[":PandaID"] = jobspec.PandaID
            self.execute(sqlC, varMap)
            resC = self.cur.fetchall()
            cntMap = {}
            for cnt, fileStatus in resC:
                cntMap[fileStatus] = cnt
            # set job attributes
            jobspec.stagerLock = None
            if "zipping" in cntMap:
                jobspec.hasOutFile = JobSpec.HO_hasZipOutput
            elif "post_zipping" in cntMap:
                jobspec.hasOutFile = JobSpec.HO_hasPostZipOutput
            elif "defined" in cntMap:
                jobspec.hasOutFile = JobSpec.HO_hasOutput
            elif "transferring" in cntMap:
                jobspec.hasOutFile = JobSpec.HO_hasTransfer
            else:
                jobspec.hasOutFile = JobSpec.HO_noOutput
            if jobspec.subStatus == "to_transfer":
                # change subStatus when no more files to trigger transfer
                if jobspec.hasOutFile not in [JobSpec.HO_hasOutput, JobSpec.HO_hasZipOutput, JobSpec.HO_hasPostZipOutput]:
                    jobspec.subStatus = "transferring"
                jobspec.stagerTime = None
            elif jobspec.subStatus == "transferring":
                # all done
                if jobspec.hasOutFile == JobSpec.HO_noOutput:
                    jobspec.trigger_propagation()
                    if "failed" in cntMap:
                        jobspec.status = "failed"
                        jobspec.subStatus = "failed_to_stage_out"
                    else:
                        jobspec.subStatus = "staged"
                        # get finished files
                        jobspec.reset_out_file()
                        sqlFF = f"SELECT {FileSpec.column_names()} FROM {fileTableName} "
                        sqlFF += "WHERE PandaID=:PandaID AND status=:status AND fileType IN (:type1,:type2) "
                        varMap = dict()
                        varMap[":PandaID"] = jobspec.PandaID
                        varMap[":status"] = "finished"
                        varMap[":type1"] = "output"
                        varMap[":type2"] = "log"
                        self.execute(sqlFF, varMap)
                        resFileList = self.cur.fetchall()
                        for resFile in resFileList:
                            fileSpec = FileSpec()
                            fileSpec.pack(resFile)
                            jobspec.add_out_file(fileSpec)
                        # make file report
                        jobspec.outputFilesToReport = core_utils.get_output_file_report(jobspec)
            # sql to update job
            sqlJ = f"UPDATE {jobTableName} SET {jobspec.bind_update_changes_expression()} "
            sqlJ += "WHERE PandaID=:PandaID AND stagerLock=:lockedBy "
            # update job
            varMap = jobspec.values_map(only_changed=True)
            varMap[":PandaID"] = jobspec.PandaID
            varMap[":lockedBy"] = locked_by
            self.execute(sqlJ, varMap)
            # commit
            self.commit()
            tmpLog.debug("done")
            # return
            return jobspec.subStatus
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None
