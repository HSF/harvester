"""
DB proxy module for files and file groups

"""

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_mods.base_module import (
    BaseModule,
    _logger,
    db_error_handler,
    fileTableName,
    jobTableName,
)
from pandaharvester.harvestercore.file_spec import FileSpec


# DB proxy module for files and file groups
class FileModule(BaseModule):
    # get file status
    @db_error_handler(default={})
    def get_file_status(self, lfn, file_type, endpoint, job_status):
        # get logger
        tmpLog = core_utils.make_logger(_logger, f"lfn={lfn} endpoint={endpoint}", method_name="get_file_status")
        tmpLog.debug("start")
        # sql to get files
        sqlF = f"SELECT f.status, f.path, COUNT(*) cnt FROM {fileTableName} f, {jobTableName} j "
        sqlF += "WHERE j.PandaID=f.PandaID AND j.status=:jobStatus "
        sqlF += "AND f.lfn=:lfn AND f.fileType=:type "
        if endpoint is not None:
            sqlF += "AND f.endpoint=:endpoint "
        sqlF += "GROUP BY f.status, f.path "
        # get files
        varMap = dict()
        varMap[":lfn"] = lfn
        varMap[":type"] = file_type
        varMap[":jobStatus"] = job_status
        if endpoint is not None:
            varMap[":endpoint"] = endpoint
        self.execute(sqlF, varMap)
        retMap = dict()
        for status, path, cnt in self.cur.fetchall():
            retMap.setdefault(status, {"cnt": 0, "path": set()})
            retMap[status]["cnt"] += cnt
            retMap[status]["path"].add(path)
        # commit
        self.commit()
        tmpLog.debug(f"got {str(retMap)}")
        return retMap

    # change file status
    @db_error_handler(default=False)
    def change_file_status(self, panda_id, data, locked_by):
        # get logger
        tmpLog = core_utils.make_logger(_logger, f"PandaID={panda_id}", method_name="change_file_status")
        tmpLog.debug(f"start lockedBy={locked_by}")
        # sql to check lock of job
        sqlJ = f"SELECT lockedBy FROM {jobTableName} "
        sqlJ += "WHERE PandaID=:PandaID FOR UPDATE "
        # sql to update files
        sqlF = f"UPDATE {fileTableName} "
        sqlF += "SET status=:status WHERE fileID=:fileID "
        # check lock
        varMap = dict()
        varMap[":PandaID"] = panda_id
        self.execute(sqlJ, varMap)
        resJ = self.cur.fetchone()
        if resJ is None:
            tmpLog.debug("skip since job not found")
        else:
            (lockedBy,) = resJ
            if lockedBy != locked_by:
                tmpLog.debug(f"skip since lockedBy is inconsistent in DB {lockedBy}")
            else:
                # update files
                for tmpFileID, tmpLFN, newStatus in data:
                    varMap = dict()
                    varMap[":fileID"] = tmpFileID
                    varMap[":status"] = newStatus
                    self.execute(sqlF, varMap)
                    tmpLog.debug(f"set new status {newStatus} to {tmpLFN}")
        # commit
        self.commit()
        tmpLog.debug("done")
        return True

    # get group for a file
    @db_error_handler(default=None)
    def get_group_for_file(self, lfn, file_type, endpoint):
        # get logger
        tmpLog = core_utils.make_logger(_logger, f"lfn={lfn} endpoint={endpoint}", method_name="get_group_for_file")
        tmpLog.debug("start")
        # sql to get group with the latest update
        sqlF = "SELECT * FROM ("
        sqlF += f"SELECT groupID,groupStatus,groupUpdateTime FROM {fileTableName} "
        sqlF += "WHERE lfn=:lfn AND fileType=:type "
        sqlF += "AND groupID IS NOT NULL AND groupStatus<>:ngStatus "
        if endpoint is not None:
            sqlF += "AND endpoint=:endpoint "
        sqlF += "ORDER BY groupUpdateTime DESC "
        sqlF += ") AS TMP LIMIT 1 "
        # get group
        varMap = dict()
        varMap[":lfn"] = lfn
        varMap[":type"] = file_type
        varMap[":ngStatus"] = "failed"
        if endpoint is not None:
            varMap[":endpoint"] = endpoint
        self.execute(sqlF, varMap)
        resF = self.cur.fetchone()
        if resF is None:
            retVal = None
        else:
            groupID, groupStatus, groupUpdateTime = resF
            retVal = {"groupID": groupID, "groupStatus": groupStatus, "groupUpdateTime": groupUpdateTime}
        # commit
        self.commit()
        tmpLog.debug(f"got {str(retVal)}")
        return retVal

    # get files with a group ID
    @db_error_handler(default=[])
    def get_files_with_group_id(self, group_id):
        # get logger
        tmpLog = core_utils.make_logger(_logger, f"groupID={group_id}", method_name="get_files_with_group_id")
        tmpLog.debug("start")
        # sql to get files
        sqlF = f"SELECT {FileSpec.column_names()} FROM {fileTableName} "
        sqlF += "WHERE groupID=:groupID "
        # get files
        varMap = dict()
        varMap[":groupID"] = group_id
        retList = []
        self.execute(sqlF, varMap)
        for resFile in self.cur.fetchall():
            fileSpec = FileSpec()
            fileSpec.pack(resFile)
            retList.append(fileSpec)
        # commit
        self.commit()
        tmpLog.debug(f"got {len(retList)} files")
        return retList

    # update group status
    @db_error_handler(default=False)
    def update_file_group_status(self, group_id, status_string):
        # get logger
        tmpLog = core_utils.make_logger(_logger, f"groupID={group_id}", method_name="update_file_group_status")
        tmpLog.debug("start")
        # sql to get files
        sqlF = f"UPDATE {fileTableName} set groupStatus=:groupStatus "
        sqlF += "WHERE groupID=:groupID "
        # get files
        varMap = dict()
        varMap[":groupID"] = group_id
        varMap[":groupStatus"] = status_string
        self.execute(sqlF, varMap)
        nRow = self.cur.rowcount
        # commit
        self.commit()
        tmpLog.debug(f"updated {nRow} files")
        return True

    # get file group status
    @db_error_handler(default=[])
    def get_file_group_status(self, group_id):
        # get logger
        tmpLog = core_utils.make_logger(_logger, f"groupID={group_id}", method_name="get_file_group_status")
        tmpLog.debug("start")
        # sql to get files
        sqlF = f"SELECT DISTINCT groupStatus FROM {fileTableName} "
        sqlF += "WHERE groupID=:groupID "
        # get files
        varMap = dict()
        varMap[":groupID"] = group_id
        self.execute(sqlF, varMap)
        res = self.cur.fetchall()
        retVal = set()
        for (groupStatus,) in res:
            retVal.add(groupStatus)
        # commit
        self.commit()
        tmpLog.debug(f"get {str(retVal)}")
        return retVal

    # set file group
    @db_error_handler(default=False)
    def set_file_group(self, file_specs, group_id, status_string):
        # get logger
        tmpLog = core_utils.make_logger(_logger, f"groupID={group_id}", method_name="set_file_group")
        tmpLog.debug("start")
        timeNow = core_utils.naive_utcnow()
        # sql to update files
        sqlF = f"UPDATE {fileTableName} "
        sqlF += "SET groupID=:groupID,groupStatus=:groupStatus,groupUpdateTime=:groupUpdateTime "
        sqlF += "WHERE lfn=:lfn "
        # update files
        for fileSpec in file_specs:
            varMap = dict()
            varMap[":groupID"] = group_id
            varMap[":groupStatus"] = status_string
            varMap[":groupUpdateTime"] = timeNow
            varMap[":lfn"] = fileSpec.lfn
            self.execute(sqlF, varMap)
        # commit
        self.commit()
        tmpLog.debug("done")
        return True

    # refresh file group info
    @db_error_handler(default=False)
    def refresh_file_group_info(self, job_spec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, f"pandaID={job_spec.PandaID}", method_name="refresh_file_group_info")
        tmpLog.debug("start")
        # sql to get info
        sqlF = f"SELECT groupID,groupStatus,groupUpdateTime FROM {fileTableName} "
        sqlF += "WHERE lfn=:lfn "
        # get info
        for fileSpec in job_spec.inFiles.union(job_spec.outFiles):
            varMap = dict()
            varMap[":lfn"] = fileSpec.lfn
            self.execute(sqlF, varMap)
            resF = self.cur.fetchone()
            if resF is None:
                continue
            groupID, groupStatus, groupUpdateTime = resF
            fileSpec.groupID = groupID
            fileSpec.groupStatus = groupStatus
            fileSpec.groupUpdateTime = groupUpdateTime
        # commit
        self.commit()
        tmpLog.debug("done")
        return True

    # get all active input files
    @db_error_handler(default=set())
    def get_all_active_input_files(self):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="get_all_active_input_files")
        tmpLog.debug("start")
        # sql to get files
        sqlF = f"SELECT lfn FROM {fileTableName} "
        sqlF += "WHERE fileType IN (:type1,:type2) "
        # get files
        varMap = dict()
        varMap[":type1"] = "input"
        varMap[":type2"] = FileSpec.AUX_INPUT
        self.execute(sqlF, varMap)
        ret = set()
        for (lfn,) in self.cur.fetchall():
            ret.add(lfn)
        # commit
        self.commit()
        tmpLog.debug(f"got {len(ret)} files")
        return ret
