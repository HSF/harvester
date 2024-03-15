"""
database connection

"""

import copy
import datetime
import inspect
import os
import random
import re
import sys
import threading
import time

from pandaharvester.harvesterconfig import harvester_config

from . import core_utils
from .cache_spec import CacheSpec
from .command_spec import CommandSpec
from .diag_spec import DiagSpec
from .event_spec import EventSpec
from .file_spec import FileSpec
from .job_spec import JobSpec
from .job_worker_relation_spec import JobWorkerRelationSpec
from .panda_queue_spec import PandaQueueSpec
from .process_lock_spec import ProcessLockSpec
from .queue_config_dump_spec import QueueConfigDumpSpec
from .seq_number_spec import SeqNumberSpec
from .service_metrics_spec import ServiceMetricSpec
from .work_spec import WorkSpec

# logger
_logger = core_utils.setup_logger("db_proxy")

# table names
commandTableName = "command_table"
jobTableName = "job_table"
workTableName = "work_table"
fileTableName = "file_table"
cacheTableName = "cache_table"
eventTableName = "event_table"
seqNumberTableName = "seq_table"
pandaQueueTableName = "pq_table"
jobWorkerTableName = "jw_table"
processLockTableName = "lock_table"
diagTableName = "diag_table"
queueConfigDumpTableName = "qcdump_table"
serviceMetricsTableName = "sm_table"

# connection lock
conLock = threading.Lock()


# connection class
class DBProxy(object):
    # constructor
    def __init__(self, thr_name=None, read_only=False):
        self.thrName = thr_name
        self.verbLog = None
        self.useInspect = False
        self.reconnectTimeout = 300
        self.read_only = read_only
        if hasattr(harvester_config.db, "reconnectTimeout"):
            self.reconnectTimeout = harvester_config.db.reconnectTimeout
        if harvester_config.db.verbose:
            self.verbLog = core_utils.make_logger(_logger, method_name="execute")
            if self.thrName is None:
                currentThr = threading.current_thread()
                if currentThr is not None:
                    self.thrName = currentThr.ident
            if hasattr(harvester_config.db, "useInspect") and harvester_config.db.useInspect is True:
                self.useInspect = True
        # connect DB
        self._connect_db()
        self.lockDB = False
        # using application side lock if DB doesn't have a mechanism for exclusive access
        if harvester_config.db.engine == "mariadb":
            self.usingAppLock = False
        else:
            self.usingAppLock = True

    # connect DB
    def _connect_db(self):
        if harvester_config.db.engine == "mariadb":
            if hasattr(harvester_config.db, "host"):
                host = harvester_config.db.host
            else:
                host = "127.0.0.1"
            if hasattr(harvester_config.db, "port"):
                port = harvester_config.db.port
            else:
                port = 3306
            if hasattr(harvester_config.db, "useMySQLdb") and harvester_config.db.useMySQLdb is True:
                import MySQLdb
                import MySQLdb.cursors

                class MyCursor(MySQLdb.cursors.Cursor):
                    def fetchone(self):
                        tmpRet = MySQLdb.cursors.Cursor.fetchone(self)
                        if tmpRet is None:
                            return None
                        tmpRet = core_utils.DictTupleHybrid(tmpRet)
                        tmpRet.set_attributes([d[0] for d in self.description])
                        return tmpRet

                    def fetchall(self):
                        tmpRets = MySQLdb.cursors.Cursor.fetchall(self)
                        if len(tmpRets) == 0:
                            return tmpRets
                        newTmpRets = []
                        attributes = [d[0] for d in self.description]
                        for tmpRet in tmpRets:
                            tmpRet = core_utils.DictTupleHybrid(tmpRet)
                            tmpRet.set_attributes(attributes)
                            newTmpRets.append(tmpRet)
                        return newTmpRets

                self.con = MySQLdb.connect(
                    user=harvester_config.db.user,
                    passwd=harvester_config.db.password,
                    db=harvester_config.db.schema,
                    host=host,
                    port=port,
                    cursorclass=MyCursor,
                    charset="utf8",
                )
                self.cur = self.con.cursor()
            else:
                import mysql.connector

                self.con = mysql.connector.connect(
                    user=harvester_config.db.user, passwd=harvester_config.db.password, db=harvester_config.db.schema, host=host, port=port
                )
                self.cur = self.con.cursor(named_tuple=True, buffered=True)
        else:
            import sqlite3

            if self.read_only:
                fd = os.open(harvester_config.db.database_filename, os.O_RDONLY)
                database_filename = f"/dev/fd/{fd}"
            else:
                database_filename = harvester_config.db.database_filename
            self.con = sqlite3.connect(database_filename, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES, check_same_thread=False)
            core_utils.set_file_permission(harvester_config.db.database_filename)
            # change the row factory to use Row
            self.con.row_factory = sqlite3.Row
            self.cur = self.con.cursor()
            self.cur.execute("PRAGMA journal_mode")
            resJ = self.cur.fetchone()
            if resJ[0] != "wal":
                self.cur.execute("PRAGMA journal_mode = WAL")
                # read to avoid database lock
                self.cur.fetchone()

    # exception handler for type of DBs
    def _handle_exception(self, exc):
        tmpLog = core_utils.make_logger(_logger, f"thr={self.thrName}", method_name="_handle_exception")
        if harvester_config.db.engine == "mariadb":
            tmpLog.warning(f"exception of mysql {exc.__class__.__name__} occurred")
            # Case to try renew connection
            isOperationalError = False
            if hasattr(harvester_config.db, "useMySQLdb") and harvester_config.db.useMySQLdb is True:
                import MySQLdb

                if isinstance(exc, MySQLdb.OperationalError):
                    isOperationalError = True
            else:
                import mysql.connector

                if isinstance(exc, mysql.connector.errors.OperationalError):
                    isOperationalError = True
            if isOperationalError:
                try_timestamp = time.time()
                n_retry = 1
                while time.time() - try_timestamp < self.reconnectTimeout:
                    # close DB cursor
                    try:
                        self.cur.close()
                    except Exception as e:
                        tmpLog.error(f"failed to close cursor: {e}")
                    # close DB connection
                    try:
                        self.con.close()
                    except Exception as e:
                        tmpLog.error(f"failed to close connection: {e}")
                    # restart the proxy instance
                    try:
                        self._connect_db()
                        tmpLog.info("renewed connection")
                        break
                    except Exception as e:
                        tmpLog.error(f"failed to renew connection ({n_retry} retries); {e}")
                        sleep_time = core_utils.retry_period_sec(n_retry, increment=2, max_seconds=300, min_seconds=1)
                        if not sleep_time:
                            break
                        else:
                            time.sleep(sleep_time)
                            n_retry += 1

    # convert param dict to list
    def convert_params(self, sql, varmap):
        # lock database if application side lock is used
        if self.usingAppLock and (
            re.search("^INSERT", sql, re.I) is not None
            or re.search("^UPDATE", sql, re.I) is not None
            or re.search(" FOR UPDATE", sql, re.I) is not None
            or re.search("^DELETE", sql, re.I) is not None
        ):
            self.lockDB = True
        # remove FOR UPDATE for sqlite
        if harvester_config.db.engine == "sqlite":
            sql = re.sub(" FOR UPDATE", " ", sql, re.I)
            sql = re.sub("INSERT IGNORE", "INSERT OR IGNORE", sql, re.I)
        else:
            sql = re.sub("INSERT OR IGNORE", "INSERT IGNORE", sql, re.I)
        # no conversation unless dict
        if not isinstance(varmap, dict):
            # using the printf style syntax for mariaDB
            if harvester_config.db.engine == "mariadb":
                sql = re.sub(":[^ $,)]+", "%s", sql)
            return sql, varmap
        paramList = []
        # extract placeholders
        items = re.findall(":[^ $,)]+", sql)
        for item in items:
            if item not in varmap:
                raise KeyError(f"{item} is missing in SQL parameters")
            if item not in paramList:
                paramList.append(varmap[item])
        # using the printf style syntax for mariaDB
        if harvester_config.db.engine == "mariadb":
            sql = re.sub(":[^ $,)]+", "%s", sql)
        return sql, paramList

    # wrapper for execute
    def execute(self, sql, varmap=None):
        sw = core_utils.get_stopwatch()
        if varmap is None:
            varmap = dict()
        # get lock if application side lock is used
        if self.usingAppLock and not self.lockDB:
            if harvester_config.db.verbose:
                self.verbLog.debug(f"thr={self.thrName} locking")
            conLock.acquire()
            if harvester_config.db.verbose:
                self.verbLog.debug(f"thr={self.thrName} locked")
        # execute
        try:
            # verbose
            if harvester_config.db.verbose:
                if not self.useInspect:
                    self.verbLog.debug(f"thr={self.thrName} sql={sql} var={str(varmap)}")
                else:
                    self.verbLog.debug(f"thr={self.thrName} sql={sql} var={str(varmap)} exec={inspect.stack()[1][3]}")
            # convert param dict
            newSQL, params = self.convert_params(sql, varmap)
            # execute
            try:
                retVal = self.cur.execute(newSQL, params)
            except Exception as e:
                self._handle_exception(e)
                if harvester_config.db.verbose:
                    self.verbLog.debug(f"thr={self.thrName} exception during execute")
                raise
        finally:
            # release lock
            if self.usingAppLock and not self.lockDB:
                if harvester_config.db.verbose:
                    self.verbLog.debug(f"thr={self.thrName} release")
                conLock.release()
        # return
        if harvester_config.db.verbose:
            self.verbLog.debug("thr={0}  {1}  sql=[{2}]".format(self.thrName, sw.get_elapsed_time(), newSQL.replace("\n", " ").strip()))
        return retVal

    # wrapper for executemany
    def executemany(self, sql, varmap_list):
        # get lock
        if self.usingAppLock and not self.lockDB:
            if harvester_config.db.verbose:
                self.verbLog.debug(f"thr={self.thrName} locking")
            conLock.acquire()
            if harvester_config.db.verbose:
                self.verbLog.debug(f"thr={self.thrName} locked")
        try:
            # verbose
            if harvester_config.db.verbose:
                if not self.useInspect:
                    self.verbLog.debug(f"thr={self.thrName} sql={sql} var={str(varmap_list)}")
                else:
                    self.verbLog.debug(f"thr={self.thrName} sql={sql} var={str(varmap_list)} exec={inspect.stack()[1][3]}")
            # convert param dict
            paramList = []
            newSQL = sql
            for varMap in varmap_list:
                if varMap is None:
                    varMap = dict()
                newSQL, params = self.convert_params(sql, varMap)
                paramList.append(params)
            # execute
            try:
                if harvester_config.db.engine == "sqlite":
                    retVal = []
                    iList = 0
                    nList = 5000
                    while iList < len(paramList):
                        retVal += self.cur.executemany(newSQL, paramList[iList : iList + nList])
                        iList += nList
                else:
                    retVal = self.cur.executemany(newSQL, paramList)
            except Exception as e:
                self._handle_exception(e)
                if harvester_config.db.verbose:
                    self.verbLog.debug(f"thr={self.thrName} exception during executemany")
                raise
        finally:
            # release lock
            if self.usingAppLock and not self.lockDB:
                if harvester_config.db.verbose:
                    self.verbLog.debug(f"thr={self.thrName} release")
                conLock.release()
        # return
        return retVal

    # commit
    def commit(self):
        try:
            self.con.commit()
        except Exception as e:
            self._handle_exception(e)
            if harvester_config.db.verbose:
                self.verbLog.debug(f"thr={self.thrName} exception during commit")
            raise
        if self.usingAppLock and self.lockDB:
            if harvester_config.db.verbose:
                self.verbLog.debug(f"thr={self.thrName} release with commit")
            conLock.release()
            self.lockDB = False

    # rollback
    def rollback(self):
        try:
            self.con.rollback()
        except Exception as e:
            self._handle_exception(e)
            if harvester_config.db.verbose:
                self.verbLog.debug(f"thr={self.thrName} exception during rollback")
        finally:
            if self.usingAppLock and self.lockDB:
                if harvester_config.db.verbose:
                    self.verbLog.debug(f"thr={self.thrName} release with rollback")
                conLock.release()
                self.lockDB = False

    # type conversion
    def type_conversion(self, attr_type):
        # remove decorator
        attr_type = attr_type.split("/")[0]
        attr_type = attr_type.strip()
        if attr_type == "timestamp":
            # add NULL attribute to disable automatic update
            attr_type += " null"
        # type conversion
        if harvester_config.db.engine == "mariadb":
            if attr_type.startswith("text"):
                attr_type = attr_type.replace("text", "varchar(256)")
            elif attr_type.startswith("blob"):
                attr_type = attr_type.replace("blob", "longtext")
            elif attr_type.startswith("integer"):
                attr_type = attr_type.replace("integer", "bigint")
            attr_type = attr_type.replace("autoincrement", "auto_increment")
        elif harvester_config.db.engine == "sqlite":
            if attr_type.startswith("varchar"):
                attr_type = re.sub("varchar\(\d+\)", "text", attr_type)
            attr_type = attr_type.replace("auto_increment", "autoincrement")
        return attr_type

    # check if index is needed
    def need_index(self, attr):
        isIndex = False
        isUnique = False
        # look for separator
        if "/" in attr:
            decorators = attr.split("/")[-1].split()
            if "index" in decorators:
                isIndex = True
            if "unique" in decorators:
                isIndex = True
                isUnique = True
        return isIndex, isUnique

    def initialize_jobType(self, table_name):
        # initialize old NULL entries to ANY in pq_table and work_table
        # get logger
        tmp_log = core_utils.make_logger(_logger, method_name="initialize_jobType")

        sql_update = f"UPDATE {table_name} SET jobType = 'ANY' WHERE jobType is NULL "
        try:
            self.execute(sql_update)
            # commit
            self.commit()
            tmp_log.debug(f"initialized entries in {table_name}")
        except Exception:
            core_utils.dump_error_message(tmp_log)

    # make table
    def make_table(self, cls, table_name):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="make_table")
            tmpLog.debug(f"table={table_name}")
            # check if table already exists
            varMap = dict()
            varMap[":name"] = table_name
            if harvester_config.db.engine == "mariadb":
                varMap[":schema"] = harvester_config.db.schema
                sqlC = "SELECT * FROM information_schema.tables WHERE table_schema=:schema AND table_name=:name "
            else:
                varMap[":type"] = "table"
                sqlC = "SELECT name FROM sqlite_master WHERE type=:type AND tbl_name=:name "
            self.execute(sqlC, varMap)
            resC = self.cur.fetchone()
            indexes = []
            uniques = set()
            # not exists
            if resC is None:
                #  sql to make table
                sqlM = f"CREATE TABLE {table_name}("
                # collect columns
                for attr in cls.attributesWithTypes:
                    # split to name and type
                    attrName, attrType = attr.split(":")
                    attrType = self.type_conversion(attrType)
                    # check if index is needed
                    isIndex, isUnique = self.need_index(attr)
                    if isIndex:
                        indexes.append(attrName)
                        if isUnique:
                            uniques.add(attrName)
                    sqlM += f"{attrName} {attrType},"
                sqlM = sqlM[:-1]
                sqlM += ")"
                # make table
                self.execute(sqlM)
                # commit
                self.commit()
                tmpLog.debug(f"made {table_name}")
            else:
                # check table
                missingAttrs = self.check_table(cls, table_name, True)
                if len(missingAttrs) > 0:
                    for attr in cls.attributesWithTypes:
                        # split to name and type
                        attrName, attrType = attr.split(":")
                        attrType = self.type_conversion(attrType)
                        # ony missing
                        if attrName not in missingAttrs:
                            continue
                        # check if index is needed
                        isIndex, isUnique = self.need_index(attr)
                        if isIndex:
                            indexes.append(attrName)
                            if isUnique:
                                uniques.add(attrName)
                        # add column
                        sqlA = f"ALTER TABLE {table_name} ADD COLUMN "
                        sqlA += f"{attrName} {attrType}"
                        try:
                            self.execute(sqlA)
                            # commit
                            self.commit()
                            tmpLog.debug(f"added {attr} to {table_name}")
                        except Exception:
                            core_utils.dump_error_message(tmpLog)

                        # if we just added the jobType, old entries need to be initialized
                        if (table_name == pandaQueueTableName and attrName == "jobType") or (table_name == pandaQueueTableName and attrName == "jobType"):
                            self.initialize_jobType(table_name)

            # make indexes
            for index in indexes:
                indexName = f"idx_{index}_{table_name}"
                if index in uniques:
                    sqlI = "CREATE UNIQUE INDEX "
                else:
                    sqlI = "CREATE INDEX "
                sqlI += f"{indexName} ON {table_name}({index}) "
                try:
                    self.execute(sqlI)
                    # commit
                    self.commit()
                    tmpLog.debug(f"added {indexName}")
                except Exception:
                    core_utils.dump_error_message(tmpLog)
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
        return self.check_table(cls, table_name)

    # make tables
    def make_tables(self, queue_config_mapper, communicator_pool):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="make_tables")
        tmpLog.debug("start")
        outStrs = []
        outStrs += self.make_table(CommandSpec, commandTableName)
        outStrs += self.make_table(JobSpec, jobTableName)
        outStrs += self.make_table(WorkSpec, workTableName)
        outStrs += self.make_table(FileSpec, fileTableName)
        outStrs += self.make_table(EventSpec, eventTableName)
        outStrs += self.make_table(CacheSpec, cacheTableName)
        outStrs += self.make_table(SeqNumberSpec, seqNumberTableName)
        outStrs += self.make_table(PandaQueueSpec, pandaQueueTableName)
        outStrs += self.make_table(JobWorkerRelationSpec, jobWorkerTableName)
        outStrs += self.make_table(ProcessLockSpec, processLockTableName)
        outStrs += self.make_table(DiagSpec, diagTableName)
        outStrs += self.make_table(QueueConfigDumpSpec, queueConfigDumpTableName)
        outStrs += self.make_table(ServiceMetricSpec, serviceMetricsTableName)

        # dump error messages
        if len(outStrs) > 0:
            errMsg = "ERROR : Definitions of some database tables are incorrect. "
            errMsg += "Please add missing columns, or drop those tables "
            errMsg += "so that harvester automatically re-creates those tables."
            errMsg += "\n"
            print(errMsg)
            for outStr in outStrs:
                print(outStr)
            sys.exit(1)

        # sync workerID
        init_worker = 1
        if hasattr(harvester_config.db, "syncMaxWorkerID") and harvester_config.db.syncMaxWorkerID:
            retVal, out = communicator_pool.get_max_worker_id()
            if not retVal:
                tmpLog.warning(f"failed to get max workerID with {out}")
            elif not out:
                tmpLog.debug("max workerID is undefined")
            else:
                tmpLog.debug(f"got max_workerID={out}")
                init_worker = out + 1
        # add sequential numbers
        self.add_seq_number("SEQ_workerID", init_worker)
        self.add_seq_number("SEQ_configID", 1)
        # fill PandaQueue table
        queue_config_mapper.load_data()
        # delete process locks
        self.clean_process_locks()
        tmpLog.debug("done")

    # check table
    def check_table(self, cls, table_name, get_missing=False):
        # get columns in DB
        varMap = dict()
        if harvester_config.db.engine == "mariadb":
            varMap[":name"] = table_name
            varMap[":schema"] = harvester_config.db.schema
            sqlC = "SELECT column_name,column_type FROM information_schema.columns WHERE table_schema=:schema AND table_name=:name "
        else:
            sqlC = f"PRAGMA table_info({table_name}) "
        self.execute(sqlC, varMap)
        resC = self.cur.fetchall()
        colMap = dict()
        for tmpItem in resC:
            if harvester_config.db.engine == "mariadb":
                if hasattr(tmpItem, "_asdict"):
                    tmpItem = tmpItem._asdict()
                try:
                    columnName, columnType = tmpItem["column_name"], tmpItem["column_type"]
                except KeyError:
                    columnName, columnType = tmpItem["COLUMN_NAME"], tmpItem["COLUMN_TYPE"]
            else:
                columnName, columnType = tmpItem[1], tmpItem[2]
            colMap[columnName] = columnType
        self.commit()
        # check with class definition
        outStrs = []
        for attr in cls.attributesWithTypes:
            attrName, attrType = attr.split(":")
            if attrName not in colMap:
                if get_missing:
                    outStrs.append(attrName)
                else:
                    attrType = self.type_conversion(attrType)
                    outStrs.append(f"{attrName} {attrType} is missing in {table_name}")
        return outStrs

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

    # get number of jobs to fetch
    def get_num_jobs_to_fetch(self, n_queues, interval):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="get_num_jobs_to_fetch")
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

    # get queues to submit workers
    def get_queues_to_submit(self, n_queues, lookup_interval, lock_interval, locked_by, queue_lock_interval):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_queues_to_submit")
            tmpLog.debug("start")
            retMap = dict()
            siteName = None
            resourceMap = dict()
            # sql to get a site
            sqlS = f"SELECT siteName FROM {pandaQueueTableName} "
            sqlS += "WHERE submitTime IS NULL "
            sqlS += "OR (submitTime<:lockTimeLimit AND lockedBy IS NOT NULL) "
            sqlS += "OR (submitTime<:lookupTimeLimit AND lockedBy IS NULL) "
            sqlS += "ORDER BY submitTime "
            # sql to get queues
            sqlQ = f"SELECT queueName, jobType, resourceType, nNewWorkers FROM {pandaQueueTableName} "
            sqlQ += "WHERE siteName=:siteName "
            # sql to get orphaned workers
            sqlO = f"SELECT workerID FROM {workTableName} "
            sqlO += "WHERE computingSite=:computingSite "
            sqlO += "AND status=:status AND modificationTime<:timeLimit "
            # sql to delete orphaned workers. Not to use bulk delete to avoid deadlock with 0-record deletion
            sqlD = f"DELETE FROM {workTableName} "
            sqlD += "WHERE workerID=:workerID "
            # sql to count nQueue
            sqlN = f"SELECT status, COUNT(*) cnt FROM {workTableName} "
            sqlN += "WHERE computingSite=:computingSite "
            # sql to count re-fillers
            sqlR = f"SELECT COUNT(*) cnt FROM {workTableName} "
            sqlR += "WHERE computingSite=:computingSite AND status=:status "
            sqlR += "AND nJobsToReFill IS NOT NULL AND nJobsToReFill>0 "
            # sql to update timestamp and lock site
            sqlU = f"UPDATE {pandaQueueTableName} SET submitTime=:submitTime,lockedBy=:lockedBy "
            sqlU += "WHERE siteName=:siteName "
            sqlU += "AND (submitTime IS NULL OR submitTime<:timeLimit) "
            # get sites
            timeNow = core_utils.naive_utcnow()
            varMap = dict()
            varMap[":lockTimeLimit"] = timeNow - datetime.timedelta(seconds=queue_lock_interval)
            varMap[":lookupTimeLimit"] = timeNow - datetime.timedelta(seconds=lookup_interval)
            self.execute(sqlS, varMap)
            resS = self.cur.fetchall()
            for (siteName,) in resS:
                # update timestamp to lock the site
                varMap = dict()
                varMap[":siteName"] = siteName
                varMap[":submitTime"] = timeNow
                varMap[":lockedBy"] = locked_by
                varMap[":timeLimit"] = timeNow - datetime.timedelta(seconds=lookup_interval)
                self.execute(sqlU, varMap)
                nRow = self.cur.rowcount
                # commit
                self.commit()
                # skip if not locked
                if nRow == 0:
                    continue
                # get queues
                varMap = dict()
                varMap[":siteName"] = siteName
                self.execute(sqlQ, varMap)
                resQ = self.cur.fetchall()
                for queueName, jobType, resourceType, nNewWorkers in resQ:
                    # delete orphaned workers
                    varMap = dict()
                    varMap[":computingSite"] = queueName
                    varMap[":status"] = WorkSpec.ST_pending
                    varMap[":timeLimit"] = timeNow - datetime.timedelta(seconds=lock_interval)
                    sqlO_tmp = sqlO
                    if jobType != "ANY":
                        varMap[":jobType"] = jobType
                        sqlO_tmp += "AND jobType=:jobType "
                    if resourceType != "ANY":
                        varMap[":resourceType"] = resourceType
                        sqlO_tmp += "AND resourceType=:resourceType "
                    self.execute(sqlO_tmp, varMap)
                    resO = self.cur.fetchall()
                    for (tmpWorkerID,) in resO:
                        varMap = dict()
                        varMap[":workerID"] = tmpWorkerID
                        self.execute(sqlD, varMap)
                        # commit
                        self.commit()

                    # count nQueue
                    varMap = dict()
                    varMap[":computingSite"] = queueName
                    varMap[":resourceType"] = resourceType
                    sqlN_tmp = sqlN
                    if jobType != "ANY":
                        varMap[":jobType"] = jobType
                        sqlN_tmp += "AND jobType=:jobType "
                    if resourceType != "ANY":
                        varMap[":resourceType"] = resourceType
                        sqlN_tmp += "AND resourceType=:resourceType "
                    sqlN_tmp += "GROUP BY status "
                    self.execute(sqlN_tmp, varMap)
                    nQueue = 0
                    nReady = 0
                    nRunning = 0
                    for workerStatus, tmpNum in self.cur.fetchall():
                        if workerStatus in [WorkSpec.ST_submitted, WorkSpec.ST_pending, WorkSpec.ST_idle]:
                            nQueue += tmpNum
                        elif workerStatus in [WorkSpec.ST_ready]:
                            nReady += tmpNum
                        elif workerStatus in [WorkSpec.ST_running]:
                            nRunning += tmpNum

                    # count nFillers
                    varMap = dict()
                    varMap[":computingSite"] = queueName
                    varMap[":status"] = WorkSpec.ST_running
                    sqlR_tmp = sqlR
                    if jobType != "ANY":
                        varMap[":jobType"] = jobType
                        sqlR_tmp += "AND jobType=:jobType "
                    if resourceType != "ANY":
                        varMap[":resourceType"] = resourceType
                        sqlR_tmp += "AND resourceType=:resourceType "
                    self.execute(sqlR_tmp, varMap)
                    (nReFill,) = self.cur.fetchone()
                    nReady += nReFill
                    # add
                    retMap.setdefault(queueName, {})
                    retMap[queueName].setdefault(jobType, {})
                    retMap[queueName][jobType][resourceType] = {"nReady": nReady, "nRunning": nRunning, "nQueue": nQueue, "nNewWorkers": nNewWorkers}
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
                maxJobs = int(min(maxJobs, nAvailableJobs) / n_jobs_per_worker) * n_jobs_per_worker
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

    # add a seq number
    def add_seq_number(self, number_name, init_value):
        try:
            # check if already there
            sqlC = f"SELECT curVal FROM {seqNumberTableName} WHERE numberName=:numberName "
            varMap = dict()
            varMap[":numberName"] = number_name
            self.execute(sqlC, varMap)
            res = self.cur.fetchone()
            # insert if missing
            if res is None:
                # make spec
                seqNumberSpec = SeqNumberSpec()
                seqNumberSpec.numberName = number_name
                seqNumberSpec.curVal = init_value
                # insert
                sqlI = f"INSERT INTO {seqNumberTableName} ({SeqNumberSpec.column_names()}) "
                sqlI += SeqNumberSpec.bind_values_expression()
                varMap = seqNumberSpec.values_list()
                self.execute(sqlI, varMap)
            # commit
            self.commit()
            return True
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # get next value for a seq number
    def get_next_seq_number(self, number_name):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"name={number_name}", method_name="get_next_seq_number")
            # increment
            sqlU = f"UPDATE {seqNumberTableName} SET curVal=curVal+1 WHERE numberName=:numberName "
            varMap = dict()
            varMap[":numberName"] = number_name
            self.execute(sqlU, varMap)
            # get
            sqlG = f"SELECT curVal FROM {seqNumberTableName} WHERE numberName=:numberName "
            varMap = dict()
            varMap[":numberName"] = number_name
            self.execute(sqlG, varMap)
            (retVal,) = self.cur.fetchone()
            # commit
            self.commit()
            tmpLog.debug(f"got {retVal}")
            return retVal
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # get last update time for a cached info
    def get_cache_last_update_time(self, main_key, sub_key):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"mainKey={main_key} subKey={sub_key}", method_name="get_cache_last_update_time")
            # get
            varMap = dict()
            varMap[":mainKey"] = main_key
            sqlU = f"SELECT lastUpdate FROM {cacheTableName} WHERE mainKey=:mainKey "
            if sub_key is not None:
                sqlU += "AND subKey=:subKey "
                varMap[":subKey"] = sub_key
            self.execute(sqlU, varMap)
            retVal = self.cur.fetchone()
            if retVal is not None:
                (retVal,) = retVal
            # commit
            self.commit()
            tmpLog.debug(f"got {retVal}")
            return retVal
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # refresh a cached info
    def refresh_cache(self, main_key, sub_key, new_info):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"mainKey={main_key} subKey={sub_key}", method_name="refresh_cache")
            # make spec
            cacheSpec = CacheSpec()
            cacheSpec.lastUpdate = core_utils.naive_utcnow()
            cacheSpec.data = new_info
            # check if already there
            varMap = dict()
            varMap[":mainKey"] = main_key
            sqlC = f"SELECT lastUpdate FROM {cacheTableName} WHERE mainKey=:mainKey "
            if sub_key is not None:
                sqlC += "AND subKey=:subKey "
                varMap[":subKey"] = sub_key
            self.execute(sqlC, varMap)
            retC = self.cur.fetchone()
            if retC is None:
                # insert if missing
                cacheSpec.mainKey = main_key
                cacheSpec.subKey = sub_key
                sqlU = f"INSERT INTO {cacheTableName} ({CacheSpec.column_names()}) "
                sqlU += CacheSpec.bind_values_expression()
                varMap = cacheSpec.values_list()
            else:
                # update
                sqlU = f"UPDATE {cacheTableName} SET {cacheSpec.bind_update_changes_expression()} "
                sqlU += "WHERE mainKey=:mainKey "
                varMap = cacheSpec.values_map(only_changed=True)
                varMap[":mainKey"] = main_key
                if sub_key is not None:
                    sqlU += "AND subKey=:subKey "
                    varMap[":subKey"] = sub_key
            self.execute(sqlU, varMap)
            # commit
            self.commit()
            # put into global dict
            cacheKey = f"cache|{main_key}|{sub_key}"
            globalDict = core_utils.get_global_dict()
            globalDict.acquire()
            globalDict[cacheKey] = cacheSpec.data
            globalDict.release()
            tmpLog.debug("refreshed")
            return True
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # get a cached info
    def get_cache(self, main_key, sub_key=None, from_local_cache=True):
        useDB = False
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"mainKey={main_key} subKey={sub_key}", method_name="get_cache")
            tmpLog.debug("start")
            # get from global dict
            cacheKey = f"cache|{main_key}|{sub_key}"
            globalDict = core_utils.get_global_dict()
            # lock dict
            globalDict.acquire()
            # found
            if from_local_cache and cacheKey in globalDict:
                # release dict
                globalDict.release()
                # make spec
                cacheSpec = CacheSpec()
                cacheSpec.data = globalDict[cacheKey]
            else:
                # read from database
                useDB = True
                sql = f"SELECT {CacheSpec.column_names()} FROM {cacheTableName} "
                sql += "WHERE mainKey=:mainKey "
                varMap = dict()
                varMap[":mainKey"] = main_key
                if sub_key is not None:
                    sql += "AND subKey=:subKey "
                    varMap[":subKey"] = sub_key
                self.execute(sql, varMap)
                resJ = self.cur.fetchall()
                # commit
                self.commit()
                if not resJ:
                    # release dict
                    globalDict.release()
                    return None
                else:
                    res_one = resJ[0]
                    # make spec
                    cacheSpec = CacheSpec()
                    cacheSpec.pack(res_one)
                    # put into global dict
                    globalDict[cacheKey] = cacheSpec.data
                # release dict
                globalDict.release()
            tmpLog.debug("done")
            # return
            return cacheSpec
        except Exception:
            if useDB:
                # roll back
                self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # store commands
    def store_commands(self, command_specs):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="store_commands")
        tmpLog.debug(f"{len(command_specs)} commands")
        if not command_specs:
            return True
        try:
            # sql to insert a command
            sql = f"INSERT INTO {commandTableName} ({CommandSpec.column_names()}) "
            sql += CommandSpec.bind_values_expression()
            # loop over all commands
            var_maps = []
            for command_spec in command_specs:
                var_map = command_spec.values_list()
                var_maps.append(var_map)
            # insert
            self.executemany(sql, var_maps)
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

    # get commands for a receiver
    def get_commands_for_receiver(self, receiver, command_pattern=None):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_commands_for_receiver")
            tmpLog.debug("start")
            # sql to get commands
            varMap = dict()
            varMap[":receiver"] = receiver
            varMap[":processed"] = 0
            sqlG = f"SELECT {CommandSpec.column_names()} FROM {commandTableName} "
            sqlG += "WHERE receiver=:receiver AND processed=:processed "
            if command_pattern is not None:
                varMap[":command"] = command_pattern
                if "%" in command_pattern:
                    sqlG += "AND command LIKE :command "
                else:
                    sqlG += "AND command=:command "
            sqlG += "FOR UPDATE "
            # sql to lock command
            sqlL = f"UPDATE {commandTableName} SET processed=:processed WHERE command_id=:command_id "
            self.execute(sqlG, varMap)
            commandSpecList = []
            for res in self.cur.fetchall():
                # make command
                commandSpec = CommandSpec()
                commandSpec.pack(res)
                # lock
                varMap = dict()
                varMap[":command_id"] = commandSpec.command_id
                varMap[":processed"] = 1
                self.execute(sqlL, varMap)
                # append
                commandSpecList.append(commandSpec)
            # commit
            self.commit()
            tmpLog.debug(f"got {len(commandSpecList)} commands")
            return commandSpecList
        except Exception:
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    # get command ids that have been processed and need to be acknowledged to panda server
    def get_commands_ack(self):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_commands_ack")
            tmpLog.debug("start")
            # sql to get commands that have been processed and need acknowledgement
            sql = f"""
                  SELECT command_id FROM {commandTableName}
                  WHERE ack_requested=1
                  AND processed=1
                  """
            self.execute(sql)
            command_ids = [row[0] for row in self.cur.fetchall()]
            tmpLog.debug(f"command_ids {command_ids}")
            return command_ids
        except Exception:
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    def clean_commands_by_id(self, commands_ids):
        """
        Deletes the commands specified in a list of IDs
        """
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="clean_commands_by_id")
        try:
            # sql to delete a specific command
            sql = f"""
                  DELETE FROM {commandTableName}
                  WHERE command_id=:command_id"""

            for command_id in commands_ids:
                var_map = {":command_id": command_id}
                self.execute(sql, var_map)
            self.commit()
            return True
        except Exception:
            self.rollback()
            core_utils.dump_error_message(tmpLog)
            return False

    def clean_processed_commands(self):
        """
        Deletes the commands that have been processed and do not need acknowledgement
        """
        tmpLog = core_utils.make_logger(_logger, method_name="clean_processed_commands")
        try:
            # sql to delete all processed commands that do not need an ACK
            sql = f"""
                  DELETE FROM {commandTableName}
                  WHERE (ack_requested=0 AND processed=1)
                  """
            self.execute(sql)
            self.commit()
            return True
        except Exception:
            self.rollback()
            core_utils.dump_error_message(tmpLog)
            return False

    # get workers to kill
    def get_workers_to_kill(self, max_workers, check_interval):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_workers_to_kill")
            tmpLog.debug("start")
            # sql to get worker IDs
            sqlW = f"SELECT workerID,status,configID FROM {workTableName} "
            sqlW += "WHERE killTime IS NOT NULL AND killTime<:checkTimeLimit "
            sqlW += f"ORDER BY killTime LIMIT {max_workers} "
            # sql to lock or release worker
            sqlL = f"UPDATE {workTableName} SET killTime=:setTime "
            sqlL += "WHERE workerID=:workerID "
            sqlL += "AND killTime IS NOT NULL AND killTime<:checkTimeLimit "
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

    # get worker stats
    def get_worker_stats(self, site_name):
        try:
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
            sqlW = "SELECT wt.status, wt.computingSite, pq.jobType, pq.resourceType, COUNT(*) cnt "
            sqlW += f"FROM {workTableName} wt, {pandaQueueTableName} pq "
            sqlW += "WHERE pq.siteName=:siteName AND wt.computingSite=pq.queueName AND wt.status IN (:st1,:st2,:st3) "
            sqlW += "GROUP BY wt.status, wt.computingSite, pq.jobType, pq.resourceType "
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
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # get worker stats
    def get_worker_stats_bulk(self, active_ups_queues):
        try:
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
            sqlW = "SELECT wt.status, wt.computingSite, wt.jobType, wt.resourceType, COUNT(*) cnt "
            sqlW += f"FROM {workTableName} wt "
            sqlW += "WHERE wt.status IN (:st1,:st2,:st3) "
            sqlW += "GROUP BY wt.status,wt.computingSite, wt.jobType, wt.resourceType "
            # get worker stats
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
                        retMap[ups_queue] = {"managed": {"SCORE": {"running": 0, "submitted": 0, "finished": 0, "to_submit": 0}}}

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

    # get full worker stats
    def get_worker_stats_full(self, filter_site_list=None):
        try:
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
            sqlW = "SELECT wt.status, wt.computingSite, wt.jobType, wt.resourceType, COUNT(*) cnt "
            sqlW += f"FROM {workTableName} wt "
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
                retMap[computingSite][jobType].setdefault(resourceType, {"running": 0, "submitted": 0, "to_submit": 0})
                retMap[computingSite][jobType][resourceType][workerStatus] = cnt
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

    # send kill command to workers associated to a job
    def mark_workers_to_kill_by_pandaid(self, panda_id, delay_seconds=None):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"PandaID={panda_id}", method_name="mark_workers_to_kill_by_pandaid")
            tmpLog.debug("start")
            # sql to set killTime
            sqlL = f"UPDATE {workTableName} SET killTime=:setTime "
            sqlL += "WHERE workerID=:workerID AND killTime IS NULL AND NOT status IN (:st1,:st2,:st3) "
            # sql to get associated workers
            sqlA = f"SELECT workerID FROM {jobWorkerTableName} "
            sqlA += "WHERE PandaID=:pandaID "
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
            sql_reset = f"UPDATE {pandaQueueTableName} "
            sql_reset += "SET nNewWorkers=:zero WHERE siteName=:siteName "

            # sql to get resource types
            sql_get_job_resource = f"SELECT jobType, resourceType FROM {pandaQueueTableName} "
            sql_get_job_resource += "WHERE siteName=:siteName "
            sql_get_job_resource += "FOR UPDATE "

            # sql to update nQueueLimit
            sql_update_queue = f"UPDATE {pandaQueueTableName} "
            sql_update_queue += "SET nNewWorkers=:nQueue "
            sql_update_queue += "WHERE siteName=:siteName AND jobType=:jobType AND resourceType=:resourceType "

            # sql to get num of submitted workers
            sql_count_workers = "SELECT COUNT(*) cnt "
            sql_count_workers += f"FROM {workTableName} wt, {pandaQueueTableName} pq "
            sql_count_workers += "WHERE pq.siteName=:siteName AND wt.computingSite=pq.queueName AND wt.status=:status "
            sql_count_workers += "AND pq.jobType=:jobType AND pq.resourceType=:resourceType "

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

    # delete old process locks
    def clean_process_locks(self):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="clean_process_locks")
            tmpLog.debug("start")
            # delete locks
            sqlW = f"DELETE FROM {processLockTableName} "
            # get worker stats
            self.execute(sqlW)
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

    # get a process lock
    def get_process_lock(self, process_name, locked_by, lock_interval):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"proc={process_name} by={locked_by}", method_name="get_process_lock")
            tmpLog.debug("start")
            # delete old lock
            sqlD = f"DELETE FROM {processLockTableName} "
            sqlD += "WHERE lockTime<:timeLimit "
            varMap = dict()
            varMap[":timeLimit"] = core_utils.naive_utcnow() - datetime.timedelta(hours=6)
            self.execute(sqlD, varMap)
            # commit
            self.commit()
            # check lock
            sqlC = f"SELECT lockTime FROM {processLockTableName} "
            sqlC += "WHERE processName=:processName "
            varMap = dict()
            varMap[":processName"] = process_name
            self.execute(sqlC, varMap)
            resC = self.cur.fetchone()
            retVal = False
            timeNow = core_utils.naive_utcnow()
            if resC is None:
                # insert lock if missing
                sqlI = f"INSERT INTO {processLockTableName} ({ProcessLockSpec.column_names()}) "
                sqlI += ProcessLockSpec.bind_values_expression()
                processLockSpec = ProcessLockSpec()
                processLockSpec.processName = process_name
                processLockSpec.lockedBy = locked_by
                processLockSpec.lockTime = timeNow
                varMap = processLockSpec.values_list()
                self.execute(sqlI, varMap)
                retVal = True
            else:
                (oldLockTime,) = resC
                timeLimit = timeNow - datetime.timedelta(seconds=lock_interval)
                if oldLockTime <= timeLimit:
                    # update lock if old
                    sqlU = f"UPDATE {processLockTableName} SET lockedBy=:lockedBy,lockTime=:timeNow "
                    sqlU += "WHERE processName=:processName AND lockTime<=:timeLimit "
                    varMap = dict()
                    varMap[":processName"] = process_name
                    varMap[":lockedBy"] = locked_by
                    varMap[":timeLimit"] = timeLimit
                    varMap[":timeNow"] = timeNow
                    self.execute(sqlU, varMap)
                    if self.cur.rowcount > 0:
                        retVal = True
            # commit
            self.commit()
            tmpLog.debug(f"done with {retVal}")
            return retVal
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # release a process lock
    def release_process_lock(self, process_name, locked_by):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, f"proc={process_name} by={locked_by}", method_name="release_process_lock")
            tmpLog.debug("start")
            # delete old lock
            sqlC = f"DELETE FROM {processLockTableName} "
            sqlC += "WHERE processName=:processName AND lockedBy=:lockedBy "
            varMap = dict()
            varMap[":processName"] = process_name
            varMap[":lockedBy"] = locked_by
            self.execute(sqlC, varMap)
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

    # get file status
    def get_file_status(self, lfn, file_type, endpoint, job_status):
        try:
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
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # change file status
    def change_file_status(self, panda_id, data, locked_by):
        try:
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
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # get group for a file
    def get_group_for_file(self, lfn, file_type, endpoint):
        try:
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
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # get files with a group ID
    def get_files_with_group_id(self, group_id):
        try:
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
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    # update group status
    def update_file_group_status(self, group_id, status_string):
        try:
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
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # get file group status
    def get_file_group_status(self, group_id):
        try:
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
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

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

    # set file group
    def set_file_group(self, file_specs, group_id, status_string):
        try:
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
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # refresh file group info
    def refresh_file_group_info(self, job_spec):
        try:
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

    # get queue status
    def get_worker_limits(self, site_name):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, token=f"site_name={site_name}", method_name="get_worker_limits")
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

    # get worker CE stats
    def get_worker_ce_stats(self, site_name):
        try:
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
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # get worker CE backend throughput
    def get_worker_ce_backend_throughput(self, site_name, time_window):
        try:
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
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # add dialog message
    def add_dialog_message(self, message, level, module_name, identifier=None):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="add_dialog_message")
            tmpLog.debug("start")
            # delete old messages
            sqlS = f"SELECT diagID FROM {diagTableName} "
            sqlS += "WHERE creationTime<:timeLimit "
            varMap = dict()
            varMap[":timeLimit"] = core_utils.naive_utcnow() - datetime.timedelta(minutes=60)
            self.execute(sqlS, varMap)
            resS = self.cur.fetchall()
            sqlD = f"DELETE FROM {diagTableName} "
            sqlD += "WHERE diagID=:diagID "
            for (diagID,) in resS:
                varMap = dict()
                varMap[":diagID"] = diagID
                self.execute(sqlD, varMap)
                # commit
                self.commit()
            # make spec
            diagSpec = DiagSpec()
            diagSpec.moduleName = module_name
            diagSpec.creationTime = core_utils.naive_utcnow()
            diagSpec.messageLevel = level
            try:
                diagSpec.identifier = identifier[:100]
            except Exception:
                pass
            diagSpec.diagMessage = message[:500]
            # insert
            sqlI = f"INSERT INTO {diagTableName} ({DiagSpec.column_names()}) "
            sqlI += DiagSpec.bind_values_expression()
            varMap = diagSpec.values_list()
            self.execute(sqlI, varMap)
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

    # get dialog messages to send
    def get_dialog_messages_to_send(self, n_messages, lock_interval):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_dialog_messages_to_send")
            tmpLog.debug("start")
            # sql to select messages
            sqlD = f"SELECT diagID FROM {diagTableName} "
            sqlD += "WHERE (lockTime IS NULL OR lockTime<:timeLimit) "
            sqlD += f"ORDER BY diagID LIMIT {n_messages} "
            # sql to lock message
            sqlL = f"UPDATE {diagTableName} SET lockTime=:timeNow "
            sqlL += "WHERE diagID=:diagID "
            sqlL += "AND (lockTime IS NULL OR lockTime<:timeLimit) "
            # sql to get message
            sqlM = f"SELECT {DiagSpec.column_names()} FROM {diagTableName} "
            sqlM += "WHERE diagID=:diagID "
            # select messages
            timeLimit = core_utils.naive_utcnow() - datetime.timedelta(seconds=lock_interval)
            varMap = dict()
            varMap[":timeLimit"] = timeLimit
            self.execute(sqlD, varMap)
            resD = self.cur.fetchall()
            diagList = []
            for (diagID,) in resD:
                # lock
                varMap = dict()
                varMap[":diagID"] = diagID
                varMap[":timeLimit"] = timeLimit
                varMap[":timeNow"] = core_utils.naive_utcnow()
                self.execute(sqlL, varMap)
                nRow = self.cur.rowcount
                if nRow == 1:
                    # get
                    varMap = dict()
                    varMap[":diagID"] = diagID
                    self.execute(sqlM, varMap)
                    resM = self.cur.fetchone()
                    # make spec
                    diagSpec = DiagSpec()
                    diagSpec.pack(resM)
                    diagList.append(diagSpec)
                # commit
                self.commit()
            tmpLog.debug(f"got {len(diagList)} messages")
            return diagList
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    # delete dialog messages
    def delete_dialog_messages(self, ids):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="delete_dialog_messages")
            tmpLog.debug("start")
            # sql to delete message
            sqlM = f"DELETE FROM {diagTableName} "
            sqlM += "WHERE diagID=:diagID "
            for diagID in ids:
                # lock
                varMap = dict()
                varMap[":diagID"] = diagID
                self.execute(sqlM, varMap)
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

    # insert service metrics
    def insert_service_metrics(self, service_metric_spec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="insert_service_metrics")
        tmpLog.debug("start")
        try:
            sql = f"INSERT INTO {serviceMetricsTableName} ({ServiceMetricSpec.column_names()}) "
            sql += ServiceMetricSpec.bind_values_expression()
            var_map = service_metric_spec.values_list()

            self.execute(sql, var_map)
            self.commit()

            return True
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(tmpLog)
            # return
            return False

    # get service metrics
    def get_service_metrics(self, last_update):
        try:
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
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

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

    # get all active input files
    def get_all_active_input_files(self):
        try:
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
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return set()

    # get full job stats
    def get_job_stats_full(self, filter_site_list=None):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="get_job_stats_full")
            tmpLog.debug("start")
            # get job stats
            varMap = dict()
            sqlJ = "SELECT jt.status, jt.computingSite, jt.resourceType, jt.coreCount, COUNT(*) cnt "
            sqlJ += f"FROM {jobTableName} jt "
            if filter_site_list is not None:
                site_var_name_list = []
                for j, site in enumerate(filter_site_list):
                    site_var_name = f":site{j}"
                    site_var_name_list.append(site_var_name)
                    varMap[site_var_name] = site
                filter_queue_str = ",".join(site_var_name_list)
                sqlJ += f"WHERE jt.computingSite IN ({filter_queue_str}) "
            sqlJ += "GROUP BY jt.status,jt.computingSite, jt.resourceType "
            self.execute(sqlJ, varMap)
            resJ = self.cur.fetchall()
            # fill return map
            retMap = dict()
            for jobStatus, computingSite, resourceType, coreCount, cnt in resJ:
                jobStatus = str(jobStatus)
                computingSite = str(computingSite)
                if resourceType:
                    resourceType = str(resourceType)
                else:
                    resourceType = "MCORE" if coreCount and coreCount > 1 else "SCORE"
                retMap.setdefault(computingSite, {})
                retMap[computingSite].setdefault(resourceType, {"running": 0, "starting": 0})
                retMap[computingSite][resourceType][jobStatus] = cnt
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
