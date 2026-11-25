"""
DB proxy base: database connection and SQL execution helpers

"""

import copy
import functools
import inspect
import os
import re
import threading
import time

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils

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


# get the logger a failed DB proxy method made for itself, to keep its token (e.g. PandaID=...) in the error message
def _get_method_logger(func, exc_traceback):
    # find the frame of the method in the traceback
    tb = exc_traceback
    while tb is not None:
        if tb.tb_frame.f_code is func.__code__:
            for var_name in ("tmpLog", "tmp_log", "tmp_logger"):
                tmp_log = tb.tb_frame.f_locals.get(var_name)
                if isinstance(tmp_log, core_utils.LogWrapper):
                    return tmp_log
            break
        tb = tb.tb_next
    # fallback when the method failed before making its logger or has none
    return core_utils.make_logger(_logger, method_name=func.__name__)


# decorator for DB proxy methods: on any exception, roll back, dump the error, and return a fresh copy of default
def db_error_handler(default=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                # roll back
                self.rollback()
                # dump error
                core_utils.dump_error_message(_get_method_logger(func, e.__traceback__))
                # return
                return copy.deepcopy(default)

        return wrapper

    return decorator


# base class of DB proxy modules
class BaseModule(object):
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
            sql = re.sub(" FOR UPDATE", " ", sql, flags=re.I)
            sql = re.sub("INSERT IGNORE", "INSERT OR IGNORE", sql, flags=re.I)
        else:
            sql = re.sub("INSERT OR IGNORE", "INSERT IGNORE", sql, flags=re.I)
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
            sql_str = newSQL.replace("\n", " ").strip()
            self.verbLog.debug(f"thr={self.thrName}  {sw.get_elapsed_time()}  sql=[{sql_str}]")
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
