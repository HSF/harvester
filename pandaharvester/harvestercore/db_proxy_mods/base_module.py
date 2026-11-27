"""
DB proxy base: database connection and SQL execution helpers

"""

import copy
import datetime
import functools
import os
import re
import sys
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
workerCheckTableName = "worker_check_table"
workerTargetCacheTableName = "worker_target_cache_table"

# connection lock
conLock = threading.Lock()

# SQL that writes or locks rows; it takes the application side lock until commit or rollback
_write_sql_pattern = re.compile(r"^(INSERT|UPDATE|DELETE)| FOR UPDATE", re.I)
# placeholders such as :PandaID
_placeholder_pattern = re.compile(":[^ $,)]+")


# make a list of bind variables for IN, filling var_map
def _bind_in(prefix, values, var_map):
    names = []
    for idx, val in enumerate(values):
        name = f":{prefix}{idx}"
        var_map[name] = val
        names.append(name)
    return ",".join(names)


# sqlite adapters and converters, copied from the default ones of python
def _adapt_date(val):
    return val.isoformat()


def _adapt_datetime(val):
    return val.isoformat(" ")


def _convert_date(val):
    return datetime.date(*map(int, val.split(b"-")))


def _convert_timestamp(val):
    datepart, timepart = val.split(b" ")
    year, month, day = map(int, datepart.split(b"-"))
    timepart_full = timepart.split(b".")
    hours, minutes, seconds = map(int, timepart_full[0].split(b":"))
    if len(timepart_full) == 2:
        microseconds = int(f"{timepart_full[1].decode():0<6.6}")
    else:
        microseconds = 0
    return datetime.datetime(year, month, day, hours, minutes, seconds, microseconds)


# make a logger for a DB proxy method, with the process and thread IDs first in the token
def make_logger(token=None, method_name=None):
    if method_name is None:
        method_name = sys._getframe(1).f_code.co_name
    pid = core_utils.get_pid(with_hostname=False)
    return core_utils.make_logger(_logger, pid if token is None else f"{pid} {token}", method_name=method_name)


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
    return make_logger(method_name=func.__name__)


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
    def __init__(self, read_only=False):
        self.verbose = harvester_config.db.verbose
        self.reconnectTimeout = getattr(harvester_config.db, "reconnectTimeout", 300)
        self.read_only = read_only
        # connect DB
        self._connect_db()
        self.lockDB = False
        # using application side lock if DB doesn't have a mechanism for exclusive access
        self.usingAppLock = harvester_config.db.engine != "mariadb"

    # connect DB
    def _connect_db(self):
        if harvester_config.db.engine == "mariadb":
            host = getattr(harvester_config.db, "host", "127.0.0.1")
            port = getattr(harvester_config.db, "port", 3306)
            if getattr(harvester_config.db, "useMySQLdb", False) is True:
                import MySQLdb
                import MySQLdb.cursors

                # row which is accessible also by column name
                def make_row(tmpRet, attributes):
                    tmpRet = core_utils.DictTupleHybrid(tmpRet)
                    tmpRet.set_attributes(attributes)
                    return tmpRet

                class MyCursor(MySQLdb.cursors.Cursor):
                    def fetchone(self):
                        tmpRet = MySQLdb.cursors.Cursor.fetchone(self)
                        if tmpRet is None:
                            return None
                        return make_row(tmpRet, [d[0] for d in self.description])

                    def fetchall(self):
                        tmpRets = MySQLdb.cursors.Cursor.fetchall(self)
                        if len(tmpRets) == 0:
                            return tmpRets
                        attributes = [d[0] for d in self.description]
                        return [make_row(tmpRet, attributes) for tmpRet in tmpRets]

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

            # explicit adapters and converter, the same as the default ones deprecated since python 3.12
            sqlite3.register_adapter(datetime.date, _adapt_date)
            sqlite3.register_adapter(datetime.datetime, _adapt_datetime)
            sqlite3.register_converter("date", _convert_date)
            sqlite3.register_converter("timestamp", _convert_timestamp)
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
        if harvester_config.db.engine != "mariadb":
            return
        tmpLog = make_logger(method_name=self._get_caller_name())
        tmpLog.warning(f"exception of mysql {exc.__class__.__name__} occurred")
        # renew connection for operational errors
        if getattr(harvester_config.db, "useMySQLdb", False) is True:
            import MySQLdb

            operational_error = MySQLdb.OperationalError
        else:
            import mysql.connector

            operational_error = mysql.connector.errors.OperationalError
        if not isinstance(exc, operational_error):
            return
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
                time.sleep(core_utils.retry_period_sec(n_retry, increment=2, max_seconds=300, min_seconds=1))
                n_retry += 1

    # take the application side lock until commit or rollback if the SQL writes or locks rows
    def _lock_db_for_write(self, sql):
        if self.usingAppLock and _write_sql_pattern.search(sql) is not None:
            self.lockDB = True

    # convert SQL to the syntax of the DB engine
    def _convert_sql(self, sql):
        if harvester_config.db.engine == "sqlite":
            sql = re.sub(" FOR UPDATE", " ", sql, flags=re.I)
            sql = re.sub("INSERT IGNORE", "INSERT OR IGNORE", sql, flags=re.I)
        else:
            sql = re.sub("INSERT OR IGNORE", "INSERT IGNORE", sql, flags=re.I)
        return sql

    # make parameters for converted SQL from placeholders and param dict or list
    def _make_params(self, sql, placeholders, varmap):
        # list of values in the order of placeholders
        if not isinstance(varmap, dict):
            if harvester_config.db.engine == "sqlite":
                # qmark style; sqlite doesn't allow a list for named placeholders since python 3.14
                sql = _placeholder_pattern.sub("?", sql)
            else:
                # printf style for mariadb
                sql = _placeholder_pattern.sub("%s", sql)
            return sql, varmap
        for item in placeholders:
            if item not in varmap:
                raise KeyError(f"{item} is missing in SQL parameters")
        if harvester_config.db.engine == "sqlite":
            # named style; a placeholder used more than once takes one value
            return sql, {item[1:]: varmap[item] for item in placeholders}
        # printf style for mariadb; one value for each placeholder
        return _placeholder_pattern.sub("%s", sql), [varmap[item] for item in placeholders]

    # convert SQL and param dict or list for the DB engine
    def convert_params(self, sql, varmap):
        self._lock_db_for_write(sql)
        sql = self._convert_sql(sql)
        return self._make_params(sql, _placeholder_pattern.findall(sql), varmap)

    # get the application side lock unless already held for a write
    def _acquire_app_lock(self):
        if self.usingAppLock and not self.lockDB:
            if self.verbose:
                self._log_verbose("locking")
            conLock.acquire()
            if self.verbose:
                self._log_verbose("locked")

    # release the application side lock unless it is kept for a write until commit or rollback
    def _release_app_lock(self):
        if self.usingAppLock and not self.lockDB:
            if self.verbose:
                self._log_verbose("release")
            conLock.release()

    # release the application side lock kept for a write
    def _release_write_lock(self, action):
        if self.usingAppLock and self.lockDB:
            if self.verbose:
                self._log_verbose(f"release with {action}")
            conLock.release()
            self.lockDB = False

    # get the name of the DB proxy method running SQL on this connection, skipping the internals of this class and private helpers
    def _get_caller_name(self):
        frame = sys._getframe(1)
        while frame is not None:
            code = frame.f_code
            if code is _error_handler_code:
                # rollback by db_error_handler after the method has exited
                return frame.f_locals["func"].__name__
            if code not in _base_codes and not (code.co_name.startswith("_") and frame.f_locals.get("self") is self):
                return code.co_name
            frame = frame.f_back
        return "unknown"

    # log in verbose mode with the method name and the process and thread IDs, e.g. "register_worker <123-7f0a> execute : sql=..."
    def _log_verbose(self, msg):
        operation = sys._getframe(1).f_code.co_name
        if operation.startswith("_"):
            # called from a lock helper
            operation = sys._getframe(2).f_code.co_name
        make_logger(method_name=self._get_caller_name()).debug(f"{operation} : {msg}")

    # wrapper for execute
    def execute(self, sql, varmap=None):
        sw = core_utils.get_stopwatch()
        if varmap is None:
            varmap = dict()
        self._acquire_app_lock()
        try:
            if self.verbose:
                self._log_verbose(f"sql={sql} var={varmap}")
            newSQL, params = self.convert_params(sql, varmap)
            try:
                retVal = self.cur.execute(newSQL, params)
            except Exception as e:
                self._handle_exception(e)
                if self.verbose:
                    self._log_verbose("exception")
                raise
        finally:
            self._release_app_lock()
        if self.verbose:
            sql_str = newSQL.replace("\n", " ").strip()
            self._log_verbose(f"took {sw.get_elapsed_time_in_sec():.3f} sec sql=[{sql_str}]")
        return retVal

    # wrapper for executemany
    def executemany(self, sql, varmap_list):
        self._acquire_app_lock()
        try:
            if self.verbose:
                self._log_verbose(f"sql={sql} var={varmap_list}")
            # nothing to execute
            if not varmap_list:
                return None
            # convert SQL once and make parameters for each param dict or list
            self._lock_db_for_write(sql)
            convertedSQL = self._convert_sql(sql)
            placeholders = _placeholder_pattern.findall(convertedSQL)
            paramList = []
            for varMap in varmap_list:
                if varMap is None:
                    varMap = dict()
                newSQL, params = self._make_params(convertedSQL, placeholders, varMap)
                paramList.append(params)
            try:
                # all in one call so that rowcount covers all rows
                retVal = self.cur.executemany(newSQL, paramList)
            except Exception as e:
                self._handle_exception(e)
                if self.verbose:
                    self._log_verbose("exception")
                raise
        finally:
            self._release_app_lock()
        return retVal

    # commit
    def commit(self):
        try:
            self.con.commit()
        except Exception as e:
            self._handle_exception(e)
            if self.verbose:
                self._log_verbose("exception")
            raise
        self._release_write_lock("commit")

    # rollback
    def rollback(self):
        try:
            self.con.rollback()
        except Exception as e:
            self._handle_exception(e)
            if self.verbose:
                self._log_verbose("exception")
        finally:
            self._release_write_lock("rollback")


# code objects to skip when looking for the DB proxy method running SQL
_base_codes = {func.__code__ for func in vars(BaseModule).values() if callable(func)}
_error_handler_code = db_error_handler()(lambda self: None).__code__
