"""
DB proxy module for table creation and schema checks

"""

import re
import sys

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.cache_spec import CacheSpec
from pandaharvester.harvestercore.command_spec import CommandSpec
from pandaharvester.harvestercore.db_proxy_mods.base_module import (
    BaseModule,
    _logger,
    cacheTableName,
    commandTableName,
    diagTableName,
    eventTableName,
    fileTableName,
    jobTableName,
    jobWorkerTableName,
    pandaQueueTableName,
    processLockTableName,
    queueConfigDumpTableName,
    seqNumberTableName,
    serviceMetricsTableName,
    workerCheckTableName,
    workerTargetCacheTableName,
    workTableName,
)
from pandaharvester.harvestercore.diag_spec import DiagSpec
from pandaharvester.harvestercore.event_spec import EventSpec
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore.job_worker_relation_spec import JobWorkerRelationSpec
from pandaharvester.harvestercore.panda_queue_spec import PandaQueueSpec
from pandaharvester.harvestercore.process_lock_spec import ProcessLockSpec
from pandaharvester.harvestercore.queue_config_dump_spec import QueueConfigDumpSpec
from pandaharvester.harvestercore.seq_number_spec import SeqNumberSpec
from pandaharvester.harvestercore.service_metrics_spec import ServiceMetricSpec
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.worker_check_spec import WorkerCheckSpec
from pandaharvester.harvestercore.worker_target_cache_spec import WorkerTargetCacheSpec


# DB proxy module for table creation and schema checks
class SchemaModule(BaseModule):
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
                attr_type = re.sub(r"varchar\(\d+\)", "text", attr_type)
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
            # roll back
            self.rollback()
            core_utils.dump_error_message(tmp_log)

    # make table
    def make_table(self, cls, table_name):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="make_table")
        tmpLog.debug(f"table={table_name}")
        try:
            # check if table already exists
            if harvester_config.db.engine == "mariadb":
                varMap = {":name": table_name, ":schema": harvester_config.db.schema}
                sqlC = "SELECT * FROM information_schema.tables WHERE table_schema=:schema AND table_name=:name "
            else:
                varMap = {":name": table_name, ":type": "table"}
                sqlC = "SELECT name FROM sqlite_master WHERE type=:type AND tbl_name=:name "
            self.execute(sqlC, varMap)
            resC = self.cur.fetchone()
            # columns to add; all for a new table, otherwise only missing ones
            missingAttrs = None if resC is None else set(self.check_table(cls, table_name, True))
            columns = []
            indexes = []
            uniques = set()
            for attr in cls.attributesWithTypes:
                # split to name and type
                attrName, attrType = attr.split(":")
                if missingAttrs is not None and attrName not in missingAttrs:
                    continue
                # check if index is needed
                isIndex, isUnique = self.need_index(attr)
                if isIndex:
                    indexes.append(attrName)
                    if isUnique:
                        uniques.add(attrName)
                columns.append((attr, attrName, self.type_conversion(attrType)))
            if missingAttrs is None:
                # make table
                columnsStr = ",".join(f"{attrName} {attrType}" for _, attrName, attrType in columns)
                self.execute(f"CREATE TABLE {table_name}({columnsStr})")
                # commit
                self.commit()
                tmpLog.debug(f"made {table_name}")
            else:
                for attr, attrName, attrType in columns:
                    # add column
                    try:
                        self.execute(f"ALTER TABLE {table_name} ADD COLUMN {attrName} {attrType}")
                        # commit
                        self.commit()
                        tmpLog.debug(f"added {attr} to {table_name}")
                    except Exception:
                        self.rollback()
                        core_utils.dump_error_message(tmpLog)
                    # if we just added the jobType, old entries need to be initialized
                    if attrName == "jobType" and table_name in (pandaQueueTableName, workTableName):
                        self.initialize_jobType(table_name)
            # make indexes
            for index in indexes:
                indexName = f"idx_{index}_{table_name}"
                sqlI = "CREATE UNIQUE INDEX " if index in uniques else "CREATE INDEX "
                sqlI += f"{indexName} ON {table_name}({index}) "
                try:
                    self.execute(sqlI)
                    # commit
                    self.commit()
                    tmpLog.debug(f"added {indexName}")
                except Exception:
                    self.rollback()
                    core_utils.dump_error_message(tmpLog)
        except Exception:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(tmpLog)
        return self.check_table(cls, table_name)

    # make tables
    def make_tables(self, queue_config_mapper, communicator_pool):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name="make_tables")
        tmpLog.debug("start")
        outStrs = []
        for cls, table_name in (
            (CommandSpec, commandTableName),
            (JobSpec, jobTableName),
            (WorkSpec, workTableName),
            (FileSpec, fileTableName),
            (EventSpec, eventTableName),
            (CacheSpec, cacheTableName),
            (SeqNumberSpec, seqNumberTableName),
            (PandaQueueSpec, pandaQueueTableName),
            (JobWorkerRelationSpec, jobWorkerTableName),
            (ProcessLockSpec, processLockTableName),
            (DiagSpec, diagTableName),
            (QueueConfigDumpSpec, queueConfigDumpTableName),
            (ServiceMetricSpec, serviceMetricsTableName),
            (WorkerCheckSpec, workerCheckTableName),
            (WorkerTargetCacheSpec, workerTargetCacheTableName),
        ):
            outStrs += self.make_table(cls, table_name)

        # dump error messages and exit
        if len(outStrs) > 0:
            errMsg = (
                "ERROR : Definitions of some database tables are incorrect. "
                "Please add missing columns, or drop those tables "
                "so that harvester automatically re-creates those tables."
                "\n"
            )
            print(errMsg)
            for outStr in outStrs:
                print(outStr)
            sys.exit(1)

        # sync workerID
        init_worker = 1
        if getattr(harvester_config.db, "syncMaxWorkerID", False):
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
            varMap = {":name": table_name, ":schema": harvester_config.db.schema}
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
