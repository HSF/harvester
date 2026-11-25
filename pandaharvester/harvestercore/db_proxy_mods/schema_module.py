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
                        if (table_name == pandaQueueTableName and attrName == "jobType") or (table_name == workTableName and attrName == "jobType"):
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
