"""
DB proxy module for sequence numbers, cache, commands, process locks and dialog messages

"""

import datetime
import time

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.cache_spec import CacheSpec
from pandaharvester.harvestercore.command_spec import CommandSpec
from pandaharvester.harvestercore.db_proxy_mods.base_module import (
    BaseModule,
    cacheTableName,
    commandTableName,
    db_error_handler,
    diagTableName,
    make_logger,
    processLockTableName,
    seqNumberTableName,
)
from pandaharvester.harvestercore.diag_spec import DiagSpec
from pandaharvester.harvestercore.process_lock_spec import ProcessLockSpec
from pandaharvester.harvestercore.seq_number_spec import SeqNumberSpec

# seconds a cached info in the process is used before checking lastUpdate in DB, to see updates by other processes
_local_cache_check_interval = 60


# cached info in the process
class _LocalCacheItem:
    __slots__ = ("lastUpdate", "data", "checkTime")

    def __init__(self, last_update, data):
        self.lastUpdate = last_update
        self.data = data
        self.checkTime = time.monotonic()


# DB proxy module for sequence numbers, cache, commands, process locks and dialog messages
class MiscModule(BaseModule):
    # add a seq number
    @db_error_handler(default=False)
    def add_seq_number(self, number_name, init_value):
        # check if already there
        sqlC = f"SELECT curVal FROM {seqNumberTableName} WHERE numberName=:numberName "
        self.execute(sqlC, {":numberName": number_name})
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
            self.execute(sqlI, seqNumberSpec.values_list())
        # commit
        self.commit()
        return True

    # get next value for a seq number
    @db_error_handler(default=None)
    def get_next_seq_number(self, number_name):
        # get logger
        tmpLog = make_logger(f"name={number_name}", method_name="get_next_seq_number")
        # increment
        varMap = {":numberName": number_name}
        sqlU = f"UPDATE {seqNumberTableName} SET curVal=curVal+1 WHERE numberName=:numberName "
        self.execute(sqlU, varMap)
        # get
        sqlG = f"SELECT curVal FROM {seqNumberTableName} WHERE numberName=:numberName "
        self.execute(sqlG, varMap)
        (retVal,) = self.cur.fetchone()
        # commit
        self.commit()
        tmpLog.debug(f"got {retVal}")
        return retVal

    # get last update time for a cached info
    @db_error_handler(default=None)
    def get_cache_last_update_time(self, main_key, sub_key):
        # get logger
        tmpLog = make_logger(f"mainKey={main_key} subKey={sub_key}", method_name="get_cache_last_update_time")
        # get
        varMap = {":mainKey": main_key}
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

    # get last update times of cached infos
    @db_error_handler(default=None)
    def get_cache_last_update_times(self, main_keys):
        """
        Return {mainKey: lastUpdate} for main_keys in the cache, or None if failed. Missing keys are omitted
        """
        # get logger
        tmpLog = make_logger(method_name="get_cache_last_update_times")
        varMap = {f":mainKey{idx}": main_key for idx, main_key in enumerate(main_keys)}
        sqlS = f"SELECT mainKey,lastUpdate FROM {cacheTableName} WHERE mainKey IN ({','.join(varMap)}) "
        self.execute(sqlS, varMap)
        retVal = {mainKey: lastUpdate for mainKey, lastUpdate in self.cur.fetchall()}
        # commit
        self.commit()
        tmpLog.debug(f"got {retVal}")
        return retVal

    # refresh a cached info
    @db_error_handler(default=False)
    def refresh_cache(self, main_key, sub_key, new_info):
        # get logger
        tmpLog = make_logger(f"mainKey={main_key} subKey={sub_key}", method_name="refresh_cache")
        # make spec
        cacheSpec = CacheSpec()
        cacheSpec.lastUpdate = core_utils.naive_utcnow()
        cacheSpec.data = new_info
        # check if already there
        varMap = {":mainKey": main_key}
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
        try:
            globalDict[cacheKey] = _LocalCacheItem(cacheSpec.lastUpdate, cacheSpec.data)
        finally:
            globalDict.release()
        tmpLog.debug("refreshed")
        return True

    # get a cached info
    def get_cache(self, main_key, sub_key=None, from_local_cache=True):
        """
        Return CacheSpec, or None if missing.
        With from_local_cache, the copy in the process is used, and checked against lastUpdate in DB once older than
        _local_cache_check_interval seconds, so that updates by other processes are seen
        """
        useDB = False
        # get logger
        tmpLog = make_logger(f"mainKey={main_key} subKey={sub_key}", method_name="get_cache")
        try:
            tmpLog.debug("start")
            # where clause
            sqlW = "WHERE mainKey=:mainKey "
            varMap = {":mainKey": main_key}
            if sub_key is not None:
                sqlW += "AND subKey=:subKey "
                varMap[":subKey"] = sub_key
            # get from global dict
            cacheKey = f"cache|{main_key}|{sub_key}"
            globalDict = core_utils.get_global_dict()
            # lock dict
            globalDict.acquire()
            try:
                localItem = None
                if from_local_cache and cacheKey in globalDict:
                    localItem = globalDict[cacheKey]
                    if time.monotonic() - localItem.checkTime >= _local_cache_check_interval:
                        # check if updated in DB
                        useDB = True
                        sqlL = f"SELECT lastUpdate FROM {cacheTableName} " + sqlW
                        self.execute(sqlL, varMap)
                        resL = self.cur.fetchone()
                        # commit
                        self.commit()
                        if resL is not None and resL[0] == localItem.lastUpdate:
                            localItem.checkTime = time.monotonic()
                        else:
                            tmpLog.debug("local cache outdated")
                            localItem = None
                if localItem is not None:
                    # make spec
                    cacheSpec = CacheSpec()
                    cacheSpec.lastUpdate = localItem.lastUpdate
                    cacheSpec.data = localItem.data
                else:
                    # read from database
                    useDB = True
                    sql = f"SELECT {CacheSpec.column_names()} FROM {cacheTableName} " + sqlW
                    self.execute(sql, varMap)
                    resJ = self.cur.fetchall()
                    # commit
                    self.commit()
                    if not resJ:
                        return None
                    # make spec
                    cacheSpec = CacheSpec()
                    cacheSpec.pack(resJ[0])
                    # put into global dict
                    globalDict[cacheKey] = _LocalCacheItem(cacheSpec.lastUpdate, cacheSpec.data)
            finally:
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
            core_utils.dump_error_message(tmpLog)
            # return
            return None

    # store commands
    @db_error_handler(default=False)
    def store_commands(self, command_specs):
        # get logger
        tmpLog = make_logger(method_name="store_commands")
        tmpLog.debug(f"{len(command_specs)} commands")
        if not command_specs:
            return True
        # sql to insert a command
        sql = f"INSERT INTO {commandTableName} ({CommandSpec.column_names()}) "
        sql += CommandSpec.bind_values_expression()
        # insert
        self.executemany(sql, [command_spec.values_list() for command_spec in command_specs])
        # commit
        self.commit()
        # return
        return True

    # get commands for a receiver
    @db_error_handler(default=[])
    def get_commands_for_receiver(self, receiver, command_pattern=None):
        # get logger
        tmpLog = make_logger(method_name="get_commands_for_receiver")
        tmpLog.debug("start")
        # sql to get commands
        varMap = {":receiver": receiver, ":processed": 0}
        sqlG = f"SELECT {CommandSpec.column_names()} FROM {commandTableName} WHERE receiver=:receiver AND processed=:processed "
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
            commandSpecList.append(commandSpec)
        # lock
        if commandSpecList:
            self.executemany(sqlL, [{":command_id": commandSpec.command_id, ":processed": 1} for commandSpec in commandSpecList])
        # commit
        self.commit()
        tmpLog.debug(f"got {len(commandSpecList)} commands")
        return commandSpecList

    # get command ids that have been processed and need to be acknowledged to panda server
    @db_error_handler(default=[])
    def get_commands_ack(self):
        # get logger
        tmpLog = make_logger(method_name="get_commands_ack")
        tmpLog.debug("start")
        # sql to get commands that have been processed and need acknowledgement
        sql = f"SELECT command_id FROM {commandTableName} WHERE ack_requested=1 AND processed=1"
        self.execute(sql)
        command_ids = [row[0] for row in self.cur.fetchall()]
        # commit
        self.commit()
        tmpLog.debug(f"command_ids {command_ids}")
        return command_ids

    # get the smallest command_id currently stored; None if the table is empty
    @db_error_handler(default=None)
    def get_min_command_id(self):
        # get logger
        tmpLog = make_logger(method_name="get_min_command_id")
        tmpLog.debug("start")
        sql = f"SELECT MIN(command_id) FROM {commandTableName} "
        self.execute(sql)
        min_command_id = self.cur.fetchone()[0]
        # commit
        self.commit()
        tmpLog.debug(f"min_command_id={min_command_id}")
        return min_command_id

    @db_error_handler(default=False)
    def clean_commands_by_id(self, commands_ids):
        """
        Deletes the commands specified in a list of IDs
        """
        # sql to delete a specific command
        sql = f"DELETE FROM {commandTableName} WHERE command_id=:command_id"
        if commands_ids:
            self.executemany(sql, [{":command_id": command_id} for command_id in commands_ids])
        self.commit()
        return True

    @db_error_handler(default=False)
    def clean_processed_commands(self):
        """
        Deletes the commands that have been processed and do not need acknowledgement
        """
        # sql to delete all processed commands that do not need an ACK
        sql = f"DELETE FROM {commandTableName} WHERE (ack_requested=0 AND processed=1)"
        self.execute(sql)
        self.commit()
        return True

    # delete old process locks
    @db_error_handler(default=False)
    def clean_process_locks(self):
        # get logger
        tmpLog = make_logger(method_name="clean_process_locks")
        tmpLog.debug("start")
        # delete locks
        sqlW = f"DELETE FROM {processLockTableName} "
        self.execute(sqlW)
        # commit
        self.commit()
        tmpLog.debug("done")
        return True

    # get a process lock
    @db_error_handler(default=False)
    def get_process_lock(self, process_name, locked_by, lock_interval):
        # get logger
        tmpLog = make_logger(f"proc={process_name} by={locked_by}", method_name="get_process_lock")
        tmpLog.debug("start")
        # delete old lock
        sqlD = f"DELETE FROM {processLockTableName} "
        sqlD += "WHERE lockTime<:timeLimit "
        self.execute(sqlD, {":timeLimit": core_utils.naive_utcnow() - datetime.timedelta(hours=6)})
        # commit
        self.commit()
        # check lock
        sqlC = f"SELECT lockTime FROM {processLockTableName} "
        sqlC += "WHERE processName=:processName "
        self.execute(sqlC, {":processName": process_name})
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
            self.execute(sqlI, processLockSpec.values_list())
            retVal = True
        else:
            (oldLockTime,) = resC
            timeLimit = timeNow - datetime.timedelta(seconds=lock_interval)
            if oldLockTime <= timeLimit:
                # update lock if old
                sqlU = f"UPDATE {processLockTableName} SET lockedBy=:lockedBy,lockTime=:timeNow "
                sqlU += "WHERE processName=:processName AND lockTime<=:timeLimit "
                varMap = {":processName": process_name, ":lockedBy": locked_by, ":timeLimit": timeLimit, ":timeNow": timeNow}
                self.execute(sqlU, varMap)
                if self.cur.rowcount > 0:
                    retVal = True
        # commit
        self.commit()
        tmpLog.debug(f"done with {retVal}")
        return retVal

    # release a process lock
    @db_error_handler(default=False)
    def release_process_lock(self, process_name, locked_by):
        # get logger
        tmpLog = make_logger(f"proc={process_name} by={locked_by}", method_name="release_process_lock")
        tmpLog.debug("start")
        # delete lock
        sqlC = f"DELETE FROM {processLockTableName} "
        sqlC += "WHERE processName=:processName AND lockedBy=:lockedBy "
        self.execute(sqlC, {":processName": process_name, ":lockedBy": locked_by})
        # commit
        self.commit()
        tmpLog.debug("done")
        return True

    # add dialog message
    @db_error_handler(default=False)
    def add_dialog_message(self, message, level, module_name, identifier=None):
        # get logger
        tmpLog = make_logger(method_name="add_dialog_message")
        tmpLog.debug("start")
        # delete old messages
        sqlS = f"SELECT diagID FROM {diagTableName} "
        sqlS += "WHERE creationTime<:timeLimit "
        self.execute(sqlS, {":timeLimit": core_utils.naive_utcnow() - datetime.timedelta(minutes=60)})
        resS = self.cur.fetchall()
        sqlD = f"DELETE FROM {diagTableName} "
        sqlD += "WHERE diagID=:diagID "
        for (diagID,) in resS:
            self.execute(sqlD, {":diagID": diagID})
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
        self.execute(sqlI, diagSpec.values_list())
        # commit
        self.commit()
        tmpLog.debug("done")
        return True

    # get dialog messages to send
    @db_error_handler(default=[])
    def get_dialog_messages_to_send(self, n_messages, lock_interval):
        # get logger
        tmpLog = make_logger(method_name="get_dialog_messages_to_send")
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
        self.execute(sqlD, {":timeLimit": timeLimit})
        resD = self.cur.fetchall()
        diagList = []
        for (diagID,) in resD:
            # lock
            varMap = {":diagID": diagID, ":timeLimit": timeLimit, ":timeNow": core_utils.naive_utcnow()}
            self.execute(sqlL, varMap)
            if self.cur.rowcount == 1:
                # get
                self.execute(sqlM, {":diagID": diagID})
                resM = self.cur.fetchone()
                # make spec
                diagSpec = DiagSpec()
                diagSpec.pack(resM)
                diagList.append(diagSpec)
            # commit
            self.commit()
        tmpLog.debug(f"got {len(diagList)} messages")
        return diagList

    # delete dialog messages
    @db_error_handler(default=False)
    def delete_dialog_messages(self, ids):
        # get logger
        tmpLog = make_logger(method_name="delete_dialog_messages")
        tmpLog.debug("start")
        # sql to delete message
        sqlM = f"DELETE FROM {diagTableName} "
        sqlM += "WHERE diagID=:diagID "
        for diagID in ids:
            # delete
            self.execute(sqlM, {":diagID": diagID})
            # commit
            self.commit()
        tmpLog.debug("done")
        return True
