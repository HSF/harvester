"""
DB proxy module for the worker target cache, shared by monitor plugins across threads and processes

Portability notes for SQL in this module (MariaDB and SQLite):
- a named placeholder must not appear twice in one statement, since parameters are passed by position
- upsert is done as DELETE then INSERT in one transaction, which both engines support with the same syntax
"""

import datetime
import hashlib
import json

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_mods.base_module import (
    BaseModule,
    _bind_in,
    db_error_handler,
    make_logger,
    workerTargetCacheTableName,
)
from pandaharvester.harvestercore.worker_target_cache_spec import WorkerTargetCacheSpec

# max number of bind variables in one IN clause
_in_chunk_size = 1000


# split a list into chunks
def _chunks(values, size=_in_chunk_size):
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


# DB proxy module for the worker target cache
class WorkerTargetCacheModule(BaseModule):
    # refresh the cache of a target host with all its targets, writing only changed ones
    @db_error_handler(default=None)
    def refresh_worker_target_cache(self, target_host, target_list, refresh_time):
        """
        target_list is a list of dicts of targetID, workerID, statusTime (naive UTC datetime or None) and attributes (dict of JSON types).
        Targets of the host missing in target_list are deleted.
        Return dict of n_targets, n_written, n_deleted.
        """
        tmpLog = make_logger(f"host={target_host}", method_name="refresh_worker_target_cache")
        tmpLog.debug(f"start with {len(target_list)} targets")
        # get digests in DB
        varMap = {":targetHost": target_host}
        sqlS = f"SELECT targetID,digest FROM {workerTargetCacheTableName} WHERE targetHost=:targetHost "
        self.execute(sqlS, varMap)
        old_digest_map = {targetID: digest for targetID, digest in self.cur.fetchall() if targetID is not None}
        # targets to write
        new_ids = set()
        specs_to_write = []
        keys_to_delete = []
        for item in target_list:
            target_id = item["targetID"]
            new_ids.add(target_id)
            digest = hashlib.sha1(json.dumps(item["attributes"], sort_keys=True).encode()).hexdigest()
            old_digest = old_digest_map.get(target_id)
            if old_digest == digest:
                continue
            if target_id in old_digest_map:
                keys_to_delete.append(WorkerTargetCacheSpec.make_key(target_host, target_id))
            spec = WorkerTargetCacheSpec()
            spec.targetKey = WorkerTargetCacheSpec.make_key(target_host, target_id)
            spec.targetHost = target_host
            spec.targetID = target_id
            spec.workerID = item["workerID"]
            spec.statusTime = item["statusTime"]
            spec.targetAttributes = item["attributes"]
            spec.digest = digest
            spec.updateTime = refresh_time
            specs_to_write.append(spec)
        # targets gone
        gone_keys = [WorkerTargetCacheSpec.make_key(target_host, target_id) for target_id in old_digest_map if target_id not in new_ids]
        keys_to_delete += gone_keys
        # refresh record
        refresh_spec = WorkerTargetCacheSpec()
        refresh_spec.targetKey = WorkerTargetCacheSpec.make_refresh_key(target_host)
        refresh_spec.targetHost = target_host
        refresh_spec.updateTime = refresh_time
        keys_to_delete.append(refresh_spec.targetKey)
        specs_to_write.append(refresh_spec)
        # delete changed and gone targets
        for chunk in _chunks(keys_to_delete):
            varMap = {}
            sqlD = f"DELETE FROM {workerTargetCacheTableName} WHERE targetKey IN ({_bind_in('k', chunk, varMap)}) "
            self.execute(sqlD, varMap)
        # insert new and changed targets; ignore rows written meanwhile by another refresh
        sqlI = f"INSERT IGNORE INTO {workerTargetCacheTableName} ({WorkerTargetCacheSpec.column_names()}) "
        sqlI += WorkerTargetCacheSpec.bind_values_expression()
        for chunk in _chunks(specs_to_write):
            self.executemany(sqlI, [spec.values_list() for spec in chunk])
        self.commit()
        retVal = {"n_targets": len(new_ids), "n_written": len(specs_to_write) - 1, "n_deleted": len(gone_keys)}
        tmpLog.debug(f"done with {retVal}")
        return retVal

    # get cached targets of a target host
    @db_error_handler(default=(None, {}))
    def get_worker_target_cache(self, target_host, target_ids=None):
        """
        Get targets in target_ids, or all targets if None.
        Return (refresh_time, {targetID: attributes dict}); refresh_time is the time of the last refresh, or None if never refreshed.
        """
        tmpLog = make_logger(f"host={target_host}", method_name="get_worker_target_cache")
        refresh_key = WorkerTargetCacheSpec.make_refresh_key(target_host)
        rows = []
        if target_ids is None:
            varMap = {":targetHost": target_host}
            sqlS = f"SELECT targetKey,targetID,targetAttributes,updateTime FROM {workerTargetCacheTableName} WHERE targetHost=:targetHost "
            self.execute(sqlS, varMap)
            rows += self.cur.fetchall()
        else:
            keys = [refresh_key] + [WorkerTargetCacheSpec.make_key(target_host, target_id) for target_id in target_ids]
            for chunk in _chunks(keys):
                varMap = {}
                sqlS = f"SELECT targetKey,targetID,targetAttributes,updateTime FROM {workerTargetCacheTableName} "
                sqlS += f"WHERE targetKey IN ({_bind_in('k', chunk, varMap)}) "
                self.execute(sqlS, varMap)
                rows += self.cur.fetchall()
        self.commit()
        refresh_time = None
        target_map = {}
        for targetKey, targetID, targetAttributes, updateTime in rows:
            if targetKey == refresh_key:
                refresh_time = updateTime
                continue
            try:
                target_map[targetID] = json.loads(targetAttributes)
            except Exception:
                tmpLog.warning(f"skipped targetID={targetID} with invalid attributes")
        tmpLog.debug(f"got {len(target_map)} targets refreshed at {refresh_time}")
        return refresh_time, target_map

    # delete the cache of target hosts not refreshed for longer than max_age seconds
    @db_error_handler(default=None)
    def clean_worker_target_cache(self, max_age):
        """
        Return the number of deleted rows.
        """
        tmpLog = make_logger(method_name="clean_worker_target_cache")
        tmpLog.debug("start")
        timeLimit = core_utils.naive_utcnow() - datetime.timedelta(seconds=max_age)
        # target hosts whose last refresh is too old
        varMap = {":timeLimit": timeLimit}
        sqlS = f"SELECT targetHost FROM {workerTargetCacheTableName} WHERE targetID IS NULL AND updateTime<:timeLimit "
        self.execute(sqlS, varMap)
        hosts = [targetHost for (targetHost,) in self.cur.fetchall()]
        n_deleted = 0
        for chunk in _chunks(hosts):
            varMap = {}
            sqlD = f"DELETE FROM {workerTargetCacheTableName} WHERE targetHost IN ({_bind_in('h', chunk, varMap)}) "
            self.execute(sqlD, varMap)
            n_deleted += self.cur.rowcount
        self.commit()
        tmpLog.debug(f"deleted {n_deleted} rows of {len(hosts)} target hosts")
        return n_deleted
