"""
DB proxy module for the worker check schedule of the monitor

Portability notes for SQL in this module (MariaDB and SQLite):
- a named placeholder must not appear twice in one statement, since parameters are passed by position
- in UPDATE, MySQL evaluates SET assignments left to right with already updated values while SQLite uses old ones,
  so an assignment reading a column must come before the assignment of that column
"""

import datetime
import random

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_mods.base_module import (
    BaseModule,
    _bind_in,
    db_error_handler,
    jobWorkerTableName,
    make_logger,
    workerCheckTableName,
    workTableName,
)
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.worker_check_spec import WorkerCheckSpec

# worker statuses the monitor checks
_active_statuses = (WorkSpec.ST_submitted, WorkSpec.ST_running, WorkSpec.ST_idle)

# worker statuses that keep a schedule row
_non_final_statuses = _active_statuses + (WorkSpec.ST_ready, WorkSpec.ST_pending)


# DB proxy module for the worker check schedule of the monitor
class WorkerCheckModule(BaseModule):
    # claim due workers to check with a lease, and get their worker records
    @db_error_handler(default=(None, {}, {}))
    def claim_workers_to_check(self, locked_by, max_workers, lock_interval):
        """
        Claim up to max_workers due workers, ordered by priority then by next check time.
        Return (lock_token, workspecs_per_queue, check_specs):
        - lock_token: identifies this claim; pass it to release_checked_workers
        - workspecs_per_queue: {queueName: {configID: [[workSpec], ...]}} of claimed workers in work_table, of any status
        - check_specs: {workerID: WorkerCheckSpec} of claimed workers, as when claimed
        Schedule rows of workers missing in work_table or of multi-worker mapping are deleted.
        """
        tmpLog = make_logger(f"by={locked_by}", method_name="claim_workers_to_check")
        timeNow = core_utils.naive_utcnow()
        lock_token = f"{locked_by}-{random.getrandbits(32):08x}"
        # get candidates
        sqlC = f"SELECT workerID FROM {workerCheckTableName} "
        sqlC += "WHERE nextCheckTime<=:timeNow AND (lockedBy IS NULL OR lockedUntil<:lockTimeLimit) "
        sqlC += f"ORDER BY priority DESC,nextCheckTime LIMIT {int(max_workers)} "
        varMap = {":timeNow": timeNow, ":lockTimeLimit": timeNow}
        self.execute(sqlC, varMap)
        candidates = [workerID for (workerID,) in self.cur.fetchall()]
        if not candidates:
            self.commit()
            return lock_token, {}, {}
        # lease candidates unless leased by others in the meantime, or already checked and released by others with a new schedule
        varMap = {
            ":lockedBy": lock_token,
            ":lockedUntil": timeNow + datetime.timedelta(seconds=lock_interval),
            ":lockTimeLimit": timeNow,
            ":timeNow": timeNow,
        }
        sqlL = f"UPDATE {workerCheckTableName} SET lockedBy=:lockedBy,lockedUntil=:lockedUntil "
        sqlL += f"WHERE workerID IN ({_bind_in('wID', candidates, varMap)}) "
        sqlL += "AND nextCheckTime<=:timeNow AND (lockedBy IS NULL OR lockedUntil<:lockTimeLimit) "
        self.execute(sqlL, varMap)
        self.commit()
        # get schedule of workers leased by this claim
        varMap = {":lockedBy": lock_token}
        sqlS = f"SELECT {WorkerCheckSpec.column_names()} FROM {workerCheckTableName} "
        sqlS += f"WHERE workerID IN ({_bind_in('wID', candidates, varMap)}) AND lockedBy=:lockedBy "
        self.execute(sqlS, varMap)
        check_specs = {}
        for res in self.cur.fetchall():
            check_spec = WorkerCheckSpec()
            check_spec.pack(res)
            check_specs[check_spec.workerID] = check_spec
        if not check_specs:
            self.commit()
            tmpLog.debug(f"lost all {len(candidates)} candidates to other claims")
            return lock_token, {}, {}
        claimed_ids = list(check_specs)
        # get workers
        varMap = {}
        sqlW = f"SELECT {WorkSpec.column_names()} FROM {workTableName} "
        sqlW += f"WHERE workerID IN ({_bind_in('wID', claimed_ids, varMap)}) "
        self.execute(sqlW, varMap)
        workspec_map = {}
        for res in self.cur.fetchall():
            workSpec = WorkSpec()
            workSpec.pack(res)
            workSpec.pandaid_list = []
            workspec_map[workSpec.workerID] = workSpec
        # get associated PandaIDs
        varMap = {}
        sqlP = f"SELECT workerID,PandaID FROM {jobWorkerTableName} "
        sqlP += f"WHERE workerID IN ({_bind_in('wID', claimed_ids, varMap)}) "
        self.execute(sqlP, varMap)
        for workerID, pandaID in self.cur.fetchall():
            if workerID in workspec_map:
                workspec_map[workerID].pandaid_list.append(pandaID)
        # group workers
        workspecs_per_queue = {}
        to_delete = []
        for workerID in claimed_ids:
            workSpec = workspec_map.get(workerID)
            if workSpec is None or workSpec.mapType == WorkSpec.MT_MultiWorkers:
                to_delete.append(workerID)
                del check_specs[workerID]
                continue
            if len(workSpec.pandaid_list) > 0:
                workSpec.nJobs = len(workSpec.pandaid_list)
            # ignore configID
            configID = workSpec.configID if core_utils.dynamic_plugin_change() else None
            workspecs_per_queue.setdefault(workSpec.computingSite, {}).setdefault(configID, []).append([workSpec])
        # delete schedule of workers not to check
        if to_delete:
            varMap = {}
            sqlD = f"DELETE FROM {workerCheckTableName} WHERE workerID IN ({_bind_in('wID', to_delete, varMap)}) "
            self.execute(sqlD, varMap)
        self.commit()
        tmpLog.debug(f"claimed {len(check_specs)} workers out of {len(candidates)} candidates; deleted {len(to_delete)} schedules")
        return lock_token, workspecs_per_queue, check_specs

    # release workers claimed by claim_workers_to_check, with their next schedule
    @db_error_handler(default=False)
    def release_checked_workers(self, lock_token, check_start_time, release_list):
        """
        Release claimed workers. Each item of release_list is a dict:
        - workerID
        - delete: True to delete the schedule (worker in final status); the other keys are then ignored
        - nextCheckTime, priority, priorityTime, lastTrigger: new schedule
        - checkedOK: True if checked successfully; sets lastCheckTime, clears firstFailTime and sets work_table.checkTime
        - firstFailTime: start of consecutive failures when checkedOK is False
        - lastEventTime: lastEventTime of the schedule as claimed
        A worker that got a new event while leased is kept due now with at least event priority.
        Workers are updated with one statement per group of identical values.
        """
        tmpLog = make_logger(f"by={lock_token}", method_name="release_checked_workers")
        tmpLog.debug(f"start for {len(release_list)} workers")
        timeNow = core_utils.naive_utcnow()
        to_delete = [item["workerID"] for item in release_list if item.get("delete")]
        to_update = [item for item in release_list if not item.get("delete")]
        # get events arrived while leased, with row locks to block new events until commit
        current_event_time_map = {}
        if to_update:
            varMap = {}
            sqlE = f"SELECT workerID,lastEventTime FROM {workerCheckTableName} "
            sqlE += f"WHERE workerID IN ({_bind_in('wID', [item['workerID'] for item in to_update], varMap)}) FOR UPDATE "
            self.execute(sqlE, varMap)
            current_event_time_map = {workerID: lastEventTime for workerID, lastEventTime in self.cur.fetchall()}
        # group workers with identical values
        groups = {}
        n_event = 0
        for item in to_update:
            workerID = item["workerID"]
            new_event_time = current_event_time_map.get(workerID)
            nextCheckTime = item["nextCheckTime"]
            priority = item["priority"]
            priorityTime = item["priorityTime"]
            lastTrigger = item["lastTrigger"]
            if new_event_time is not None and (item.get("lastEventTime") is None or new_event_time > item["lastEventTime"]):
                # new event while leased
                n_event += 1
                nextCheckTime = min(nextCheckTime, timeNow)
                if priority < WorkerCheckSpec.PRIO_EVENT:
                    priority = WorkerCheckSpec.PRIO_EVENT
                    priorityTime = timeNow
                lastTrigger = WorkerCheckSpec.TRIG_event
            if item["checkedOK"]:
                firstFailTime = None
            else:
                firstFailTime = item["firstFailTime"]
            key = (nextCheckTime, priority, priorityTime, lastTrigger, bool(item["checkedOK"]), firstFailTime)
            groups.setdefault(key, []).append(workerID)
        # update schedules
        n_updated = 0
        for (nextCheckTime, priority, priorityTime, lastTrigger, checkedOK, firstFailTime), worker_ids in groups.items():
            varMap = {
                ":nextCheckTime": nextCheckTime,
                ":priority": priority,
                ":priorityTime": priorityTime,
                ":lastTrigger": lastTrigger,
                ":firstFailTime": firstFailTime,
                ":lockedBy": lock_token,
            }
            sqlU = f"UPDATE {workerCheckTableName} SET nextCheckTime=:nextCheckTime,priority=:priority,priorityTime=:priorityTime,"
            sqlU += "lastTrigger=:lastTrigger,firstFailTime=:firstFailTime,lockedBy=NULL,lockedUntil=NULL "
            if checkedOK:
                sqlU += ",lastCheckTime=:lastCheckTime "
                varMap[":lastCheckTime"] = check_start_time
            sqlU += f"WHERE workerID IN ({_bind_in('wID', worker_ids, varMap)}) AND lockedBy=:lockedBy "
            self.execute(sqlU, varMap)
            n_updated += self.cur.rowcount
        # set check time of workers checked successfully
        checked_ids = [item["workerID"] for item in to_update if item["checkedOK"]]
        if checked_ids:
            varMap = {":checkTime": check_start_time}
            sqlT = f"UPDATE {workTableName} SET checkTime=:checkTime WHERE workerID IN ({_bind_in('wID', checked_ids, varMap)}) "
            self.execute(sqlT, varMap)
        # delete schedules of workers in final status
        if to_delete:
            varMap = {}
            sqlD = f"DELETE FROM {workerCheckTableName} WHERE workerID IN ({_bind_in('wID', to_delete, varMap)}) "
            self.execute(sqlD, varMap)
        self.commit()
        if n_updated < len(to_update):
            tmpLog.warning(f"{len(to_update) - n_updated} workers were no longer leased by this claim. Lease may be too short")
        tmpLog.debug(f"done; updated {n_updated} in {len(groups)} groups with {n_event} new events, deleted {len(to_delete)}")
        return True

    # make workers reported by event-based check due now
    @db_error_handler(default=0)
    def apply_worker_events(self, event_list):
        """
        event_list is a list of (workerID, eventTime), eventTime being a naive UTC datetime of the change on the resource.
        An event is applied only if newer than the last applied event of the worker, which ignores repeated reports.
        Return the number of events applied.
        """
        tmpLog = make_logger(method_name="apply_worker_events")
        tmpLog.debug(f"start for {len(event_list)} events")
        timeNow = core_utils.naive_utcnow()
        # assignments reading priority come before the assignment of priority
        sqlU = f"UPDATE {workerCheckTableName} SET "
        sqlU += "priorityTime=CASE WHEN priority<:prio1 THEN :timeNow1 ELSE priorityTime END,"
        sqlU += "nextCheckTime=CASE WHEN nextCheckTime>:timeNow2 THEN :timeNow3 ELSE nextCheckTime END,"
        sqlU += "priority=CASE WHEN priority<:prio2 THEN :prio3 ELSE priority END,"
        sqlU += "lastTrigger=:lastTrigger,lastEventTime=:eventTime1 "
        sqlU += "WHERE workerID=:workerID AND (lastEventTime IS NULL OR lastEventTime<:eventTime2) "
        varMaps = []
        for workerID, eventTime in event_list:
            varMaps.append(
                {
                    ":prio1": WorkerCheckSpec.PRIO_EVENT,
                    ":prio2": WorkerCheckSpec.PRIO_EVENT,
                    ":prio3": WorkerCheckSpec.PRIO_EVENT,
                    ":timeNow1": timeNow,
                    ":timeNow2": timeNow,
                    ":timeNow3": timeNow,
                    ":lastTrigger": WorkerCheckSpec.TRIG_event,
                    ":eventTime1": eventTime,
                    ":eventTime2": eventTime,
                    ":workerID": workerID,
                }
            )
        n_applied = 0
        if varMaps:
            self.executemany(sqlU, varMaps)
            n_applied = self.cur.rowcount
        self.commit()
        tmpLog.debug(f"done; applied {n_applied} events")
        return n_applied

    # make the schedule consistent with work_table, and count workers not checked for long
    @db_error_handler(default=None)
    def reconcile_worker_checks(self, check_interval, max_interval, stale_age, jitter_fraction=0.1):
        """
        - add schedules of non-final workers missing one, due around modificationTime + check_interval but at most check_interval from now
        - delete schedules of workers in final status, missing, or of multi-worker mapping
        - make due now the schedules further than max_interval in the future, or without next check time
        - count active workers whose checkTime is older than stale_age
        Return dict of n_inserted, n_deleted, n_clamped, n_stale.
        """
        tmpLog = make_logger(method_name="reconcile_worker_checks")
        tmpLog.debug("start")
        timeNow = core_utils.naive_utcnow()
        retVal = {}
        # get non-final workers without schedule
        varMap = {":mapType": WorkSpec.MT_MultiWorkers}
        sqlM = f"SELECT w.workerID,w.modificationTime FROM {workTableName} w "
        sqlM += f"WHERE w.status IN ({_bind_in('st', _non_final_statuses, varMap)}) "
        sqlM += "AND (w.mapType IS NULL OR w.mapType<>:mapType) "
        sqlM += f"AND NOT EXISTS (SELECT 1 FROM {workerCheckTableName} c WHERE c.workerID=w.workerID) "
        self.execute(sqlM, varMap)
        resM = self.cur.fetchall()
        # insert schedules
        sqlI = f"INSERT IGNORE INTO {workerCheckTableName} ({WorkerCheckSpec.column_names()}) "
        sqlI += WorkerCheckSpec.bind_values_expression()
        varMaps = []
        latest_time = timeNow + datetime.timedelta(seconds=check_interval)
        for workerID, modificationTime in resM:
            if modificationTime is None:
                nextCheckTime = timeNow
            else:
                nextCheckTime = modificationTime + datetime.timedelta(seconds=check_interval)
            # jitter before the cap, not to go beyond the cap and get clamped below
            nextCheckTime += datetime.timedelta(seconds=check_interval * random.uniform(-jitter_fraction, jitter_fraction))
            nextCheckTime = min(nextCheckTime, latest_time)
            check_spec = WorkerCheckSpec()
            check_spec.workerID = workerID
            check_spec.nextCheckTime = nextCheckTime
            check_spec.lastTrigger = WorkerCheckSpec.TRIG_reconcile
            varMaps.append(check_spec.values_map())
        retVal["n_inserted"] = 0
        if varMaps:
            self.executemany(sqlI, varMaps)
            retVal["n_inserted"] = self.cur.rowcount
        self.commit()
        # delete schedules of workers in final status or missing
        varMap = {":mapType": WorkSpec.MT_MultiWorkers}
        sqlD = f"DELETE FROM {workerCheckTableName} WHERE NOT EXISTS "
        sqlD += f"(SELECT 1 FROM {workTableName} w WHERE w.workerID={workerCheckTableName}.workerID "
        sqlD += f"AND w.status IN ({_bind_in('st', _non_final_statuses, varMap)}) "
        sqlD += "AND (w.mapType IS NULL OR w.mapType<>:mapType)) "
        self.execute(sqlD, varMap)
        retVal["n_deleted"] = self.cur.rowcount
        self.commit()
        # make due now the schedules too far in the future
        varMap = {
            ":timeNow": timeNow,
            ":lastTrigger": WorkerCheckSpec.TRIG_reconcile,
            ":timeLimit": timeNow + datetime.timedelta(seconds=max_interval),
        }
        sqlC = f"UPDATE {workerCheckTableName} SET nextCheckTime=:timeNow,lastTrigger=:lastTrigger "
        sqlC += "WHERE nextCheckTime>:timeLimit OR nextCheckTime IS NULL "
        self.execute(sqlC, varMap)
        retVal["n_clamped"] = self.cur.rowcount
        self.commit()
        # count active workers not checked for long
        varMap = {":mapType": WorkSpec.MT_MultiWorkers, ":staleLimit": timeNow - datetime.timedelta(seconds=stale_age)}
        sqlS = f"SELECT COUNT(*) FROM {workTableName} "
        sqlS += f"WHERE status IN ({_bind_in('st', _active_statuses, varMap)}) "
        sqlS += "AND (mapType IS NULL OR mapType<>:mapType) AND checkTime<:staleLimit "
        self.execute(sqlS, varMap)
        (retVal["n_stale"],) = self.cur.fetchone()
        self.commit()
        tmpLog.debug(f"done with {retVal}")
        return retVal

    # insert the schedule of a new worker, without commit; to be used in the transaction of worker registration
    def _insert_worker_check(self, worker_id, next_check_time, trigger):
        sqlI = f"INSERT IGNORE INTO {workerCheckTableName} ({WorkerCheckSpec.column_names()}) "
        sqlI += WorkerCheckSpec.bind_values_expression()
        check_spec = WorkerCheckSpec()
        check_spec.workerID = worker_id
        check_spec.nextCheckTime = next_check_time
        check_spec.lastTrigger = trigger
        self.execute(sqlI, check_spec.values_map())

    # get statistics of the worker check schedule
    @db_error_handler(default=None)
    def get_worker_check_stats(self):
        """
        Return dict of n_total, n_due (due and not leased), n_leased, lag (seconds since the oldest due and not leased
        worker became due; 0 if none), per_priority {priority: n}, per_trigger {lastTrigger: n}.
        """
        tmpLog = make_logger(method_name="get_worker_check_stats")
        tmpLog.debug("start")
        timeNow = core_utils.naive_utcnow()
        retVal = {"n_total": 0, "per_priority": {}, "per_trigger": {}}
        # counts per priority and trigger
        sqlG = f"SELECT priority,lastTrigger,COUNT(*) FROM {workerCheckTableName} GROUP BY priority,lastTrigger "
        self.execute(sqlG)
        for priority, lastTrigger, cnt in self.cur.fetchall():
            retVal["n_total"] += cnt
            retVal["per_priority"][priority] = retVal["per_priority"].get(priority, 0) + cnt
            retVal["per_trigger"][lastTrigger] = retVal["per_trigger"].get(lastTrigger, 0) + cnt
        # leased
        varMap = {":timeNow": timeNow}
        sqlL = f"SELECT COUNT(*) FROM {workerCheckTableName} WHERE lockedBy IS NOT NULL AND lockedUntil>=:timeNow "
        self.execute(sqlL, varMap)
        (retVal["n_leased"],) = self.cur.fetchone()
        # due and not leased
        varMap = {":timeNow": timeNow, ":lockTimeLimit": timeNow}
        sqlD = f"SELECT COUNT(*) FROM {workerCheckTableName} "
        sqlD += "WHERE nextCheckTime<=:timeNow AND (lockedBy IS NULL OR lockedUntil<:lockTimeLimit) "
        self.execute(sqlD, varMap)
        (retVal["n_due"],) = self.cur.fetchone()
        # lag; selecting the column rather than MIN() keeps its type with SQLite
        varMap = {":timeNow": timeNow, ":lockTimeLimit": timeNow}
        sqlO = f"SELECT nextCheckTime FROM {workerCheckTableName} "
        sqlO += "WHERE nextCheckTime<=:timeNow AND (lockedBy IS NULL OR lockedUntil<:lockTimeLimit) "
        sqlO += "ORDER BY nextCheckTime LIMIT 1 "
        self.execute(sqlO, varMap)
        resO = self.cur.fetchone()
        retVal["lag"] = 0 if resO is None else (timeNow - resO[0]).total_seconds()
        self.commit()
        tmpLog.debug(f"done with {retVal}")
        return retVal

    # make workers due now with at least the given priority, optionally only of a queue
    @db_error_handler(default=0)
    def force_worker_checks(self, queue_name=None, priority=WorkerCheckSpec.PRIO_ADMIN):
        """
        Return the number of schedules updated.
        """
        tmpLog = make_logger(f"queue={queue_name}", method_name="force_worker_checks")
        tmpLog.debug("start")
        timeNow = core_utils.naive_utcnow()
        varMap = {
            ":prio1": priority,
            ":prio2": priority,
            ":prio3": priority,
            ":timeNow1": timeNow,
            ":timeNow2": timeNow,
            ":lastTrigger": WorkerCheckSpec.TRIG_admin,
        }
        # assignments reading priority come before the assignment of priority
        sqlU = f"UPDATE {workerCheckTableName} SET "
        sqlU += "priorityTime=CASE WHEN priority<:prio1 THEN :timeNow1 ELSE priorityTime END,"
        sqlU += "priority=CASE WHEN priority<:prio2 THEN :prio3 ELSE priority END,"
        sqlU += "nextCheckTime=:timeNow2,lastTrigger=:lastTrigger "
        if queue_name is not None:
            sqlU += f"WHERE workerID IN (SELECT workerID FROM {workTableName} WHERE computingSite=:computingSite) "
            varMap[":computingSite"] = queue_name
        self.execute(sqlU, varMap)
        nRow = self.cur.rowcount
        self.commit()
        tmpLog.debug(f"done with {nRow}")
        return nRow
