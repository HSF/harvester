"""
database connection

"""

import re
import types
import inspect
import datetime
import threading

from command_spec import CommandSpec
from job_spec import JobSpec
from work_spec import WorkSpec
from file_spec import FileSpec
from event_spec import EventSpec
from cache_spec import CacheSpec
from seq_number_spec import SeqNumberSpec
from panda_queue_spec import PandaQueueSpec
from job_worker_relation_spec import JobWorkerRelationSpec

import core_utils
from pandaharvester.harvesterconfig import harvester_config

# logger
_logger = core_utils.setup_logger()

# table names
commandTableName = 'command_table'
jobTableName = 'job_table'
workTableName = 'work_table'
fileTableName = 'file_table'
cacheTableName = 'cache_table'
eventTableName = 'event_table'
seqNumberTableName = 'seq_table'
pandaQueueTableName = 'pq_table'
jobWorkerTableName = 'jw_table'

# connection lock
conLock = threading.Lock()


# connection class
class DBProxy:
    # constructor
    def __init__(self):
        import sqlite3
        self.con = sqlite3.connect(harvester_config.db.database_filename,
                                   detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                                   check_same_thread=False)
        self.con.row_factory = sqlite3.Row
        self.cur = self.con.cursor()
        self.lockDB = False
        self.verbLog = None

    # convert param dict to list
    def convert_params(self, sql, varmap):
        # lock database
        if re.search('^INSERT', sql, re.I) is not None or re.search('^UPDATE', sql, re.I) is not None \
                or re.search(' FOR UPDATE', sql, re.I) is not None:
            self.lockDB = True
        # remove FOR UPDATE
        sql = re.sub(' FOR UPDATE', ' ', sql, re.I)
        # no conversation unless dict
        if not isinstance(varmap, types.DictType):
            return sql, varmap
        paramList = []
        # extract placeholders
        items = re.findall(':[^ $,)]+', sql)
        for item in items:
            if item not in varmap:
                raise KeyError('{0} is missing in SQL parameters'.format(item))
            if item not in paramList:
                paramList.append(varmap[item])
        return sql, paramList

    # wrapper for execute
    def execute(self, sql, varmap=None):
        thrName = None
        if harvester_config.db.verbose:
            if self.verbLog is None:
                self.verbLog = core_utils.make_logger(_logger)
            thrName = threading.current_thread()
            if thrName is not None:
                thrName = thrName.ident
        if varmap is None:
            varmap = dict()
        # get lock
        if not self.lockDB:
            if harvester_config.db.verbose:
                self.verbLog.debug('thr={0} locking'.format(thrName))
            conLock.acquire()
            if harvester_config.db.verbose:
                self.verbLog.debug('thr={0} locked'.format(thrName))
        try:
            # verbose
            if harvester_config.db.verbose:
                self.verbLog.debug('thr={3} sql={0} var={1} exec={2}'.format(sql, str(varmap),
                                                                             inspect.stack()[1][3],
                                                                             thrName))
            # convert param dict
            newSQL, params = self.convert_params(sql, varmap)
            # execute
            retVal = self.cur.execute(newSQL, params)
        finally:
            # release lock
            if not self.lockDB:
                if harvester_config.db.verbose:
                    self.verbLog.debug('thr={0} release'.format(thrName))
                conLock.release()
        # return
        return retVal

    # wrapper for executemany
    def executemany(self, sql, varmap_list):
        thrName = None
        if harvester_config.db.verbose:
            if self.verbLog is None:
                self.verbLog = core_utils.make_logger(_logger)
            thrName = threading.current_thread()
            if thrName is not None:
                thrName = thrName.ident
        # get lock
        if not self.lockDB:
            if harvester_config.db.verbose:
                self.verbLog.debug('thr={0} locking'.format(thrName))
            conLock.acquire()
            if harvester_config.db.verbose:
                self.verbLog.debug('thr={0} locked'.format(thrName))
        try:
            # verbose
            if harvester_config.db.verbose:
                self.verbLog.debug('thr={3} sql={0} var={1} exec={2}'.format(sql, str(varmap_list),
                                                                             inspect.stack()[1][3],
                                                                             thrName))
            # convert param dict
            paramList = []
            newSQL = sql
            for varMap in varmap_list:
                if varMap is None:
                    varMap = dict()
                newSQL, params = self.convert_params(sql, varMap)
                paramList.append(params)
            # execute
            retVal = self.cur.executemany(newSQL, paramList)
        finally:
            # release lock
            if not self.lockDB:
                if harvester_config.db.verbose:
                    self.verbLog.debug('thr={0} release'.format(thrName))
                conLock.release()
        # return
        return retVal

    # commit
    def commit(self):
        try:
            self.con.commit()
        finally:
            if self.lockDB:
                conLock.release()
                self.lockDB = False

    # rollback
    def rollback(self):
        try:
            self.con.rollback()
        finally:
            if self.lockDB:
                conLock.release()
                self.lockDB = False

    # make table
    def make_table(self, cls, table_name):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger)
            tmpLog.debug('table={0}'.format(table_name))
            # check if table already exists
            varMap = dict()
            varMap[':type'] = 'table'
            varMap[':name'] = table_name
            sqlC = 'SELECT name FROM sqlite_master WHERE type=:type AND tbl_name=:name '
            self.execute(sqlC, varMap)
            resC = self.cur.fetchone()
            # not exists
            if resC is None:
                #  sql to make table
                sqlM = 'CREATE TABLE {0}('.format(table_name)
                # collect columns
                for attr in cls.attributesWithTypes:
                    # split to name and type
                    attrName, attrType = attr.split(':')
                    sqlM += '{0} {1},'.format(attrName, attrType)
                sqlM = sqlM[:-1]
                sqlM += ')'
                # make table
                self.execute(sqlM)
                # commit
                self.commit()
                tmpLog.debug('made {0}'.format(table_name))
            else:
                tmpLog.debug('reuse {0}'.format(table_name))
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)

    # make tables
    def make_tables(self, queue_config_mapper):
        self.make_table(CommandSpec, commandTableName)
        self.make_table(JobSpec, jobTableName)
        self.make_table(WorkSpec, workTableName)
        self.make_table(FileSpec, fileTableName)
        self.make_table(EventSpec, eventTableName)
        self.make_table(CacheSpec, cacheTableName)
        self.make_table(SeqNumberSpec, seqNumberTableName)
        self.make_table(PandaQueueSpec, pandaQueueTableName)
        self.make_table(JobWorkerRelationSpec, jobWorkerTableName)
        # fill PandaQueue table
        self.fill_panda_queue_table(harvester_config.qconf.queueList, queue_config_mapper)
        # add sequential numbers
        self.add_seq_number('SEQ_workerID', 1)

    # insert jobs
    def insert_jobs(self, jobspec_list):
        # get logger
        tmpLog = core_utils.make_logger(_logger)
        tmpLog.debug('{0} jobs'.format(len(jobspec_list)))
        try:
            # sql to insert a job
            sql = "INSERT INTO {0} ({1}) ".format(jobTableName, JobSpec.column_names())
            sql += JobSpec.bind_values_expression()
            # loop over all jobs
            varMaps = []
            for jobSpec in jobspec_list:
                varMap = jobSpec.values_list()
                varMaps.append(varMap)
            # insert
            self.executemany(sql, varMaps)
            # commit
            self.commit()
            # return
            return True
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(tmpLog)
            # return
            return False

    # get job
    def get_job(self, panda_id, with_file_status=None):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(panda_id))
            tmpLog.debug('start')
            # sql to get job
            sql = "SELECT {0} FROM {1} ".format(JobSpec.column_names(), jobTableName)
            sql += "WHERE PandaID=:pandaID "
            # get job
            varMap = dict()
            varMap[':pandaID'] = panda_id
            self.execute(sql, varMap)
            resJ = self.cur.fetchone()
            # commit
            self.commit()
            if resJ is None:
                return None
            # make job
            jobSpec = JobSpec()
            jobSpec.pack(resJ)
            # get files
            if with_file_status is not None:
                # sql to get files
                sqlF = "SELECT {0} FROM {1} ".format(FileSpec.column_names(), fileTableName)
                sqlF += "WHERE PandaID=:PandaID AND status=:status "
                varMap = dict()
                varMap[':PandaID'] = panda_id
                varMap[':status'] = with_file_status
                self.execute(sqlF, varMap)
                resFileList = self.cur.fetchall()
                for resFile in resFileList:
                    fileSpec = FileSpec()
                    fileSpec.pack(resFile)
                    jobSpec.add_out_file(fileSpec)
            tmpLog.debug('done')
            # return
            return jobSpec
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # get jobs (fetch entire jobTable)
    def get_jobs(self):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger)
            tmpLog.debug('start')
            # sql to get job
            sql = "SELECT {0} FROM {1} ".format(JobSpec.column_names(), jobTableName)
            sql += "WHERE PandaID IS NOT NULL"
            # get jobs
            varMap = None
            self.execute(sql, varMap)
            resJobs = self.cur.fetchall()
            if resJobs is None:
                return None
            jobSpecList=[]
            # make jobs list
            for resJ in resJobs:
                jobSpec = JobSpec()
                jobSpec.pack(resJ)
                jobSpecList.append(jobSpec)
            tmpLog.debug('done')
            # return
            return jobSpecList
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # update job
    def update_job(self, jobspec, criteria=None):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, 'PandaID={0} subStatus={1}'.format(jobspec.PandaID,
                                                                                        jobspec.subStatus))
            tmpLog.debug('start')
            if criteria is None:
                criteria = {}
            # sql to update job
            sql = "UPDATE {0} SET {1} ".format(jobTableName, jobspec.bind_update_changes_expression())
            sql += "WHERE PandaID=:PandaID "
            # update job
            varMap = jobspec.values_map(only_changed=True)
            for tmpKey, tmpVal in criteria.iteritems():
                mapKey = ':{0}_cr'.format(tmpKey)
                sql += "AND {0}={1} ".format(tmpKey, mapKey)
                varMap[mapKey] = tmpVal
            varMap[':PandaID'] = jobspec.PandaID
            self.execute(sql, varMap)
            nRow = self.cur.rowcount
            if nRow > 0:
                # update events
                for eventSpec in jobspec.events:
                    varMap = eventSpec.values_map(only_changed=True)
                    if varMap != {}:
                        sqlE = "UPDATE {0} SET {1} ".format(eventTableName, eventSpec.bind_update_changes_expression())
                        sqlE += "WHERE eventRangeID=:eventRangeID "
                        varMap[':eventRangeID'] = eventSpec.eventRangeID
                        self.execute(sqlE, varMap)
            # commit
            self.commit()
            tmpLog.debug('done with {0}'.format(nRow))
            # return
            return nRow
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # update worker
    def update_worker(self, workspec, criteria=None):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
            tmpLog.debug('start')
            if criteria is None:
                criteria = {}
            # sql to update job
            sql = "UPDATE {0} SET {1} ".format(workTableName, workspec.bind_update_changes_expression())
            sql += "WHERE workerID=:workerID "
            # update worker
            varMap = workspec.values_map(only_changed=True)
            for tmpKey, tmpVal in criteria.iteritems():
                mapKey = ':{0}_cr'.format(tmpKey)
                sql += "AND {0}={1} ".format(tmpKey, mapKey)
                varMap[mapKey] = tmpVal
            varMap[':workerID'] = workspec.workerID
            self.execute(sql, varMap)
            nRow = self.cur.rowcount
            # commit
            self.commit()
            tmpLog.debug('done with {0}'.format(nRow))
            # return
            return nRow
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # fill panda queue table
    def fill_panda_queue_table(self, panda_queue_list, queue_config_mapper):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger)
            tmpLog.debug('start')
            # get existing queues
            sqlE = "SELECT queueName FROM {0} ".format(pandaQueueTableName)
            varMap = dict()
            self.execute(sqlE, varMap)
            resE = self.cur.fetchall()
            for queueName, in resE:
                # delete if not listed in cfg
                if queueName not in panda_queue_list:
                    sqlD = "DELETE FROM {0} ".format(pandaQueueTableName)
                    sqlD += "WHERE queueName=:queueName "
                    varMap = dict()
                    varMap[':queueName'] = queueName
                    self.execute(sqlD, varMap)
            # loop over queues
            for queueName in panda_queue_list:
                queueConfig = queue_config_mapper.get_queue(queueName)
                if queueConfig is not None:
                    # check if already exist
                    sqlC = "SELECT 1 FROM {0} ".format(pandaQueueTableName)
                    sqlC += "WHERE queueName=:queueName "
                    varMap = dict()
                    varMap[':queueName'] = queueName
                    self.execute(sqlC, varMap)
                    resC = self.cur.fetchone()
                    if resC is not None:
                        continue
                    # insert queue
                    varMap = dict()
                    varMap[':queueName'] = queueName
                    sqlP = "INSERT INTO {0} (".format(pandaQueueTableName)
                    sqlS = "VALUES ("
                    for attrName in PandaQueueSpec.column_names().split(','):
                        if hasattr(queueConfig, attrName):
                            tmpKey = ':{0}'.format(attrName)
                            sqlP += '{0},'.format(attrName)
                            sqlS += '{0},'.format(tmpKey)
                            varMap[tmpKey] = getattr(queueConfig, attrName)
                    sqlP = sqlP[:-1]
                    sqlS = sqlS[:-1]
                    sqlP += ') '
                    sqlS += ') '
                    self.execute(sqlP + sqlS, varMap)
            # commit
            self.commit()
            tmpLog.debug('done')
            # return
            return True
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # get number of jobs to fetch
    def get_num_jobs_to_fetch(self, n_queues, interval):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger)
            tmpLog.debug('start')
            retMap = {}
            # sql to get queues
            sqlQ = "SELECT queueName,nQueueLimitJob FROM {0} ".format(pandaQueueTableName)
            sqlQ += "WHERE jobFetchTime IS NULL OR jobFetchTime<:timeLimit "
            sqlQ += "ORDER BY jobFetchTime "
            sqlQ += "FOR UPDATE "
            # sql to count nQueue
            sqlN = "SELECT count(*) FROM {0} ".format(jobTableName)
            sqlN += "WHERE computingSite=:computingSite AND status=:status "
            # sql to update timestamp
            sqlU = "UPDATE {0} SET jobFetchTime=:jobFetchTime ".format(pandaQueueTableName)
            sqlU += "WHERE queueName=:queueName "
            # get queues
            timeNow = datetime.datetime.utcnow()
            varMap = dict()
            varMap[':timeLimit'] = timeNow - datetime.timedelta(seconds=interval)
            self.execute(sqlQ, varMap)
            resQ = self.cur.fetchall()
            for queueName, nQueueLimit in resQ:
                # count nQueue
                varMap = dict()
                varMap[':computingSite'] = queueName
                varMap[':status'] = 'starting'
                self.execute(sqlN, varMap)
                nQueue, = self.cur.fetchone()
                # more jobs need to be queued
                if nQueue < nQueueLimit:
                    retMap[queueName] = nQueueLimit - nQueue
                # update timestamp
                varMap = dict()
                varMap[':queueName'] = queueName
                varMap[':jobFetchTime'] = timeNow
                self.execute(sqlU, varMap)
                # enough queues
                if len(retMap) >= n_queues:
                    break
            # commit
            self.commit()
            tmpLog.debug('got {0}'.format(str(retMap)))
            return retMap
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # get jobs to propagate checkpoints
    def get_jobs_to_propagate(self, max_jobs, lock_interval, update_interval, locked_by):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, 'thr={0}'.format(locked_by))
            tmpLog.debug('start')
            # sql to get jobs
            sql = "SELECT {0} FROM {1} ".format(JobSpec.column_names(), jobTableName)
            sql += "WHERE propagatorTime IS NOT NULL "
            sql += "AND ((propagatorTime<:lockTimeLimit AND propagatorLock IS NOT NULL) "
            sql += "OR propagatorTime<:updateTimeLimit) "
            sql += "ORDER BY propagatorTime LIMIT {0} ".format(max_jobs)
            sql += "FOR UPDATE "
            # sql to lock job
            sqlL = "UPDATE {0} SET propagatorTime=:timeNow,propagatorLock=:lockedBy ".format(jobTableName)
            sqlL += "WHERE PandaID=:PandaID AND propagatorTime<:lockTimeLimit "
            # sql to get events
            sqlE = "SELECT {0} FROM {1} ".format(EventSpec.column_names(), eventTableName)
            sqlE += "WHERE PandaID=:PandaID AND subStatus<>:statusDone "
            # sql to get file
            sqlF = "SELECT {0} FROM {1} ".format(FileSpec.column_names(), fileTableName)
            sqlF += "WHERE PandaID=:PandaID AND fileID=:fileID "
            # sql to get fileID of zip
            sqlZ = "SELECT zipFileID FROM {0} ".format(fileTableName)
            sqlZ += "WHERE PandaID=:PandaID AND fileID=:fileID "
            # get jobs
            timeNow = datetime.datetime.utcnow()
            lockTimeLimit = timeNow - datetime.timedelta(seconds=lock_interval)
            updateTimeLimit = timeNow - datetime.timedelta(seconds=update_interval)
            varMap = dict()
            varMap[':lockTimeLimit'] = lockTimeLimit
            varMap[':updateTimeLimit'] = updateTimeLimit
            self.execute(sql, varMap)
            resList = self.cur.fetchall()
            jobSpecList = []
            for res in resList:
                # make job
                jobSpec = JobSpec()
                jobSpec.pack(res)
                # lock job
                varMap = dict()
                varMap[':PandaID'] = jobSpec.PandaID
                varMap[':timeNow'] = timeNow
                varMap[':lockedBy'] = locked_by
                varMap[':lockTimeLimit'] = lockTimeLimit
                self.execute(sqlL, varMap)
                nRow = self.cur.rowcount
                if nRow > 0:
                    jobSpec.propagatorLock = locked_by
                    zipFiles = {}
                    # read events
                    varMap = dict()
                    varMap[':PandaID'] = jobSpec.PandaID
                    varMap[':statusDone'] = 'done'
                    self.execute(sqlE, varMap)
                    resEs = self.cur.fetchall()
                    for resE in resEs:
                        eventSpec = EventSpec()
                        eventSpec.pack(resE)
                        zipFileSpec = None
                        # get associated zip file if any
                        if eventSpec.fileID is not None:
                            varMap = dict()
                            varMap[':PandaID'] = jobSpec.PandaID
                            varMap[':fileID'] = eventSpec.fileID
                            self.execute(sqlZ, varMap)
                            zipFileID, = self.cur.fetchone()
                            if zipFileID is not None:
                                if zipFileID not in zipFiles:
                                    varMap = dict()
                                    varMap[':PandaID'] = jobSpec.PandaID
                                    varMap[':fileID'] = zipFileID
                                    self.execute(sqlF, varMap)
                                    resF = self.cur.fetchone()
                                    fileSpec = FileSpec()
                                    fileSpec.pack(resF)
                                    zipFiles[zipFileID] = fileSpec
                                zipFileSpec = zipFiles[zipFileID]
                        jobSpec.add_event(eventSpec, zipFileSpec)
                    jobSpecList.append(jobSpec)
            # commit
            self.commit()
            tmpLog.debug('got {0} jobs'.format(len(jobSpecList)))
            return jobSpecList
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    # get jobs in substatus
    def get_jobs_in_sub_status(self, sub_status, max_jobs, time_column=None, lock_column=None, interval_with_lock=None,
                               interval_without_lock=None, locked_by=None, new_sub_status=None):
        try:
            # get logger
            if locked_by is None:
                msgPfx = None
            else:
                msgPfx = 'thr={0}'.format(locked_by)
            tmpLog = core_utils.make_logger(_logger, msgPfx)
            tmpLog.debug('start subStatus={0} timeColumn={1}'.format(sub_status, time_column))
            # sql to count jobs beeing processed
            sqlC = "SELECT COUNT(*) FROM {0} ".format(jobTableName)
            sqlC += "WHERE ({0} IS NOT NULL AND subStatus=:subStatus) ".format(lock_column)
            sqlC += "OR subStatus=:newSubStatus "
            # count jobs
            if max_jobs > 0 and new_sub_status is not None:
                varMap = dict()
                varMap[':subStatus'] = sub_status
                varMap[':newSubStatus'] = new_sub_status
                self.execute(sqlC, varMap)
                nProcessing, = self.cur.fetchone()
                if nProcessing >= max_jobs:
                    # commit
                    self.commit()
                    tmpLog.debug('enough jobs {0} are beeing processed'.format(len(nProcessing)))
                    return []
                max_jobs -= nProcessing
            # sql to get jobs
            sql = "SELECT {0} FROM {1} ".format(JobSpec.column_names(), jobTableName)
            sql += "WHERE subStatus=:subStatus "
            if time_column is not None:
                sql += "AND ({0} IS NULL ".format(time_column)
                if interval_with_lock is not None:
                    sql += "OR ({0}<:lockTimeLimit AND {1} IS NOT NULL) ".format(time_column, lock_column)
                if interval_without_lock is not None:
                    sql += "OR {0}<:updateTimeLimit ".format(time_column)
                sql += ') '
                sql += "ORDER BY {0} ".format(time_column)
            sql += "LIMIT {0} ".format(max_jobs)
            sql += "FOR UPDATE "
            # sql to lock job
            sqlL = "UPDATE {0} SET {1}=:timeNow,{2}=:lockedBy ".format(jobTableName, time_column, lock_column)
            sqlL += "WHERE PandaID=:PandaID "
            # get jobs
            timeNow = datetime.datetime.utcnow()
            varMap = dict()
            varMap[':subStatus'] = sub_status
            if interval_with_lock is not None:
                varMap[':lockTimeLimit'] = timeNow - datetime.timedelta(seconds=interval_with_lock)
            if interval_without_lock is not None:
                varMap[':updateTimeLimit'] = timeNow - datetime.timedelta(seconds=interval_without_lock)
            self.execute(sql, varMap)
            resList = self.cur.fetchall()
            jobSpecList = []
            for res in resList:
                # make job
                jobSpec = JobSpec()
                jobSpec.pack(res)
                # lock job
                if locked_by is not None:
                    varMap = dict()
                    varMap[':PandaID'] = jobSpec.PandaID
                    varMap[':timeNow'] = timeNow
                    varMap[':lockedBy'] = locked_by
                    self.execute(sqlL, varMap)
                    nRow = self.cur.rowcount
                    jobSpec.lockedBy = locked_by
                    setattr(jobSpec, time_column, timeNow)
                else:
                    nRow = 1
                if nRow > 0:
                    jobSpecList.append(jobSpec)
            # commit
            self.commit()
            tmpLog.debug('got {0} jobs'.format(len(jobSpecList)))
            return jobSpecList
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    # register a worker
    def register_worker(self, workspec, jobspec_list, locked_by):
        try:
            tmpLog = core_utils.make_logger(_logger, 'batchID={0}'.format(workspec.batchID))
            tmpLog.debug('start')
            # sql to insert a worker
            sqlI = "INSERT INTO {0} ({1}) ".format(workTableName, WorkSpec.column_names())
            sqlI += WorkSpec.bind_values_expression()
            # sql to update a worker
            sqlU = "UPDATE {0} SET {1} ".format(workTableName, workspec.bind_update_changes_expression())
            sqlU += "WHERE workerID=:workerID "
            # sql to insert job and worker relationship
            sqlR = "INSERT INTO {0} ({1}) ".format(jobWorkerTableName, JobWorkerRelationSpec.column_names())
            sqlR += JobWorkerRelationSpec.bind_values_expression()
            # insert worker if new
            if workspec.isNew:
                varMap = workspec.values_list()
                self.execute(sqlI, varMap)
            else:
                varMap = workspec.values_map(only_changed=True)
                varMap[':workerID'] = workspec.workerID
                self.execute(sqlU, varMap)
            # collect values to update jobs or insert job/worker mapping
            varMapsR = []
            for jobSpec in jobspec_list:
                # update attributes
                if workspec.hasJob == 1:
                    jobSpec.subStatus = 'submitted'
                else:
                    jobSpec.subStatus = 'queued'
                jobSpec.lockedBy = None
                # sql to update job
                sqlJ = "UPDATE {0} SET {1} ".format(jobTableName, jobSpec.bind_update_changes_expression())
                sqlJ += "WHERE PandaID=:cr_PandaID AND lockedBy=:cr_lockedBy "
                # update job
                varMap = jobSpec.values_map(only_changed=True)
                varMap[':cr_PandaID'] = jobSpec.PandaID
                varMap[':cr_lockedBy'] = locked_by
                self.execute(sqlJ, varMap)
                if jobSpec.subStatus == 'submitted':
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
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # get queues to submit workers
    def get_queues_to_submit(self, n_queues, interval):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger)
            tmpLog.debug('start')
            retMap = dict()
            siteName = None
            # sql to get a site
            sqlS = "SELECT siteName FROM {0} ".format(pandaQueueTableName)
            sqlS += "WHERE submitTime IS NULL OR submitTime<:timeLimit "
            sqlS += "ORDER BY submitTime "
            sqlS += "FOR UPDATE "
            # sql to get queues
            sqlQ = "SELECT queueName FROM {0} ".format(pandaQueueTableName)
            sqlQ += "WHERE siteName=:siteName "
            # sql to count nQueue
            sqlN = "SELECT status,count(*) FROM {0} ".format(workTableName)
            sqlN += "WHERE computingSite=:computingSite GROUP BY status "
            # sql to update timestamp
            sqlU = "UPDATE {0} SET submitTime=:submitTime ".format(pandaQueueTableName)
            sqlU += "WHERE queueName=:queueName "
            # get sites
            timeNow = datetime.datetime.utcnow()
            varMap = dict()
            varMap[':timeLimit'] = timeNow - datetime.timedelta(seconds=interval)
            self.execute(sqlS, varMap)
            resS = self.cur.fetchone()
            if resS is not None:
                # get queues
                siteName, = resS
                varMap = dict()
                varMap[':siteName'] = siteName
                self.execute(sqlQ, varMap)
                resQ = self.cur.fetchall()
                for queueName, in resQ:
                    # count nQueue
                    varMap = dict()
                    varMap[':computingSite'] = queueName
                    self.execute(sqlN, varMap)
                    nQueue = 0
                    nReady = 0
                    nRunning = 0
                    for workerStatus, tmpNum in self.cur.fetchall():
                        if workerStatus in [WorkSpec.ST_submitted]:
                            nQueue += tmpNum
                        elif workerStatus in [WorkSpec.ST_ready]:
                            nReady += tmpNum
                        elif workerStatus in [WorkSpec.ST_running]:
                            nRunning += tmpNum
                    # add
                    retMap[queueName] = {'nReady': nReady,
                                         'nRunning': nRunning,
                                         'nQueue': nQueue}
                    # update timestamp
                    varMap = dict()
                    varMap[':queueName'] = queueName
                    varMap[':submitTime'] = timeNow
                    self.execute(sqlU, varMap)
                    # enough queues
                    if len(retMap) >= n_queues:
                        break
            # commit
            self.commit()
            tmpLog.debug('got {0}'.format(str(retMap)))
            return retMap, siteName
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}, None

    # get job chunks to make workers
    def get_job_chunks_for_workers(self, queue_name, n_workers, n_ready, n_jobs_per_worker, n_workers_per_job,
                                   use_job_late_binding, check_interval, lock_interval, locked_by):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, 'queue={0}'.format(queue_name))
            tmpLog.debug('start')
            # define maxJobs
            if n_jobs_per_worker is not None:
                maxJobs = (n_workers + n_ready) * n_jobs_per_worker
            else:
                maxJobs = -(-(n_workers + n_ready) // n_workers_per_job)
            # sql to get jobs
            sql = "SELECT {0} FROM {1} ".format(JobSpec.column_names(), jobTableName)
            sql += "WHERE subStatus IN (:subStatus1,:subStatus2) "
            sql += "AND (submitterTime IS NULL "
            sql += "OR ((submitterTime<:lockTimeLimit AND lockedBy IS NOT NULL) "
            sql += "OR submitterTime<:checkTimeLimit)) "
            sql += "AND computingSite=:queueName "
            sql += "ORDER BY currentPriority DESC,taskID,PandaID LIMIT {0} ".format(maxJobs)
            sql += "FOR UPDATE "
            # sql to lock job
            sqlL = "UPDATE {0} SET submitterTime=:timeNow,lockedBy=:lockedBy ".format(jobTableName)
            sqlL += "WHERE PandaID=:PandaID "
            # get jobs
            timeNow = datetime.datetime.utcnow()
            varMap = dict()
            varMap[':subStatus1'] = 'prepared'
            varMap[':subStatus2'] = 'queued'
            varMap[':queueName'] = queue_name
            varMap[':lockTimeLimit'] = timeNow - datetime.timedelta(seconds=lock_interval)
            varMap[':checkTimeLimit'] = timeNow - datetime.timedelta(seconds=check_interval)
            self.execute(sql, varMap)
            resList = self.cur.fetchall()
            jobChunkList = []
            jobChunk = []
            for res in resList:
                # make job
                jobSpec = JobSpec()
                jobSpec.pack(res)
                # new chunk
                if len(jobChunk) > 0 and jobChunk[0].taskID != jobSpec.taskID:
                    jobChunkList.append(jobChunk)
                    jobChunk = []
                # only prepared for new worker
                if len(jobChunkList) >= n_ready and jobSpec.subStatus == 'queued':
                    continue
                jobChunk.append(jobSpec)
                # enough jobs in chunk
                if n_jobs_per_worker is not None and len(jobChunk) >= n_jobs_per_worker:
                    jobChunkList.append(jobChunk)
                    jobChunk = []
                # one job per multiple workers
                elif n_workers_per_job is not None:
                    for i in range(n_workers_per_job):
                        jobChunkList.append(jobChunk)
                    jobChunk = []
                # lock job
                varMap = dict()
                varMap[':PandaID'] = jobSpec.PandaID
                varMap[':timeNow'] = timeNow
                varMap[':lockedBy'] = locked_by
                self.execute(sqlL, varMap)
                jobSpec.lockedBy = locked_by
                # enough job chunks
                if len(jobChunkList) >= n_workers:
                    break
            # commit
            self.commit()
            tmpLog.debug('got {0} job chunks'.format(len(jobChunkList)))
            return jobChunkList
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    # get workers to monitor
    def get_workers_to_update(self, max_workers, check_interval, lock_interval, locked_by):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger)
            tmpLog.debug('start')
            # sql to get workers
            sqlW = "SELECT workerID FROM {0} ".format(workTableName)
            sqlW += "WHERE status IN (:st_submitted,:st_running) "
            sqlW += "AND ((modificationTime<:lockTimeLimit AND lockedBy IS NOT NULL) "
            sqlW += "OR modificationTime<:checkTimeLimit) "
            sqlW += "ORDER BY modificationTime LIMIT {0} ".format(max_workers)
            sqlW += "FOR UPDATE "
            # sql to lock worker
            sqlL = "UPDATE {0} SET modificationTime=:timeNow,lockedBy=:lockedBy ".format(workTableName)
            sqlL += "WHERE workerID=:workerID "
            # sql to get associated workerIDs
            sqlA = "SELECT t.workerID FROM {0} t, {0} s ".format(jobWorkerTableName)
            sqlA += "WHERE s.PandaID=t.PandaID AND s.workerID=:workerID "
            # sql to get associated workers
            sqlG = "SELECT {0} FROM {1} ".format(WorkSpec.column_names(), workTableName)
            sqlG += "WHERE workerID=:workerID "
            # get workerIDs
            timeNow = datetime.datetime.utcnow()
            varMap = dict()
            varMap[':st_submitted'] = WorkSpec.ST_submitted
            varMap[':st_running'] = WorkSpec.ST_running
            varMap[':lockTimeLimit'] = timeNow - datetime.timedelta(seconds=lock_interval)
            varMap[':checkTimeLimit'] = timeNow - datetime.timedelta(seconds=check_interval)
            self.execute(sqlW, varMap)
            resW = self.cur.fetchall()
            tmpWorkers = set()
            for workerID, in resW:
                tmpWorkers.add(workerID)
            checkedIDs = set()
            retVal = {}
            for workerID in tmpWorkers:
                # skip 
                if workerID in checkedIDs:
                    continue
                # get associated workerIDs
                varMap = dict()
                varMap[':workerID'] = workerID
                self.execute(sqlA, varMap)
                resA = self.cur.fetchall()
                # get workers
                queueName = None
                workersList = []
                workerIDtoScan = set()
                for tmpWorkID, in resA:
                    workerIDtoScan.add(tmpWorkID)
                # add original ID just in case since no relation when job is not yet bound
                workerIDtoScan.add(workerID)
                for tmpWorkID in workerIDtoScan:
                    checkedIDs.add(tmpWorkID)
                    # get worker
                    varMap = dict()
                    varMap[':workerID'] = tmpWorkID
                    self.execute(sqlG, varMap)
                    resG = self.cur.fetchone()
                    workSpec = WorkSpec()
                    workSpec.pack(resG)
                    if queueName is None:
                        queueName = workSpec.computingSite
                    workersList.append(workSpec)
                    # lock worker
                    varMap = dict()
                    varMap[':workerID'] = tmpWorkID
                    varMap[':lockedBy'] = locked_by
                    varMap[':timeNow'] = timeNow
                    self.execute(sqlL, varMap)
                    workSpec.lockedBy = locked_by
                # add
                if queueName is not None:
                    if queueName not in retVal:
                        retVal[queueName] = []
                    retVal[queueName].append(workersList)
            # commit
            self.commit()
            tmpLog.debug('got {0}'.format(str(retVal)))
            return retVal
        except:
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
            tmpLog = core_utils.make_logger(_logger)
            tmpLog.debug('start')
            # sql to get worker IDs
            sqlW = "SELECT workerID FROM {0} ".format(workTableName)
            sqlW += "WHERE lastUpdate IS NOT NULL AND lastUpdate<:checkTimeLimit "
            sqlW += "ORDER BY lastUpdate LIMIT {0} ".format(max_workers)
            sqlW += "FOR UPDATE "
            # sql to lock worker
            sqlL = "UPDATE {0} SET lastUpdate=:timeNow ".format(workTableName)
            sqlL += "WHERE workerID=:workerID "
            # sql to get associated PandaIDs
            sqlA = "SELECT PandaID FROM {0} ".format(jobWorkerTableName)
            sqlA += "WHERE workerID=:workerID "
            # sql to get workers
            sqlG = "SELECT {0} FROM {1} ".format(WorkSpec.column_names(), workTableName)
            sqlG += "WHERE workerID=:workerID "
            timeNow = datetime.datetime.utcnow()
            # get workerIDs
            varMap = dict()
            varMap[':checkTimeLimit'] = timeNow - datetime.timedelta(seconds=check_interval)
            self.execute(sqlW, varMap)
            resW = self.cur.fetchall()
            tmpWorkers = set()
            for workerID, in resW:
                tmpWorkers.add(workerID)
            retVal = []
            for workerID in tmpWorkers:
                # get worker
                varMap = dict()
                varMap[':workerID'] = workerID
                self.execute(sqlG, varMap)
                resG = self.cur.fetchone()
                workSpec = WorkSpec()
                workSpec.pack(resG)
                retVal.append(workSpec)
                # lock worker with N sec offset for time-based locking
                varMap = dict()
                varMap[':workerID'] = workerID
                varMap[':timeNow'] = timeNow + datetime.timedelta(seconds=5)
                self.execute(sqlL, varMap)
                # get associated PandaIDs
                varMap = dict()
                varMap[':workerID'] = workerID
                self.execute(sqlA, varMap)
                resA = self.cur.fetchall()
                workSpec.pandaid_list = []
                for pandaID, in resA:
                    workSpec.pandaid_list.append(pandaID)
            # commit
            self.commit()
            tmpLog.debug('got {0} workers'.format(len(retVal)))
            return retVal
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # get workers to feed events
    def get_workers_to_feed_events(self, max_workers, lock_interval):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger)
            tmpLog.debug('start')
            # sql to get workers
            sqlW = "SELECT workerID, status FROM {0} ".format(workTableName)
            sqlW += "WHERE eventsRequest=:eventsRequest AND status IN (:status1,:status2) "
            sqlW += "AND (eventFeedTime IS NULL OR eventFeedTime<:lockTimeLimit) "
            sqlW += "ORDER BY eventFeedTime LIMIT {0} ".format(max_workers)
            sqlW += "FOR UPDATE "
            # sql to lock worker
            sqlL = "UPDATE {0} SET eventFeedTime=:timeNow ".format(workTableName)
            sqlL += "WHERE eventsRequest=:eventsRequest AND status=:status "
            sqlL += "AND (eventFeedTime IS NULL OR eventFeedTime<:lockTimeLimit) "
            sqlL += "AND workerID=:workerID "
            # sql to get associated workers
            sqlG = "SELECT {0} FROM {1} ".format(WorkSpec.column_names(), workTableName)
            sqlG += "WHERE workerID=:workerID "
            # get workerIDs
            timeNow = datetime.datetime.utcnow()
            lockTimeLimit = timeNow - datetime.timedelta(seconds=lock_interval)
            varMap = dict()
            varMap[':status1'] = WorkSpec.ST_running
            varMap[':status2'] = WorkSpec.ST_submitted
            varMap[':eventsRequest'] = WorkSpec.EV_requestEvents
            varMap[':lockTimeLimit'] = lockTimeLimit
            self.execute(sqlW, varMap)
            resW = self.cur.fetchall()
            tmpWorkers = dict()
            for tmpWorkerID, tmpWorkStatus in resW:
                tmpWorkers[tmpWorkerID] = tmpWorkStatus
            retVal = {}
            for workerID, workStatus in tmpWorkers.iteritems():
                # lock worker
                varMap = dict()
                varMap[':workerID'] = workerID
                varMap[':timeNow'] = timeNow
                varMap[':status'] = workStatus
                varMap[':eventsRequest'] = WorkSpec.EV_requestEvents
                varMap[':lockTimeLimit'] = lockTimeLimit
                self.execute(sqlL, varMap)
                nRow = self.cur.rowcount
                if nRow > 0:
                    # get worker
                    varMap = dict()
                    varMap[':workerID'] = workerID
                    self.execute(sqlG, varMap)
                    resG = self.cur.fetchone()
                    workSpec = WorkSpec()
                    workSpec.pack(resG)
                    if workSpec.computingSite not in retVal:
                        retVal[workSpec.computingSite] = []
                    retVal[workSpec.computingSite].append(workSpec)
            # commit
            self.commit()
            tmpLog.debug('got {0} workers'.format(len(retVal)))
            return retVal
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # update jobs and workers
    def update_jobs_workers(self, jobspec_list, workspec_list, locked_by, panda_ids_list=None):
        try:
            timeNow = datetime.datetime.utcnow()
            # sql to check file
            sqlFC = "SELECT 1 FROM {0} WHERE PandaID=:PandaID AND lfn=:lfn ".format(fileTableName)
            # sql to insert file
            sqlFI = "INSERT INTO {0} ({1}) ".format(fileTableName, FileSpec.column_names())
            sqlFI += FileSpec.bind_values_expression()
            # sql to get pending files
            sqlFP = "SELECT fileID,fsize,lfn FROM {0} ".format(fileTableName)
            sqlFP += "WHERE PandaID=:PandaID AND status=:status "
            # sql to update pending files
            sqlFU = "UPDATE {0} ".format(fileTableName)
            sqlFU += "SET status=:status,zipFileID=:zipFileID "
            sqlFU += "WHERE fileID=:fileID "
            # sql to check event
            sqlEC = "SELECT 1 FROM {0} WHERE PandaID=:PandaID AND eventRangeID=:eventRangeID ".format(eventTableName)
            # sql to check associated file
            sqlEF = "SELECT status FROM {0} WHERE PandaID=:PandaID AND eventRangeID=:eventRangeID ".format(
                fileTableName)
            # sql to insert event
            sqlEI = "INSERT INTO {0} ({1}) ".format(eventTableName, EventSpec.column_names())
            sqlEI += EventSpec.bind_values_expression()
            # sql to update event
            sqlEU = "UPDATE {0} ".format(eventTableName)
            sqlEU += "SET eventStatus=:eventStatus,subStatus=:subStatus "
            sqlEU += "WHERE PandaID=:PandaID AND eventRangeID=:eventRangeID "
            # sql to check if relationship is already available
            sqlCR = "SELECT 1 FROM {0} WHERE PandaID=:PandaID AND workerID=:workerID ".format(jobWorkerTableName)
            # sql to insert job and worker relationship
            sqlIR = "INSERT INTO {0} ({1}) ".format(jobWorkerTableName, JobWorkerRelationSpec.column_names())
            sqlIR += JobWorkerRelationSpec.bind_values_expression()
            # update job
            if jobspec_list is not None:
                for jobSpec in jobspec_list:
                    tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(jobSpec.PandaID))
                    # insert files
                    nFiles = 0
                    fileIdMap = {}
                    for fileSpec in jobSpec.outFiles:
                        # check file
                        varMap = dict()
                        varMap[':PandaID'] = fileSpec.PandaID
                        varMap[':lfn'] = fileSpec.lfn
                        self.execute(sqlFC, varMap)
                        resFC = self.cur.fetchone()
                        # insert file
                        if resFC is None:
                            if jobSpec.zipPerMB is None or fileSpec.isZip == 0:
                                fileSpec.status = 'defined'
                                jobSpec.hasOutFile = JobSpec.HO_hasOutput
                            else:
                                fileSpec.status = 'pending'
                            varMap = fileSpec.values_list()
                            self.execute(sqlFI, varMap)
                            nFiles += 1
                            # mapping between event range ID and file ID
                            if fileSpec.eventRangeID is not None:
                                fileIdMap[fileSpec.eventRangeID] = self.cur.lastrowid
                    if nFiles > 0:
                        tmpLog.debug('inserted {0} files'.format(nFiles))
                    # check pending files
                    if jobSpec.zipPerMB is not None and \
                            not (jobSpec.zipPerMB == 0 and jobSpec.subStatus != 'to_transfer'):
                        varMap = dict()
                        varMap[':PandaID'] = jobSpec.PandaID
                        varMap[':status'] = 'pending'
                        self.execute(sqlFP, varMap)
                        resFP = self.cur.fetchall()
                        tmpLog.debug('got {0} pending files'.format(len(resFP)))
                        # make subsets
                        subTotalSize = 0
                        subFileIDs = []
                        zippedFileIDs = []
                        for tmpFileID, tmpFsize, tmpLFN in resFP:
                            if jobSpec.zipPerMB > 0 and subTotalSize > 0 \
                                    and subTotalSize + tmpFsize > jobSpec.zipPerMB * 1024 * 1024:
                                zippedFileIDs.append(subFileIDs)
                                subFileIDs = []
                                subTotalSize = 0
                            subTotalSize += tmpFsize
                            subFileIDs.append((tmpFileID, tmpLFN))
                        if (jobSpec.subStatus == 'to_transfer' or subTotalSize > jobSpec.zipPerMB * 1024 * 1024) \
                                and len(subFileIDs) > 0:
                            zippedFileIDs.append(subFileIDs)
                        # make zip files
                        for subFileIDs in zippedFileIDs:
                            # insert zip file
                            fileSpec = FileSpec()
                            fileSpec.status = 'zipping'
                            fileSpec.lfn = 'panda.' + subFileIDs[0][-1] + '.zip'
                            fileSpec.fileType = 'zip_output'
                            fileSpec.PandaID = jobSpec.PandaID
                            fileSpec.taskID = jobSpec.taskID
                            fileSpec.isZip = 1
                            varMap = fileSpec.values_list()
                            self.execute(sqlFI, varMap)
                            # update pending files
                            varMaps = []
                            for tmpFileID, tmpLFN in subFileIDs:
                                varMap = dict()
                                varMap[':status'] = 'zipped'
                                varMap[':fileID'] = tmpFileID
                                varMap[':zipFileID'] = self.cur.lastrowid
                                varMaps.append(varMap)
                            self.executemany(sqlFU, varMaps)
                        # set zip output flag
                        if len(zippedFileIDs) > 0:
                            jobSpec.hasOutFile = JobSpec.HO_hasZipOutput
                    # insert or update events
                    varMapsEI = []
                    varMapsEU = []
                    for eventSpec in jobSpec.events:
                        # set subStatus
                        if eventSpec.eventStatus == 'finished':
                            # check associated file
                            varMap = dict()
                            varMap[':PandaID'] = jobSpec.PandaID
                            varMap[':eventRangeID'] = eventSpec.eventRangeID
                            self.execute(sqlEF, varMap)
                            resEF = self.cur.fetchone()
                            if resEF is None or resEF[0] == 'finished':
                                eventSpec.subStatus = 'finished'
                            elif resEF[0] == 'failed':
                                eventSpec.eventStatus = 'failed'
                                eventSpec.subStatus = 'failed'
                            else:
                                eventSpec.subStatus = 'transferring'
                        else:
                            eventSpec.subStatus = eventSpec.eventStatus
                        # set fileID
                        if eventSpec.eventRangeID in fileIdMap:
                            eventSpec.fileID = fileIdMap[eventSpec.eventRangeID]
                        # check event
                        varMap = dict()
                        varMap[':PandaID'] = jobSpec.PandaID
                        varMap[':eventRangeID'] = eventSpec.eventRangeID
                        self.execute(sqlEC, varMap)
                        resEC = self.cur.fetchone()
                        # insert or update event
                        if resEC is None:
                            varMap = eventSpec.values_list()
                            varMapsEI.append(varMap)
                        else:
                            varMap = dict()
                            varMap[':PandaID'] = jobSpec.PandaID
                            varMap[':eventRangeID'] = eventSpec.eventRangeID
                            varMap[':eventStatus'] = eventSpec.eventStatus
                            varMap[':subStatus'] = eventSpec.subStatus
                            varMapsEU.append(varMap)
                    if len(varMapsEI) > 0:
                        self.executemany(sqlEI, varMapsEI)
                        tmpLog.debug('inserted {0} event'.format(len(varMapsEI)))
                    if len(varMapsEU) > 0:
                        self.executemany(sqlEU, varMapsEU)
                        tmpLog.debug('updated {0} event'.format(len(varMapsEU)))
                    tmpLog.debug('update job')
                    # sql to update job
                    sqlJ = "UPDATE {0} SET {1} ".format(jobTableName, jobSpec.bind_update_changes_expression())
                    sqlJ += "WHERE PandaID=:PandaID AND lockedBy=:cr_lockedBy "
                    jobSpec.lockedBy = None
                    jobSpec.modificationTime = timeNow
                    varMap = jobSpec.values_map(only_changed=True)
                    varMap[':PandaID'] = jobSpec.PandaID
                    varMap[':cr_lockedBy'] = locked_by
                    self.execute(sqlJ, varMap)
                    nRow = self.cur.rowcount
                    tmpLog.debug('done with {0}'.format(nRow))
            # update worker
            for idxW, workSpec in enumerate(workspec_list):
                tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workSpec.workerID))
                tmpLog.debug('update')
                # sql to update worker
                sqlW = "UPDATE {0} SET {1} ".format(workTableName, workSpec.bind_update_changes_expression())
                sqlW += "WHERE workerID=:workerID AND lockedBy=:cr_lockedBy "
                workSpec.lockedBy = None
                if not workSpec.nextLookup:
                    workSpec.modificationTime = timeNow
                else:
                    workSpec.nextLookup = False
                varMap = workSpec.values_map(only_changed=True)
                varMap[':workerID'] = workSpec.workerID
                varMap[':cr_lockedBy'] = locked_by
                self.execute(sqlW, varMap)
                nRow = self.cur.rowcount
                tmpLog.debug('done with {0}'.format(nRow))
                # insert relationship if necessary
                if panda_ids_list is not None and len(panda_ids_list) > idxW:
                    varMapsIR = []
                    for pandaID in panda_ids_list[idxW]:
                        varMap = dict()
                        varMap[':PandaID'] = pandaID
                        varMap[':workerID'] = workSpec.workerID
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
            # commit
            self.commit()
            # return
            return True
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # get jobs with workerID
    def get_jobs_with_worker_id(self, worker_id, locked_by):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(worker_id))
            tmpLog.debug('start')
            # sql to get PandaIDs
            sqlP = "SELECT PandaID FROM {0} ".format(jobWorkerTableName)
            sqlP += "WHERE workerID=:workerID "
            # sql to get jobs
            sqlJ = "SELECT {0} FROM {1} ".format(JobSpec.column_names(), jobTableName)
            sqlJ += "WHERE PandaID=:PandaID "
            # sql to lock job
            sqlL = "UPDATE {0} SET modificationTime=:timeNow,lockedBy=:lockedBy ".format(jobTableName)
            sqlL += "WHERE PandaID=:PandaID "
            # get jobs
            jobChunkList = []
            timeNow = datetime.datetime.utcnow()
            varMap = dict()
            varMap[':workerID'] = worker_id
            self.execute(sqlP, varMap)
            resW = self.cur.fetchall()
            for pandaID, in resW:
                # get job
                varMap = dict()
                varMap[':PandaID'] = pandaID
                self.execute(sqlJ, varMap)
                resJ = self.cur.fetchone()
                # make job
                jobSpec = JobSpec()
                jobSpec.pack(resJ)
                jobSpec.lockedBy = locked_by
                # lock job
                if locked_by is not None:
                    varMap = dict()
                    varMap[':PandaID'] = pandaID
                    varMap[':lockedBy'] = locked_by
                    varMap[':timeNow'] = timeNow
                    self.execute(sqlL, varMap)
                jobChunkList.append(jobSpec)
            # commit
            self.commit()
            tmpLog.debug('got {0} job chunks'.format(len(jobChunkList)))
            return jobChunkList
        except:
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
            tmpLog = core_utils.make_logger(_logger, 'queue={0}'.format(queue_name))
            tmpLog.debug('start')
            # sql to get workers
            sqlG = "SELECT {0} FROM {1} ".format(WorkSpec.column_names(), workTableName)
            sqlG += "WHERE status=:status AND computingSite=:queueName "
            sqlG += "ORDER BY modificationTime LIMIT {0} ".format(n_ready)
            # get workers
            varMap = dict()
            varMap[':status'] = WorkSpec.ST_ready
            varMap[':queueName'] = queue_name
            self.execute(sqlG, varMap)
            resList = self.cur.fetchall()
            retVal = []
            for res in resList:
                workSpec = WorkSpec()
                workSpec.pack(res)
                retVal.append(workSpec)
            # commit
            self.commit()
            tmpLog.debug('got {0}'.format(str(retVal)))
            return retVal
        except:
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
            tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(worker_id))
            tmpLog.debug('start')
            # sql to get a worker
            sqlG = "SELECT {0} FROM {1} ".format(WorkSpec.column_names(), workTableName)
            sqlG += "WHERE workerID=:workerID "
            # get a worker
            varMap = dict()
            varMap[':workerID'] = worker_id
            self.execute(sqlG, varMap)
            res = self.cur.fetchone()
            workSpec = WorkSpec()
            workSpec.pack(res)
            # commit
            self.commit()
            tmpLog.debug('got')
            return workSpec
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # get jobs to trigger or check output transfer or zip output
    def get_jobs_for_stage_out(self, max_jobs, interval_with_lock, interval_without_lock, locked_by,
                               sub_status, has_out_file_flag, bad_has_out_file_flag=None):
        try:
            # get logger
            msgPfx = 'thr={0}'.format(locked_by)
            tmpLog = core_utils.make_logger(_logger, msgPfx)
            tmpLog.debug('start')
            # sql to get jobs
            sql = "SELECT {0} FROM {1} ".format(JobSpec.column_names(), jobTableName)
            sql += "WHERE (subStatus=:subStatus OR hasOutFile=:hasOutFile) "
            if bad_has_out_file_flag is not None:
                sql += "AND hasOutFile<>:badHasOutFile "
            sql += "AND (stagerTime IS NULL "
            sql += "OR (stagerTime<:lockTimeLimit AND stagerLock IS NOT NULL) "
            sql += "OR stagerTime<:updateTimeLimit) "
            sql += "ORDER BY stagerTime "
            sql += "LIMIT {0} ".format(max_jobs)
            sql += "FOR UPDATE "
            # sql to lock job
            sqlL = "UPDATE {0} SET stagerTime=:timeNow,stagerLock=:lockedBy ".format(jobTableName)
            sqlL += "WHERE PandaID=:PandaID "
            sqlL += "AND (stagerTime IS NULL "
            sqlL += "OR (stagerTime<:lockTimeLimit AND stagerLock IS NOT NULL) "
            sqlL += "OR stagerTime<:updateTimeLimit) "
            # sql to get files
            sqlF = "SELECT {0} FROM {1} ".format(FileSpec.column_names(), fileTableName)
            sqlF += "WHERE PandaID=:PandaID AND status=:status "
            # sql to get associated files
            sqlAF = "SELECT {0} FROM {1} ".format(FileSpec.column_names(), fileTableName)
            sqlAF += "WHERE PandaID=:PandaID AND zipFileID=:zipFileID "
            # get jobs
            timeNow = datetime.datetime.utcnow()
            lockTimeLimit = timeNow - datetime.timedelta(seconds=interval_with_lock)
            updateTimeLimit = timeNow - datetime.timedelta(seconds=interval_without_lock)
            varMap = dict()
            varMap[':subStatus'] = sub_status
            varMap[':hasOutFile'] = has_out_file_flag
            if bad_has_out_file_flag is not None:
                varMap[':badHasOutFile'] = bad_has_out_file_flag
            varMap[':lockTimeLimit'] = lockTimeLimit
            varMap[':updateTimeLimit'] = updateTimeLimit
            self.execute(sql, varMap)
            resList = self.cur.fetchall()
            jobSpecList = []
            for res in resList:
                # make job
                jobSpec = JobSpec()
                jobSpec.pack(res)
                # lock job
                varMap = dict()
                varMap[':PandaID'] = jobSpec.PandaID
                varMap[':timeNow'] = timeNow
                varMap[':lockedBy'] = locked_by
                varMap[':lockTimeLimit'] = lockTimeLimit
                varMap[':updateTimeLimit'] = updateTimeLimit
                self.execute(sqlL, varMap)
                nRow = self.cur.rowcount
                if nRow > 0:
                    jobSpec.stagerLock = locked_by
                    jobSpec.stagerTime = timeNow
                    # get files
                    varMap = dict()
                    varMap[':PandaID'] = jobSpec.PandaID
                    if has_out_file_flag == JobSpec.HO_hasOutput:
                        varMap[':status'] = 'defined'
                    elif has_out_file_flag == JobSpec.HO_hasZipOutput:
                        varMap[':status'] = 'zipping'
                    else:
                        varMap[':status'] = 'transferring'
                    self.execute(sqlF, varMap)
                    resFileList = self.cur.fetchall()
                    for resFile in resFileList:
                        fileSpec = FileSpec()
                        fileSpec.pack(resFile)
                        jobSpec.add_out_file(fileSpec)
                    jobSpecList.append(jobSpec)
                    # get associated files
                    if has_out_file_flag == JobSpec.HO_hasZipOutput:
                        for fileSpec in jobSpec.outFiles:
                            varMap = dict()
                            varMap[':PandaID'] = fileSpec.PandaID
                            varMap[':zipFileID'] = fileSpec.fileID
                            self.execute(sqlAF, varMap)
                            resAFs = self.cur.fetchall()
                            for resAF in resAFs:
                                assFileSpec = FileSpec()
                                assFileSpec.pack(resAF)
                                fileSpec.add_associated_file(assFileSpec)
            # commit
            self.commit()
            tmpLog.debug('got {0} jobs'.format(len(jobSpecList)))
            return jobSpecList
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    # update job for stage-out
    def update_job_for_stage_out(self, jobspec, update_event_status):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, 'PandaID={0} subStatus={1}'.format(jobspec.PandaID,
                                                                                        jobspec.subStatus))
            tmpLog.debug('start')
            # sql to update event
            sqlEU = "UPDATE {0} ".format(eventTableName)
            sqlEU += "SET eventStatus=:eventStatus,subStatus=:subStatus "
            sqlEU += "WHERE PandaID=:PandaID AND eventRangeID=:eventRangeID "
            sqlEU += "AND eventStatus<>:statusFailed AND subStatus<>:statusDone "
            # get associated events
            sqlAE = "SELECT eventRangeID FROM {0} ".format(fileTableName)
            sqlAE += "WHERE PandaID=:PandaID AND zipFileID=:zipFileID "
            # update files
            for fileSpec in jobspec.outFiles:
                # sql to update file
                sqlF = "UPDATE {0} SET {1} ".format(fileTableName, fileSpec.bind_update_changes_expression())
                sqlF += "WHERE PandaID=:PandaID AND fileID=:fileID "
                varMap = fileSpec.values_map(only_changed=True)
                if len(varMap) > 0:
                    varMap[':PandaID'] = fileSpec.PandaID
                    varMap[':fileID'] = fileSpec.fileID
                    self.execute(sqlF, varMap)
                # update event status
                if update_event_status:
                    eventRangeIDs = []
                    if fileSpec.eventRangeID is not None:
                        eventRangeIDs.append(fileSpec.eventRangeID)
                    elif fileSpec.isZip == 1:
                        # get files associated with zip file
                        varMap = dict()
                        varMap[':PandaID'] = fileSpec.PandaID
                        varMap[':zipFileID'] = fileSpec.fileID
                        self.execute(sqlAE, varMap)
                        resAE = self.cur.fetchall()
                        for eventRangeID, in resAE:
                            eventRangeIDs.append(eventRangeID)
                    varMaps = []
                    for eventRangeID in eventRangeIDs:
                        varMap = dict()
                        varMap[':PandaID'] = fileSpec.PandaID
                        varMap[':eventRangeID'] = eventRangeID
                        varMap[':eventStatus'] = fileSpec.status
                        varMap[':subStatus'] = fileSpec.status
                        varMap[':statusFailed'] = 'failed'
                        varMap[':statusDone'] = 'done'
                        varMaps.append(varMap)
                    self.executemany(sqlEU, varMaps)
            # count files
            sqlC = "SELECT COUNT(*),status FROM {0} ".format(fileTableName)
            sqlC += "WHERE PandaID=:PandaID GROUP BY status "
            varMap = dict()
            varMap[':PandaID'] = jobspec.PandaID
            self.execute(sqlC, varMap)
            resC = self.cur.fetchall()
            cntMap = {}
            for cnt, fileStatus in resC:
                cntMap[fileStatus] = cnt
            # set job attributes
            jobspec.stagerLock = None
            if 'zipping' in cntMap:
                jobspec.hasOutFile = JobSpec.HO_hasZipOutput
            elif 'defined' in cntMap:
                jobspec.hasOutFile = JobSpec.HO_hasOutput
            elif 'transferring' in cntMap:
                jobspec.hasOutFile = JobSpec.HO_hasTransfer
            else:
                jobspec.hasOutFile = JobSpec.HO_noOutput
            if jobspec.subStatus == 'to_transfer':
                # change subStatus when no more files to trigger transfer
                if jobspec.hasOutFile not in [JobSpec.HO_hasOutput, JobSpec.HO_hasZipOutput]:
                    jobspec.subStatus = 'transferring'
                jobspec.stagerTime = None
            elif jobspec.subStatus == 'transferring':
                # all done
                if jobspec.hasOutFile == JobSpec.HO_noOutput:
                    jobspec.trigger_propagation()
                    if 'failed' in cntMap:
                        jobspec.status = 'failed'
                        jobspec.subStatus = 'failed_to_stage_out'
                    else:
                        jobspec.subStatus = 'staged'
                        # get finished files
                        jobspec.reset_out_file()
                        sqlFF = "SELECT {0} FROM {1} ".format(FileSpec.column_names(), fileTableName)
                        sqlFF += "WHERE PandaID=:PandaID AND status=:status AND fileType IN (:type1,:type2) "
                        varMap = dict()
                        varMap[':PandaID'] = jobspec.PandaID
                        varMap[':status'] = 'finished'
                        varMap[':type1'] = 'output'
                        varMap[':type2'] = 'log'
                        self.execute(sqlFF, varMap)
                        resFileList = self.cur.fetchall()
                        for resFile in resFileList:
                            fileSpec = FileSpec()
                            fileSpec.pack(resFile)
                            jobspec.add_out_file(fileSpec)
                        # make file report
                        jobspec.outputFilesToReport = core_utils.get_output_file_report(jobspec)
            # sql to update job
            sqlJ = "UPDATE {0} SET {1} ".format(jobTableName, jobspec.bind_update_changes_expression())
            sqlJ += "WHERE PandaID=:PandaID "
            # update job
            varMap = jobspec.values_map(only_changed=True)
            varMap[':PandaID'] = jobspec.PandaID
            self.execute(sqlJ, varMap)
            # commit
            self.commit()
            tmpLog.debug('done')
            # return
            return jobspec.subStatus
        except:
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
            sqlC = "SELECT curVal FROM {0} WHERE numberName=:numberName ".format(seqNumberTableName)
            varMap = dict()
            varMap[':numberName'] = number_name
            self.execute(sqlC, varMap)
            res = self.cur.fetchone()
            # insert if missing
            if res is None:
                # make spec
                seqNumberSpec = SeqNumberSpec()
                seqNumberSpec.numberName = number_name
                seqNumberSpec.curVal = init_value
                # insert
                sqlI = "INSERT INTO {0} ({1}) ".format(seqNumberTableName, SeqNumberSpec.column_names())
                sqlI += SeqNumberSpec.bind_values_expression()
                varMap = seqNumberSpec.values_list()
                self.execute(sqlI, varMap)
            # commit
            self.commit()
            return True
        except:
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
            tmpLog = core_utils.make_logger(_logger, 'name={0}'.format(number_name))
            # increment
            sqlU = "UPDATE {0} SET curVal=curVal+1 WHERE numberName=:numberName ".format(seqNumberTableName)
            varMap = dict()
            varMap[':numberName'] = number_name
            self.execute(sqlU, varMap)
            # get
            sqlG = "SELECT curVal FROM {0} WHERE numberName=:numberName ".format(seqNumberTableName)
            varMap = dict()
            varMap[':numberName'] = number_name
            self.execute(sqlG, varMap)
            retVal, = self.cur.fetchone()
            # commit
            self.commit()
            tmpLog.debug('got {0}'.format(retVal))
            return retVal
        except:
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
            tmpLog = core_utils.make_logger(_logger, 'mainKey={0} subKey={1}'.format(main_key, sub_key))
            # get
            varMap = dict()
            varMap[":mainKey"] = main_key
            sqlU = "SELECT lastUpdate FROM {0} WHERE mainKey=:mainKey ".format(cacheTableName)
            if sub_key is not None:
                sqlU += "AND subKey=:subKey "
                varMap[":subKey"] = sub_key
            self.execute(sqlU, varMap)
            retVal = self.cur.fetchone()
            if retVal is not None:
                retVal, = retVal
            # commit
            self.commit()
            tmpLog.debug('got {0}'.format(retVal))
            return retVal
        except:
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
            tmpLog = core_utils.make_logger(_logger, 'mainKey={0} subKey={1}'.format(main_key, sub_key))
            # make spec
            cacheSpec = CacheSpec()
            cacheSpec.lastUpdate = datetime.datetime.utcnow()
            cacheSpec.data = new_info
            # check if already there
            varMap = dict()
            varMap[":mainKey"] = main_key
            sqlC = "SELECT lastUpdate FROM {0} WHERE mainKey=:mainKey ".format(cacheTableName)
            if sub_key is not None:
                sqlC += "AND subKey=:subKey "
                varMap[":subKey"] = sub_key
            self.execute(sqlC, varMap)
            retC = self.cur.fetchone()
            if retC is None:
                # insert if missing
                cacheSpec.mainKey = main_key
                cacheSpec.subKey = sub_key
                sqlU = "INSERT INTO {0} ({1}) ".format(cacheTableName, CacheSpec.column_names())
                sqlU += CacheSpec.bind_values_expression()
                varMap = cacheSpec.values_list()
            else:
                # update
                sqlU = "UPDATE {0} SET {1} ".format(cacheTableName, cacheSpec.bind_update_changes_expression())
                sqlU += "WHERE mainKey=:mainKey "
                varMap = cacheSpec.values_map(only_changed=True)
                varMap[":mainKey"] = main_key
                if sub_key is not None:
                    sqlU += "AND subKey=:subKey "
                    varMap[":subKey"] = sub_key
            self.execute(sqlU, varMap)
            # commit
            self.commit()
            tmpLog.debug('refreshed')
            return True
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False

    # get a cached info
    def get_cache(self, main_key, sub_key):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger, 'mainKey={0} subKey={1}'.format(main_key, sub_key))
            tmpLog.debug('start')
            # sql to get job
            sql = "SELECT {0} FROM {1} ".format(CacheSpec.column_names(), cacheTableName)
            sql += "WHERE mainKey=:mainKey "
            varMap = dict()
            varMap[":mainKey"] = main_key
            if sub_key is not None:
                sql += "AND subKey=:subKey "
                varMap[":subKey"] = sub_key
            self.execute(sql, varMap)
            resJ = self.cur.fetchone()
            # commit
            self.commit()
            if resJ is None:
                return None
            # make spec
            cacheSpec = CacheSpec()
            cacheSpec.pack(resJ)
            tmpLog.debug('done')
            # return
            return cacheSpec
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return None

    # store commands
    def store_commands(self, command_specs):
        # get logger
        tmpLog = core_utils.make_logger(_logger)
        tmpLog.debug('{0} commands'.format(len(command_specs)))
        if not command_specs:
            return True
        try:
            # sql to insert a command
            sql = "INSERT INTO {0} ({1}) ".format(commandTableName, CommandSpec.column_names())
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
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(tmpLog)
            # return
            return False

    # get command ids that have been processed and need to be acknowledged to panda server
    def get_commands_ack(self):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger)
            tmpLog.debug('start')
            # sql to get commands that have been processed and need acknowledgement
            sql = """
                  SELECT command_id FROM {0}
                  WHERE ack_requested=1
                  AND processed=1
                  """.format(commandTableName)
            self.execute(sql)
            command_ids = [row[0] for row in self.cur.fetchall()]
            tmpLog.debug('command_ids {0}'.format(command_ids))
            return command_ids
        except:
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return []

    def clean_commands_by_id(self, commands_ids):
        """
        Deletes the commands specified in a list of IDs
        """
        # get logger
        tmpLog = core_utils.make_logger(_logger)
        try:
            # sql to delete a specific command
            sql = """
                  DELETE FROM {0}
                  WHERE command_id=:command_id""".format(commandTableName)

            for command_id in commands_ids:
                var_map = {':command_id': command_id}
                self.execute(sql, var_map)
            self.commit()
            return True
        except:
            self.rollback()
            core_utils.dump_error_message(tmpLog)
            return False

    def clean_processed_commands(self):
        """
        Deletes the commands that have been processed and do not need acknowledgement
        """
        tmpLog = core_utils.make_logger(_logger)
        try:
            # sql to delete all processed commands that do not need an ACK
            sql = """
                  DELETE FROM {0}
                  WHERE (ack_requested=0 AND processed=1)
                  """.format(commandTableName)
            self.execute(sql)
            self.commit()
            return True
        except:
            self.rollback()
            core_utils.dump_error_message(tmpLog)
            return False

    # get workers to kill
    def get_workers_to_kill(self, max_workers, check_interval):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger)
            tmpLog.debug('start')
            # sql to get worker IDs
            sqlW = "SELECT workerID,status FROM {0} ".format(workTableName)
            sqlW += "WHERE killTime IS NOT NULL AND killTime<:checkTimeLimit "
            sqlW += "ORDER BY killTime LIMIT {0} ".format(max_workers)
            sqlW += "FOR UPDATE "
            # sql to lock or release worker
            sqlL = "UPDATE {0} SET killTime=:setTime ".format(workTableName)
            sqlL += "WHERE workerID=:workerID "
            # sql to get workers
            sqlG = "SELECT {0} FROM {1} ".format(WorkSpec.column_names(), workTableName)
            sqlG += "WHERE workerID=:workerID "
            timeNow = datetime.datetime.utcnow()
            # get workerIDs
            varMap = dict()
            varMap[':checkTimeLimit'] = timeNow - datetime.timedelta(seconds=check_interval)
            self.execute(sqlW, varMap)
            resW = self.cur.fetchall()
            retVal = dict()
            for workerID, workerStatus in resW:
                # lock or release worker
                varMap = dict()
                varMap[':workerID'] = workerID
                if workerStatus in (WorkSpec.ST_cancelled, WorkSpec.ST_failed, WorkSpec.ST_finished):
                    # release
                    varMap[':setTime'] = None
                else:
                    # lock worker with N sec offset for time-based locking
                    varMap[':setTime'] = timeNow + datetime.timedelta(seconds=5)
                self.execute(sqlL, varMap)
                # get worker
                nRow = self.cur.rowcount
                if nRow == 1 and varMap[':setTime'] is not None:
                    varMap = dict()
                    varMap[':workerID'] = workerID
                    self.execute(sqlG, varMap)
                    resG = self.cur.fetchone()
                    workSpec = WorkSpec()
                    workSpec.pack(resG)
                    queueName = workSpec.computingSite
                    if queueName not in retVal:
                        retVal[queueName] = []
                    retVal[queueName].append(workSpec)
            # commit
            self.commit()
            tmpLog.debug('got {0} workers'.format(len(retVal)))
            return retVal
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return {}

    # send kill command to worker associated to a job
    def kill_workers_with_job(self, panda_id):
        try:
            # get logger
            tmpLog = core_utils.make_logger(_logger)
            tmpLog.debug('start')
            # sql to set killTime
            sqlL = "UPDATE {0} SET killTime=:setTime ".format(workTableName)
            sqlL += "WHERE workerID=:workerID AND killTime IS NULL AND NOT status IN (:st1,:st2,:st3) "
            # sql to get associated workers
            sqlA = "SELECT workerID FROM {0} ".format(jobWorkerTableName)
            sqlA += "WHERE PandaID=:pandaID "
            # set an older time to trigger sweeper
            setTime = datetime.datetime.utcnow() - datetime.timedelta(hours=6)
            # get workers
            varMap = dict()
            varMap[':pandaID'] = panda_id
            self.execute(sqlA, varMap)
            resA = self.cur.fetchall()
            nRow = 0
            for workerID, in resA:
                # set killTime
                varMap = dict()
                varMap[':workerID'] = workerID
                varMap[':setTime'] = setTime
                varMap[':st1'] = WorkSpec.ST_finished
                varMap[':st2'] = WorkSpec.ST_failed
                varMap[':st3'] = WorkSpec.ST_cancelled
                self.execute(sqlL, varMap)
                nRow += self.cur.rowcount
            # commit
            self.commit()
            tmpLog.debug('set killTime to {0} workers'.format(nRow))
            return nRow
        except:
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
            tmpLog = core_utils.make_logger(_logger)
            tmpLog.debug('start')
            # sql to get worker IDs
            timeNow = datetime.datetime.utcnow()
            varMap = dict()
            varMap[':timeLimit'] = timeNow - datetime.timedelta(minutes=60)
            sqlW = "SELECT workerID FROM {0} ".format(workTableName)
            sqlW += "WHERE lastUpdate IS NULL AND ("
            for tmpStatus, tmpTimeout in status_timeout_map.iteritems():
                tmpStatusKey = ':status_{0}'.format(tmpStatus)
                tmpTimeoutKey = ':timeLimit_{0}'.format(tmpStatus)
                sqlW += '(status={0} AND endTime<={1}) OR '.format(tmpStatusKey, tmpTimeoutKey)
                varMap[tmpStatusKey] = tmpStatus
                varMap[tmpTimeoutKey] = timeNow - datetime.timedelta(hours=tmpTimeout)
            sqlW = sqlW[:-4]
            sqlW += ') '
            sqlW += 'AND modificationTime<:timeLimit '
            sqlW += "ORDER BY modificationTime LIMIT {0} ".format(max_workers)
            sqlW += "FOR UPDATE "
            # sql to lock or release worker
            sqlL = "UPDATE {0} SET modificationTime=:setTime ".format(workTableName)
            sqlL += "WHERE workerID=:workerID "
            # sql to check associated jobs
            sqlA = "SELECT COUNT(*) FROM {0} j, {1} r ".format(jobTableName, jobWorkerTableName)
            sqlA += "WHERE j.PandaID=r.PandaID AND r.workerID=:workerID "
            sqlA += "AND propagatorTime IS NOT NULL "
            # sql to get workers
            sqlG = "SELECT {0} FROM {1} ".format(WorkSpec.column_names(), workTableName)
            sqlG += "WHERE workerID=:workerID "
            # get workerIDs
            timeNow = datetime.datetime.utcnow()
            self.execute(sqlW, varMap)
            resW = self.cur.fetchall()
            retVal = dict()
            iWorkers = 0
            for workerID, in resW:
                # lock worker with N sec offset for time-based locking
                varMap = dict()
                varMap[':workerID'] = workerID
                varMap[':setTime'] = timeNow + datetime.timedelta(seconds=5)
                self.execute(sqlL, varMap)
                # check associated jobs
                varMap = dict()
                varMap[':workerID'] = workerID
                self.execute(sqlA, varMap)
                nActJobs, = self.cur.fetchone()
                if nActJobs == 0:
                    # get worker
                    varMap = dict()
                    varMap[':workerID'] = workerID
                    self.execute(sqlG, varMap)
                    resG = self.cur.fetchone()
                    workSpec = WorkSpec()
                    workSpec.pack(resG)
                    queueName = workSpec.computingSite
                    if queueName not in retVal:
                        retVal[queueName] = []
                    retVal[queueName].append(workSpec)
                    iWorkers += 1
            # commit
            self.commit()
            tmpLog.debug('got {0} workers'.format(iWorkers))
            return retVal
        except:
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
            tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(worker_id))
            tmpLog.debug('start')
            # sql to get jobs
            sqlJ = "SELECT PandaID FROM {0} ".format(jobWorkerTableName)
            sqlJ += "WHERE workerID=:workerID "
            # sql to delete job
            sqlDJ = "DELETE FROM {0} ".format(jobTableName)
            sqlDJ += "WHERE PandaID=:PandaID "
            # sql to delete relations
            sqlDR = "DELETE FROM {0} ".format(jobWorkerTableName)
            sqlDR += "WHERE PandaID=:PandaID "
            # sql to delete worker
            sqlDW = "DELETE FROM {0} ".format(workTableName)
            sqlDW += "WHERE workerID=:workerID "
            # get jobs
            varMap = dict()
            varMap[':workerID'] = worker_id
            self.execute(sqlJ, varMap)
            resJ = self.cur.fetchall()
            for pandaID, in resJ:
                varMap = dict()
                varMap[':PandaID'] = pandaID
                # delete job
                self.execute(sqlDJ, varMap)
                # delete relations
                self.execute(sqlDR, varMap)
            # delete worker
            varMap = dict()
            varMap[':workerID'] = worker_id
            self.execute(sqlDW, varMap)
            # commit
            self.commit()
            tmpLog.debug('done')
            return True
        except:
            # roll back
            self.rollback()
            # dump error
            core_utils.dump_error_message(_logger)
            # return
            return False
