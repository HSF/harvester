"""
database connection

"""

import os
import re
import sys
import types
import inspect
import datetime
import traceback
import threading

from JobSpec import JobSpec
from WorkSpec import WorkSpec
from PandaQueueSpec import PandaQueueSpec
from JobWorkerRelationSpec import JobWorkerRelationSpec

import CoreUtils
from pandaharvester.harvesterconfig import harvester_config

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('DBProxy')


# table names
jobTableName        = 'job_table'
workTableName       = 'work_table'
pandaQueueTableName = 'pq_table'
jobWorkerTableName  = 'jw_table'


# connection lock
conLock = threading.Lock()



# connection class
class DBProxy:

    # constructor
    def __init__(self):
        import sqlite3
        self.con = sqlite3.connect(harvester_config.db.database_filename,
                                   detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES,
                                   check_same_thread=False)
        self.con.row_factory = sqlite3.Row
        self.cur = self.con.cursor()
        self.lockDB = False
        self.verbLog = None



    # convert param dict to list
    def convertParams(self,sql,varMap):
        # no conversation unless dict
        if not isinstance(varMap,types.DictType):
            return varMap
        paramList = []
        # extract placeholders
        items = re.findall(':[^ $,)]+',sql)
        for item in items:
            if not item in varMap:
                raise KeyError, '{0} is missing in SQL parameters'.format(item)
            if not item in paramList:
                paramList.append(varMap[item])
        # lock database
        if re.search('^INSERT',sql,re.I) == None and re.search('^UPDATE',sql,re.I) == None:
            self.lockDB = True
        return paramList



    # wrapper for execute
    def execute(self,sql,varMap=None):
        if varMap == None:
            varMap = {}
        # get lock
        if not self.lockDB:
            conLock.acquire()
        try:
            # verbose
            if harvester_config.db.verbose:
                if self.verbLog == None:
                    self.verbLog = CoreUtils.makeLogger(_logger)
                self.verbLog.debug('sql={0} var={1} exec={2}'.format(sql,str(varMap),
                                                                     inspect.stack()[1][3]))
            # convert param dict
            params = self.convertParams(sql,varMap)
            # execute
            retVal = self.cur.execute(sql,params)
        finally:
            # release lock
            if not self.lockDB:
                conLock.release()
        # return
        return retVal



    # wrapper for executemany
    def executemany(self,sql,varMapList):
        # get lock
        if not self.lockDB:
            conLock.acquire()
        try:
            # verbose
            if harvester_config.db.verbose:
                if self.verbLog == None:
                    self.verbLog = CoreUtils.makeLogger(_logger)
                self.verbLog.debug('sql={0} var={1}'.format(sql,str(varMapList)))
            # convert param dict
            paramList = []
            for varMap in varMapList:
                if varMap == None:
                    varMap = {}
                params = self.convertParams(sql,varMap)
                paramList.append(params)
            # execute
            retVal = self.cur.executemany(sql,paramList)
        finally:
            # release lock
            if not self.lockDB:
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
    def makeTable(self,cls,tableName):
        try:
            # get logger
            tmpLog = CoreUtils.makeLogger(_logger)
            tmpLog.debug('table={0}'.format(tableName))
            # check if table already exists
            varMap = {}
            varMap[':type'] = 'table'
            varMap[':name'] = tableName
            sqlC = 'SELECT name FROM sqlite_master WHERE type=:type AND tbl_name=:name '
            self.execute(sqlC,varMap)
            resC = self.cur.fetchone()
            # not exists
            if resC == None:
                # 
                sqlM = 'CREATE TABLE {0}('.format(tableName)
                # collect columns
                for attr in cls.attributesWithTypes:
                    # split to name and type
                    attrName,attrType = attr.split(':')
                    sqlM += '{0} {1},'.format(attrName,attrType)
                sqlM = sqlM[:-1]
                sqlM += ')'
                # make table
                self.execute(sqlM)
                # commit
                self.commit()
                tmpLog.debug('made {0}'.format(tableName))
            else:
                tmpLog.debug('reuse {0}'.format(tableName))
        except:
            # roll back
            self.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(_logger)



    # make tables
    def makeTables(self,queueConfigMapper):
        self.makeTable(JobSpec,jobTableName)
        self.makeTable(WorkSpec,workTableName)
        self.makeTable(PandaQueueSpec,pandaQueueTableName)
        self.makeTable(JobWorkerRelationSpec,jobWorkerTableName)
        # fill PandaQueue table
        self.fillPandaQueueTable(harvester_config.qconf.queueList,queueConfigMapper)



    # insert jobs
    def insertJobs(self,jobSpecs):
        try:
            # get logger
            tmpLog = CoreUtils.makeLogger(_logger)
            tmpLog.debug('{0} jobs'.format(len(jobSpecs)))
            # sql to insert a job
            sql  = "INSERT INTO {0} ({1}) ".format(jobTableName,JobSpec.columnNames())
            sql += JobSpec.bindValuesExpression()
            # loop over all jobs
            varMaps = []
            for jobSpec in jobSpecs:
               varMap = jobSpec.valuesList()
               varMaps.append(varMap)
            # insert
            self.executemany(sql,varMaps)
            # commit
            self.commit()
            # return
            return True
        except:
            # roll back
            self.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(tmpLog)
            # return
            return False



    # get job
    def getJob(self,pandaID):
        try:
            # get logger
            tmpLog = CoreUtils.makeLogger(_logger,'PandaID={0}'.format(pandaID))
            tmpLog.debug('start')
            # sql to get job
            sql  = "SELECT {0} FROM {1} ".format(JobSpec.columnNames(),jobTableName)
            sql += "WHERE PandaID=:pandaID "
            # get job
            varMap = {}
            varMap[':pandaID'] = pandaID
            self.execute(sql,varMap)
            resJ = self.cur.fetchone()
            # commit
            self.commit()
            if resJ == None:
                return None
            # make job
            jobSpec = JobSpec()
            jobSpec.pack(resJ)
            tmpLog.debug('done')
            # return
            return jobSpec
        except:
            # roll back
            self.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return None



    # update job
    def updateJob(self,jobSpec,criteria=None):
        try:
            # get logger
            tmpLog = CoreUtils.makeLogger(_logger,'PandaID={0} subStatus={1}'.format(jobSpec.PandaID,
                                                                                     jobSpec.subStatus))
            tmpLog.debug('start')
            if criteria == None:
                criteria = {}
            # sql to update job
            sql  = "UPDATE {0} SET {1} ".format(jobTableName,jobSpec.bindUpdateChangesExpression())
            sql += "WHERE PandaID=:PandaID "
            # update job
            varMap = jobSpec.valuesMap(onlyChanged=True)
            for tmpKey,tmpVal in criteria.iteritems():
                mapKey = ':{0}_cr'.format(tmpKey)
                sql += "AND {0}={1} ".format(tmpKey,mapKey)
                varMap[mapKey] = tmpVal
            varMap[':PandaID'] = jobSpec.PandaID
            self.execute(sql,varMap)
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
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return None



    # fill panda queue table
    def fillPandaQueueTable(self,pandaQueues,queueConfigMapper):
        try:
            # get logger
            tmpLog = CoreUtils.makeLogger(_logger)
            tmpLog.debug('start')
            # loop over queues
            for queueName in pandaQueues:
                queueConfig = queueConfigMapper.getQueue(queueName)
                if queueConfig != None:
                    varMap = {}
                    varMap[':queueName'] = queueName
                    # sql to insert queue
                    sqlP = "INSERT INTO {0} (".format(pandaQueueTableName)
                    sqlS = "VALUES ("
                    for attrName in PandaQueueSpec.columnNames().split(','):
                        if hasattr(queueConfig,attrName):
                            tmpKey = ':{0}'.format(attrName)
                            sqlP += '{0},'.format(attrName)
                            sqlS += '{0},'.format(tmpKey)
                            varMap[tmpKey] = getattr(queueConfig,attrName)
                    sqlP = sqlP[:-1]
                    sqlS = sqlS[:-1]
                    sqlP += ') '
                    sqlS += ') '
                    self.execute(sqlP+sqlS,varMap)
            # commit
            self.commit()
            tmpLog.debug('done')
            # return
            return True
        except:
            # roll back
            self.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return False



    # get number of jobs to fetch
    def getNumJobsToFetch(self,nQueues,interval):
        try:
            # get logger
            tmpLog = CoreUtils.makeLogger(_logger)
            tmpLog.debug('start')
            retMap = {}
            # sql to get queues
            sqlQ  = "SELECT queueName,nQueueLimitJob FROM {0} ".format(pandaQueueTableName)
            sqlQ += "WHERE jobFetchTime IS NULL OR jobFetchTime<:timeLimit "
            sqlQ += "ORDER BY jobFetchTime "
            # sql to count nQueue
            sqlN  = "SELECT count(*) FROM {0} ".format(jobTableName)
            sqlN += "WHERE computingSite=:computingSite AND status=:status "
            # sql to update timestamp
            sqlU  = "UPDATE {0} SET jobFetchTime=:jobFetchTime ".format(pandaQueueTableName)
            sqlU += "WHERE queueName=:queueName "
            # get queues
            timeNow = datetime.datetime.utcnow()
            varMap = {}
            varMap[':timeLimit'] = timeNow - datetime.timedelta(seconds=interval)
            self.execute(sqlQ,varMap)
            resQ = self.cur.fetchall()
            for queueName,nQueueLimit in resQ:
                # count nQueue
                varMap = {}
                varMap[':computingSite'] = queueName
                varMap[':status'] = 'starting'
                self.execute(sqlN,varMap)
                nQueue, = self.cur.fetchone()
                # more jobs need to be queued
                if nQueue < nQueueLimit:
                    retMap[queueName] = nQueueLimit - nQueue
                # update timestamp
                varMap = {}
                varMap[':queueName'] = queueName
                varMap[':jobFetchTime'] = timeNow
                self.execute(sqlU,varMap)
                # enough queues
                if len(retMap) >= nQueues:
                    break
            # commit
            self.commit()
            tmpLog.debug('got {0}'.format(str(retMap)))
            return retMap
        except:
            # roll back
            self.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return {}



    # get jobs to propagate checkpoints
    def getJobsToPropagate(self,maxJobs,lockInterval,updateInterval,lockedBy):
        try:
            # get logger
            tmpLog = CoreUtils.makeLogger(_logger,'thr={0}'.format(lockedBy))
            tmpLog.debug('start')
            # sql to get jobs
            sql  = "SELECT {0} FROM {1} ".format(JobSpec.columnNames(),jobTableName)
            sql += "WHERE propagatorTime IS NOT NULL "
            sql += "AND ((propagatorTime<:lockTimeLimit AND propagatorLock IS NOT NULL) "
            sql += "OR propagatorTime<:updateTimeLimit) "
            sql += "ORDER BY propagatorTime LIMIT {0} ".format(maxJobs)
            # sql to lock job
            sqlL  = "UPDATE {0} SET propagatorTime=:timeNow,propagatorLock=:lockedBy ".format(jobTableName)
            sqlL += "WHERE PandaID=:PandaID "
            # get jobs
            timeNow = datetime.datetime.utcnow()
            varMap = {}
            varMap[':lockTimeLimit']   = timeNow - datetime.timedelta(seconds=lockInterval)
            varMap[':updateTimeLimit'] = timeNow - datetime.timedelta(seconds=updateInterval)
            self.execute(sql,varMap)
            resList = self.cur.fetchall()
            jobSpecList  = []
            for res in resList:
                # make job
                jobSpec = JobSpec()
                jobSpec.pack(res)
                # lock job
                varMap = {}
                varMap[':PandaID']  = jobSpec.PandaID
                varMap[':timeNow']  = timeNow
                varMap[':lockedBy'] = lockedBy
                self.execute(sqlL,varMap)
                nRow = self.cur.rowcount
                if nRow > 0:
                    jobSpec.propagatorLock = lockedBy
                    jobSpecList.append(jobSpec)
            # commit
            self.commit()
            tmpLog.debug('got {0} jobs'.format(len(jobSpecList)))
            return jobSpecList
        except:
            # roll back
            self.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return []



    # get jobs in substatus
    def getJobsInSubStatus(self,subStatus,maxJobs,timeColumn=None,lockColumn=None,intervalWithLock=None,
                           intervalWoLock=None,lockedBy=None,newSubStatus=None):
        try:
            # get logger
            if lockedBy == None:
                msgPfx = None
            else:
                msgPfx = 'thr={0}'.format(lockedBy)
            tmpLog = CoreUtils.makeLogger(_logger,msgPfx)
            tmpLog.debug('start subStatus={0} timeColumn={1}'.format(subStatus,timeColumn))
            # sql to count jobs beeing processed
            sqlC  = "SELECT COUNT(*) FROM {0} ".format(jobTableName)
            sqlC += "WHERE ({0} IS NOT NULL AND subStatus=:subStatus) ".format(lockColumn)
            sqlC += "OR subStatus=:newSubStatus "
            # count jobs
            if maxJobs > 0 and newSubStatus != None:
                varMap = {}
                varMap[':subStatus'] = subStatus
                varMap[':newSubStatus'] = newSubStatus
                self.execute(sqlC,varMap)
                nProcessing, = self.cur.fetchone()
                if nProcessing >= maxJobs:
                    # commit
                    self.commit()
                    tmpLog.debug('enough jobs {0} are beeing processed'.format(len(nProcessing)))
                    return []
                maxJobs -= nProcessing
            # sql to get jobs
            sql  = "SELECT {0} FROM {1} ".format(JobSpec.columnNames(),jobTableName)
            sql += "WHERE subStatus=:subStatus "
            if timeColumn != None:
                sql += "AND ({0} IS NULL ".format(timeColumn)
                if intervalWithLock != None:
                    sql += "OR ({0}<:lockTimeLimit AND {1} IS NOT NULL) ".format(timeColumn,lockColumn)
                if intervalWoLock != None:
                    sql += "OR {0}<:updateTimeLimit ".format(timeColumn)
                sql += ') '
                sql += "ORDER BY {0} ".format(timeColumn)
            sql += "LIMIT {0} ".format(maxJobs)
            # sql to lock job
            sqlL  = "UPDATE {0} SET {1}=:timeNow,{2}=:lockedBy ".format(jobTableName,timeColumn,lockColumn)
            sqlL += "WHERE PandaID=:PandaID "
            # get jobs
            timeNow = datetime.datetime.utcnow()
            varMap = {}
            varMap[':subStatus'] = subStatus
            if intervalWithLock != None:
                varMap[':lockTimeLimit']   = timeNow - datetime.timedelta(seconds=intervalWithLock)
            if intervalWoLock != None:
                varMap[':updateTimeLimit'] = timeNow - datetime.timedelta(seconds=intervalWoLock)
            self.execute(sql,varMap)
            resList = self.cur.fetchall()
            jobSpecList  = []
            for res in resList:
                # make job
                jobSpec = JobSpec()
                jobSpec.pack(res)
                # lock job
                if lockedBy != None:
                    varMap = {}
                    varMap[':PandaID']  = jobSpec.PandaID
                    varMap[':timeNow']  = timeNow
                    varMap[':lockedBy'] = lockedBy
                    self.execute(sqlL,varMap)
                    nRow = self.cur.rowcount
                    jobSpec.lockedBy = lockedBy
                    setattr(jobSpec,timeColumn,timeNow)
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
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return []



    # register a worker
    def registerWorker(self,workSpec,jobList,lockedBy):
        try:
            tmpLog = CoreUtils.makeLogger(_logger,'batchID={0}'.format(workSpec.batchID))
            tmpLog.debug('start')
            retMap = {}
            # sql to insert a worker
            sqlI  = "INSERT INTO {0} ({1}) ".format(workTableName,WorkSpec.columnNames())
            sqlI += WorkSpec.bindValuesExpression()
            # sql to update a worker
            sqlU  = "UPDATE {0} SET {1} ".format(workTableName,workSpec.bindUpdateChangesExpression())
            sqlU += "WHERE workerID=:workerID "
            # sql to insert job and worker relationship
            sqlR  = "INSERT INTO {0} ({1}) ".format(jobWorkerTableName,JobWorkerRelationSpec.columnNames())
            sqlR += JobWorkerRelationSpec.bindValuesExpression()
            # insert worker if new
            if workSpec.workerID == None:
                varMap = workSpec.valuesList()
                self.execute(sqlI,varMap)
            else:
                varMap = workSpec.valuesMap(onlyChanged=True)
                varMap[':workerID'] = workSpec.workerID
                self.execute(sqlU,varMap)
            # get workerID
            workSpec.workerID = self.cur.lastrowid
            # collect values to update jobs or insert job/worker mapping
            varMapsR = []
            for jobSpec in jobList:
                # sql to update job
                sqlJ  = "UPDATE {0} SET {1} ".format(jobTableName,jobSpec.bindUpdateChangesExpression())
                sqlJ += "WHERE PandaID=:cr_PandaID AND lockedBy=:cr_lockedBy "
                # update job
                varMap = jobSpec.valuesMap(onlyChanged=True)
                varMap[':cr_PandaID'] = jobSpec.PandaID
                varMap[':cr_lockedBy'] = lockedBy
                self.execute(sqlJ,varMap)
                if jobSpec.subStatus == 'submitted':
                    # values for job/worker mapping
                    jwRelation = JobWorkerRelationSpec()
                    jwRelation.PandaID = jobSpec.PandaID
                    jwRelation.workerID = workSpec.workerID
                    varMap = jwRelation.valuesList()
                    varMapsR.append(varMap)
            # insert job/worker mapping
            if len(varMapsR) > 0:
                self.executemany(sqlR,varMapsR)
            # commit
            self.commit()
            # return
            return True
        except:
            # roll back
            self.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return False



    # get queues to submit workers
    def getQueuesToSubmit(self,nQueues,interval):
        try:
            # get logger
            tmpLog = CoreUtils.makeLogger(_logger)
            tmpLog.debug('start')
            retMap = {}
            # sql to get queues
            sqlQ  = "SELECT queueName,nQueueLimitWorker,maxWorkers FROM {0} ".format(pandaQueueTableName)
            sqlQ += "WHERE submitTime IS NULL OR submitTime<:timeLimit "
            sqlQ += "ORDER BY submitTime "
            # sql to count nQueue
            sqlN  = "SELECT status,count(*) FROM {0} ".format(workTableName)
            sqlN += "WHERE computingSite=:computingSite GROUP BY status "
            # sql to update timestamp
            sqlU  = "UPDATE {0} SET submitTime=:submitTime ".format(pandaQueueTableName)
            sqlU += "WHERE queueName=:queueName "
            # get queues
            timeNow = datetime.datetime.utcnow()
            varMap = {}
            varMap[':timeLimit'] = timeNow - datetime.timedelta(seconds=interval)
            self.execute(sqlQ,varMap)
            resQ = self.cur.fetchall()
            for queueName,nQueueLimit,maxWorkers in resQ:
                # count nQueue
                varMap = {}
                varMap[':computingSite'] = queueName
                self.execute(sqlN,varMap)
                nQueue = 0
                nReady = 0
                nRunning = 0
                for workerStatus,nQueue in self.cur.fetchall():
                    if workerStatus in [WorkSpec.ST_submitted]:
                        nQueue += 1
                    elif workerStatus in [WorkSpec.ST_ready]:
                        nReady += 1
                    elif workerStatus in [WorkSpec.ST_running]:
                        nRunning += 1
                # include ready workers
                nWorkers = nReady
                # new workers
                if nQueueLimit > 0 and nQueue >= nQueueLimit:
                    # only ready workers since enough queued workers are there
                    pass
                elif maxWorkers > 0 and nQueue+nReady+nRunning >= maxWorkers:
                    # only ready workers since enough workers are there
                    pass
                else:
                    # add new workers
                    nWorkers += min(max(nQueueLimit-nQueue,0),max(maxWorkers-nQueue+nReady+nRunning,0))
                # add
                retMap[queueName] = {'nWorkers':nWorkers,
                                     'nReady':nReady}
                # update timestamp
                varMap = {}
                varMap[':queueName'] = queueName
                varMap[':submitTime'] = timeNow
                self.execute(sqlU,varMap)
                # enough queues
                if len(retMap) >= nQueues:
                    break
            # commit
            self.commit()
            tmpLog.debug('got {0}'.format(str(retMap)))
            return retMap
        except:
            # roll back
            self.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return {}




    # get job chunks to make workers
    def getJobChunksForWorkers(self,queueName,nWorkers,nReady,nJobsPerWorker,nWorkersPerJob,useJobLateBinding,
                               checkInterval,lockInterval,lockedBy):
        try:
            # get logger
            tmpLog = CoreUtils.makeLogger(_logger,'queue={0}'.format(queueName))
            tmpLog.debug('start')
            # define maxJobs
            if nJobsPerWorker != None:
                maxJobs = (nWorkers+nReady) * nJobsPerWorker
            else:
                maxJobs = -(-(nWorkers+nReady) // nWorkersPerJob)
            # sql to get jobs
            sql  = "SELECT {0} FROM {1} ".format(JobSpec.columnNames(),jobTableName)
            sql += "WHERE subStatus IN (:subStatus1,:subStatus2) "
            sql += "AND (submitterTime IS NULL "
            sql += "OR ((submitterTime<:lockTimeLimit AND lockedBy IS NOT NULL) "
            sql += "OR submitterTime<:checkTimeLimit)) "
            sql += "AND computingSite=:queueName "
            sql += "ORDER BY currentPriority DESC,taskID,PandaID LIMIT {0} ".format(maxJobs)
            # sql to lock job
            sqlL  = "UPDATE {0} SET submitterTime=:timeNow,lockedBy=:lockedBy ".format(jobTableName)
            sqlL += "WHERE PandaID=:PandaID "
            # get jobs
            timeNow = datetime.datetime.utcnow()
            varMap = {}
            varMap[':subStatus1'] = 'prepared'
            varMap[':subStatus2'] = 'queued'
            varMap[':queueName'] = queueName
            varMap[':lockTimeLimit']  = timeNow - datetime.timedelta(seconds=lockInterval)
            varMap[':checkTimeLimit'] = timeNow - datetime.timedelta(seconds=checkInterval)
            self.execute(sql,varMap)
            resList = self.cur.fetchall()
            jobChunkList  = []
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
                if len(jobChunkList) >= nReady and jobSpec.subStatus == 'queued':
                    continue
                jobChunk.append(jobSpec)
                # enough jobs in chunk
                if nJobsPerWorker != None and len(jobChunk) >= nJobsPerWorker:
                    jobChunkList.append(jobChunk)
                    jobChunk = []
                # one job per multiple workers
                elif nWorkersPerJob != None:
                    for i in range(nWorkersPerJob):
                        jobChunkList.append(jobChunk)
                    jobChunk = []
                # lock job
                varMap = {}
                varMap[':PandaID']  = jobSpec.PandaID
                varMap[':timeNow']  = timeNow
                varMap[':lockedBy'] = lockedBy
                self.execute(sqlL,varMap)
                nRow = self.cur.rowcount
                jobSpec.lockedBy = lockedBy
                if useJobLateBinding and len(jobChunkList) >= nReady:
                    jobSpec.subStatus = 'queued'
                else:
                    jobSpec.subStatus = 'submitted'
                # enough job chunks
                if len(jobChunkList) >= nWorkers:
                    break
            # commit
            self.commit()
            tmpLog.debug('got {0} job chunks'.format(len(jobChunkList)))
            return jobChunkList
        except:
            # roll back
            self.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return []



    # get queues to submit workers
    def getWorkersToUpdate(self,maxWorkers,checkInterval,lockInterval,lockedBy):
        try:
            # get logger
            tmpLog = CoreUtils.makeLogger(_logger)
            tmpLog.debug('start')
            # sql to get workers
            sqlW  = "SELECT workerID FROM {0} ".format(workTableName)
            sqlW += "WHERE status IN (:st_submitted,:st_running) "
            sqlW += "AND ((modificationTime<:lockTimeLimit AND lockedBy IS NOT NULL) "
            sqlW += "OR modificationTime<:checkTimeLimit) "
            sqlW += "ORDER BY modificationTime LIMIT {0} ".format(maxWorkers)
            # sql to lock worker
            sqlL  = "UPDATE {0} SET modificationTime=:timeNow,lockedBy=:lockedBy ".format(workTableName)
            sqlL += "WHERE workerID=:workerID "
            # sql to get associated workerIDs
            sqlA  = "SELECT t.workerID FROM {0} t, {0} s ".format(jobWorkerTableName)
            sqlA += "WHERE s.PandaID=t.PandaID AND s.workerID=:workerID "
            # sql to get associated workers
            sqlG  = "SELECT {0} FROM {1} ".format(WorkSpec.columnNames(),workTableName)
            sqlG += "WHERE workerID=:workerID "
            # get workerIDs
            timeNow = datetime.datetime.utcnow()
            varMap = {}
            varMap[':st_submitted'] = WorkSpec.ST_submitted
            varMap[':st_running']   = WorkSpec.ST_running
            varMap[':lockTimeLimit']  = timeNow - datetime.timedelta(seconds=lockInterval)
            varMap[':checkTimeLimit'] = timeNow - datetime.timedelta(seconds=checkInterval)
            self.execute(sqlW,varMap)
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
                varMap = {}
                varMap[':workerID'] = workerID
                self.execute(sqlA,varMap)
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
                    varMap = {}
                    varMap[':workerID'] = tmpWorkID
                    self.execute(sqlG,varMap)
                    resG =  self.cur.fetchone()
                    workSpec = WorkSpec()
                    workSpec.pack(resG)
                    if queueName == None:
                        queueName = workSpec.computingSite
                    workersList.append(workSpec)
                    # lock worker
                    varMap = {}
                    varMap[':workerID'] = tmpWorkID
                    varMap[':lockedBy'] = lockedBy
                    varMap[':timeNow'] = timeNow
                    self.execute(sqlL,varMap)
                # add
                if queueName != None:
                    if not queueName in retVal:
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
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return {}




    # update jobs and workers
    def updateJobsWorkers(self,jobSpecs,workSpecs,lockedBy):
        try:
            timeNow = datetime.datetime.utcnow()
            # update job
            if jobSpecs != None:
                for jobSpec in jobSpecs:
                    tmpLog = CoreUtils.makeLogger(_logger,'PandaID={0}'.format(jobSpec.PandaID))
                    tmpLog.debug('updating')
                    # sql to update job
                    sqlJ  = "UPDATE {0} SET {1} ".format(jobTableName,jobSpec.bindUpdateChangesExpression())
                    sqlJ += "WHERE PandaID=:PandaID AND lockedBy=:cr_lockedBy "
                    jobSpec.lockedBy = None
                    jobSpec.modificationTime = timeNow
                    varMap = jobSpec.valuesMap(onlyChanged=True)
                    varMap[':PandaID'] = jobSpec.PandaID
                    varMap[':cr_lockedBy'] = lockedBy
                    self.execute(sqlJ,varMap)
                    nRow = self.cur.rowcount
                    tmpLog.debug('done with {0}'.format(nRow))
            # update worker
            for workSpec in workSpecs:
                tmpLog = CoreUtils.makeLogger(_logger,'workerID={0}'.format(workSpec.workerID))
                tmpLog.debug('update')
                # sql to update worker
                sqlW  = "UPDATE {0} SET {1} ".format(workTableName,workSpec.bindUpdateChangesExpression())
                sqlW += "WHERE workerID=:workerID AND lockedBy=:cr_lockedBy "
                workSpec.lockedBy = None
                workSpec.modificationTime = timeNow
                varMap = workSpec.valuesMap(onlyChanged=True)
                varMap[':workerID'] = workSpec.workerID
                varMap[':cr_lockedBy'] = lockedBy
                self.execute(sqlW,varMap)
                nRow = self.cur.rowcount
                tmpLog.debug('done with {0}'.format(nRow))
            # commit
            self.commit()
            # return
            return True
        except:
            # roll back
            self.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return False



    # get jobs with workerID
    def getJobsWithWorkerID(self,workerID,lockedBy):
        try:
            # get logger
            tmpLog = CoreUtils.makeLogger(_logger,'workerID={0}'.format(workerID))
            tmpLog.debug('start')
            # sql to get PandaIDs
            sqlP  = "SELECT PandaID FROM {0} ".format(jobWorkerTableName)
            sqlP += "WHERE workerID=:workerID "
            # sql to get jobs
            sqlJ  = "SELECT {0} FROM {1} ".format(JobSpec.columnNames(),jobTableName)
            sqlJ += "WHERE PandaID=:PandaID "
            # sql to lock job
            sqlL  = "UPDATE {0} SET modificationTime=:timeNow,lockedBy=:lockedBy ".format(jobTableName)
            sqlL += "WHERE PandaID=:PandaID "
            # get jobs
            jobChunkList = []
            timeNow = datetime.datetime.utcnow()
            varMap = {}
            varMap[':workerID'] = workerID
            self.execute(sqlP,varMap)
            resW = self.cur.fetchall()
            for pandaID, in resW:
                # get job
                varMap = {}
                varMap[':PandaID'] = pandaID
                self.execute(sqlJ,varMap)
                resJ = self.cur.fetchone()
                # make job
                jobSpec = JobSpec()
                jobSpec.pack(resJ)
                jobSpec.lockedBy = lockedBy
                # lock job
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':lockedBy'] = lockedBy
                varMap[':timeNow'] = timeNow
                self.execute(sqlL,varMap)
                jobChunkList.append(jobSpec)
            # commit
            self.commit()
            tmpLog.debug('got {0} job chunks'.format(len(jobChunkList)))
            return jobChunkList
        except:
            # roll back
            self.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return []




    # get ready workers
    def getReadyWorkers(self,queueName,nReady):
        try:
            # get logger
            tmpLog = CoreUtils.makeLogger(_logger,'queue={0}'.format(queueName))
            tmpLog.debug('start')
            # sql to get workers
            sqlG  = "SELECT {0} FROM {1} ".format(WorkSpec.columnNames(),workTableName)
            sqlG += "WHERE status=:status AND computingSite=:queueName "
            sqlG += "ORDER BY modificationTime LIMIT {0} ".format(nReady)
            # get workers
            timeNow = datetime.datetime.utcnow()
            varMap = {}
            varMap[':status']    = WorkSpec.ST_ready
            varMap[':queueName'] = queueName
            self.execute(sqlG,varMap)
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
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return []
