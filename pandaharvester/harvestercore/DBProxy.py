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
from PandaQueueSpec import PandaQueueSpec

import CoreUtils
from pandaharvester.harvesterconfig import harvester_config

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('DBProxy')


# table names
jobTableName        = 'job_table'
workTableName       = 'work_table'
pandaQueueTableName = 'pq_table'


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
                self.verbLog.debug('sql={0} var={1}'.format(sql,str(varMap)))
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
                self.verbLog.debug('sql={0} var={1}'.format(sql,str(varMap)))
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
        self.makeTable(PandaQueueSpec,pandaQueueTableName)
        # fill PandaQueue table
        self.fillPandaQueueTable(harvester_config.qconf.queueList,queueConfigMapper)



    # insert jobs
    def insertJobs(self,jobSpecs):
        try:
            # get logger
            tmpLog = CoreUtils.makeLogger(_logger)
            tmpLog.debug('{0} jobs'.format(len(jobSpecs)))
            # sql to insert job
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
            # sql to get job
            sql  = "INSERT INTO {0} ".format(pandaQueueTableName)
            sql += "(queueName,nQueueLimit) VALUES (:queueName,:nQueueLimit) "
            # insert queues
            for queueName in pandaQueues:
                queueConfig = queueConfigMapper.getQueue(queueName)
                if queueConfig != None:
                    varMap = {}
                    varMap[':queueName'] = queueName
                    varMap[':nQueueLimit'] = queueConfig.nQueueLimit
                    self.execute(sql,varMap)
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
            sqlQ  = "SELECT queueName,nQueueLimit FROM {0} ".format(pandaQueueTableName)
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



