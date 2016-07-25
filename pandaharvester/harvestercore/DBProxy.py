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
                raise KeyError, '{0} is missing in SQL parameters sql={1} var={2}'.format(item,sql,
                                                                                          str(varMap))
            if not item in paramList:
                paramList.append(varMap[item])
        return paramList



    # wrapper for execute
    def execute(self,sql,varMap=None):
        if varMap == None:
            varMap = {}
        # get lock
        conLock.acquire()
        try:
            # convert param dict
            params = self.convertParams(sql,varMap)
            # execute
            retVal = self.cur.execute(sql,params)
        finally:
            # release lock
            conLock.release()
        # return
        return retVal



    # wrapper for executemany
    def executemany(self,sql,varMapList):
        # get lock
        conLock.acquire()
        try:
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
            conLock.release()
        # return
        return retVal



    # make table
    def makeTable(self,cls,tableName):
        try:
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
                self.con.commit()
        except:
            # roll back
            self.con.rollback()
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
            self.con.commit()
            # return
            return True
        except:
            # roll back
            self.con.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(tmpLog)
            # return
            return False



    # get job
    def getJob(self,pandaID):
        try:
            # sql to get job
            sql  = "SELECT {0} FROM {1} ".format(JobSpec.columnNames(),jobTableName)
            sql += "WHERE PandaID=:pandaID "
            # get job
            varMap = {}
            varMap[':pandaID'] = pandaID
            self.execute(sql,varMap)
            resJ = self.cur.fetchone()
            # commit
            self.con.commit()
            if resJ == None:
                return None
            # make job
            jobSpec = JobSpec()
            jobSpec.pack(resJ)
            # return
            return jobSpec
        except:
            # roll back
            self.con.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return None



    # fill panda queue table
    def fillPandaQueueTable(self,pandaQueues,queueConfigMapper):
        try:
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
            self.con.commit()
            return True
        except:
            # roll back
            self.con.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return False



    # get number of jobs to fetch
    def getNumJobsToFetch(self,nQueues,interval):
        try:
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
            self.con.commit()
            return retMap
        except:
            # roll back
            self.con.rollback()
            # dump error
            CoreUtils.dumpErrorMessage(_logger)
            # return
            return {}
