from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.DBProxy import DBProxy
from pandaharvester.harvestercore.PluginFactory import PluginFactory


# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('WorkMaker')


# class to make worker
class WorkMaker:

    # constructor
    def __init__(self):
        self.pluginFactory = PluginFactory()
        self.dbProxy = DBProxy()



    # make workers
    def makeWorkers(self,jobChunkList,queueConfig):
        try:
            tmpLog = CoreUtils.makeLogger(_logger,'queue={0}'.format(queueConfig.queueName))
            tmpLog.debug('start')
            # get plugin
            maker = self.pluginFactory.getPlugin(queueConfig.workMaker)
            if maker == None:
                # not found
                tmpLog.error('plugin for {0} not found'.format(queueConfig.queueName))
                return [],jobChunkList
            # loop over all chunks
            okChunks = []
            ngChunks = []
            for jobChunk in jobChunkList:
                # make a worker
                woker = maker.makeWorker(jobChunk,queueConfig)
                # failed
                if woker == None:
                    ngChunks.append(jobChunk)
                    continue
                okChunks.append((woker,jobChunk))
            # dump
            tmpLog.debug('made {0} workers while {1} chunks failed'.format(len(okChunks),
                                                                           len(ngChunks)))
            return okChunks,ngChunks
        except:
            # dump error
            CoreUtils.dumpErrorMessage(tmpLog)
            return [],jobChunkList


    
    # get number of jobs per worker
    def getNumJobsPerWorker(self,queueConfig):
        # get plugin
        maker = self.pluginFactory.getPlugin(queueConfig.workMaker)
        return maker.getNumJobsPerWorker()



    # get number of workers per job
    def getNumWorkersPerJob(self,queueConfig):
        # get plugin
        maker = self.pluginFactory.getPlugin(queueConfig.workMaker)
        return maker.getNumWorkersPerJob()

