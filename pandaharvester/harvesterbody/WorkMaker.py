from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.DBProxy import DBProxy
from pandaharvester.harvestercore.PluginFactory import PluginFactory

# logger
_logger = CoreUtils.setupLogger()



# class to make worker
class WorkMaker:

    # constructor
    def __init__(self):
        self.pluginFactory = PluginFactory()
        self.dbProxy = DBProxy()



    # make workers
    def makeWorkers(self,jobChunkList,queueConfig,nReady):
        try:
            tmpLog = CoreUtils.makeLogger(_logger,'queue={0}'.format(queueConfig.queueName))
            tmpLog.debug('start')
            # get plugin
            maker = self.pluginFactory.getPlugin(queueConfig.workMaker)
            if maker == None:
                # not found
                tmpLog.error('plugin for {0} not found'.format(queueConfig.queueName))
                return [],jobChunkList
            # get ready workers
            readyWorkers = self.dbProxy.getReadyWorkers(queueConfig.queueName,nReady)
            # loop over all chunks
            okChunks = []
            ngChunks = []
            for iChunk,jobChunk in enumerate(jobChunkList):
                # make a worker
                if iChunk >= nReady:
                    workSpec = maker.makeWorker(jobChunk,queueConfig)
                else:
                    # use ready worker
                    if iChunk < len(readyWorkers):
                        workSpec = readyWorkers[iChunk]
                    else:
                        workSpec = None
                # failed
                if workSpec == None:
                    ngChunks.append(jobChunk)
                    continue
                # set workerID
                if workSpec.workerID is None:
                    workSpec.workerID = self.dbProxy.get_next_seq_number('SEQ_workerID')
                    workSpec.isNew = True
                okChunks.append((workSpec,jobChunk))
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

