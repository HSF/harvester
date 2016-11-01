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
    def makeWorkers(self, jobchunk_list, queue_config, n_ready):
        try:
            tmpLog = CoreUtils.makeLogger(_logger, 'queue={0}'.format(queue_config.queueName))
            tmpLog.debug('start')
            # get plugin
            maker = self.pluginFactory.getPlugin(queue_config.workMaker)
            if maker is None:
                # not found
                tmpLog.error('plugin for {0} not found'.format(queue_config.queueName))
                return [], jobchunk_list
            # get ready workers
            readyWorkers = self.dbProxy.getReadyWorkers(queue_config.queueName, n_ready)
            # loop over all chunks
            okChunks = []
            ngChunks = []
            for iChunk, jobChunk in enumerate(jobchunk_list):
                # make a worker
                if iChunk >= n_ready:
                    workSpec = maker.makeWorker(jobChunk, queue_config)
                else:
                    # use ready worker
                    if iChunk < len(readyWorkers):
                        workSpec = readyWorkers[iChunk]
                    else:
                        workSpec = None
                # failed
                if workSpec is None:
                    ngChunks.append(jobChunk)
                    continue
                # set workerID
                if workSpec.workerID is None:
                    workSpec.workerID = self.dbProxy.get_next_seq_number('SEQ_workerID')
                    workSpec.isNew = True
                okChunks.append((workSpec, jobChunk))
            # dump
            tmpLog.debug('made {0} workers while {1} chunks failed'.format(len(okChunks),
                                                                           len(ngChunks)))
            return okChunks, ngChunks
        except:
            # dump error
            CoreUtils.dumpErrorMessage(tmpLog)
            return [], jobchunk_list

    # get number of jobs per worker
    def getNumJobsPerWorker(self, queue_config):
        # get plugin
        maker = self.pluginFactory.getPlugin(queue_config.workMaker)
        return maker.getNumJobsPerWorker()

    # get number of workers per job
    def getNumWorkersPerJob(self, queue_config):
        # get plugin
        maker = self.pluginFactory.getPlugin(queue_config.workMaker)
        return maker.getNumWorkersPerJob()
