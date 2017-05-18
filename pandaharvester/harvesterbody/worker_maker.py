from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory

# logger
_logger = core_utils.setup_logger()


# class to make worker
class WorkerMaker:
    # constructor
    def __init__(self):
        self.pluginFactory = PluginFactory()
        self.dbProxy = DBProxy()

    # make workers
    def make_workers(self, jobchunk_list, queue_config, n_ready):
        tmpLog = core_utils.make_logger(_logger, 'queue={0}'.format(queue_config.queueName))
        tmpLog.debug('start')
        try:
            # get plugin
            maker = self.pluginFactory.get_plugin(queue_config.workerMaker)
            if maker is None:
                # not found
                tmpLog.error('plugin for {0} not found'.format(queue_config.queueName))
                return [], jobchunk_list
            # get ready workers
            readyWorkers = self.dbProxy.get_ready_workers(queue_config.queueName, n_ready)
            # loop over all chunks
            okChunks = []
            ngChunks = []
            for iChunk, jobChunk in enumerate(jobchunk_list):
                # make a worker
                if iChunk >= n_ready:
                    workSpec = maker.make_worker(jobChunk, queue_config)
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
            core_utils.dump_error_message(tmpLog)
            return [], jobchunk_list

    # get number of jobs per worker
    def get_num_jobs_per_worker(self, queue_config):
        # get plugin
        maker = self.pluginFactory.get_plugin(queue_config.workMaker)
        return maker.get_num_jobs_per_worker()

    # get number of workers per job
    def get_num_workers_per_job(self, queue_config):
        # get plugin
        maker = self.pluginFactory.get_plugin(queue_config.workMaker)
        return maker.get_num_workers_per_job()
