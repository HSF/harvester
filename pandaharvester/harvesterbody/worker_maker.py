from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory

# logger
_logger = core_utils.setup_logger("worker_maker")


# class to make worker
class WorkerMaker:
    # constructor
    def __init__(self):
        self.pluginFactory = PluginFactory()
        self.dbProxy = DBProxy()

    # get plugin
    def get_plugin(self, queue_config):
        return self.pluginFactory.get_plugin(queue_config.workerMaker)

    # make workers
    def make_workers(self, jobchunk_list, queue_config, n_ready, job_type, resource_type, prod_source_label="ANY", maker=None):
        tmpLog = core_utils.make_logger(
            _logger, f"queue={queue_config.queueName} jtype={job_type} rtype={resource_type} pslabel={prod_source_label}", method_name="make_workers"
        )
        tmpLog.debug("start")
        try:
            # get plugin
            if maker is None:
                maker = self.get_plugin(queue_config)
            if maker is None:
                # not found
                tmpLog.error(f"plugin for {queue_config.queueName} not found")
                return [], jobchunk_list
            # get ready workers
            readyWorkers = self.dbProxy.get_ready_workers(queue_config.queueName, n_ready)
            # loop over all chunks
            okChunks = []
            ngChunks = []
            for iChunk, jobChunk in enumerate(jobchunk_list):
                # make a worker
                if iChunk >= n_ready:
                    workSpec = maker.make_worker(jobChunk, queue_config, job_type, resource_type, prod_source_label=prod_source_label)
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
                    workSpec.workerID = self.dbProxy.get_next_seq_number("SEQ_workerID")
                    workSpec.configID = queue_config.configID
                    workSpec.isNew = True
                okChunks.append((workSpec, jobChunk))
            # dump
            tmpLog.debug(f"made {len(okChunks)} workers while {len(ngChunks)} chunks failed")
            return okChunks, ngChunks
        except Exception:
            # dump error
            core_utils.dump_error_message(tmpLog)
            return [], jobchunk_list
