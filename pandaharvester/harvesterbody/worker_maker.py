from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_factory import PluginFactory

# logger
_logger = core_utils.setup_logger("worker_maker")


# class to make worker
class WorkerMaker(object):
    # constructor
    def __init__(self):
        self.pluginFactory = PluginFactory()
        self.dbProxy = DBProxy()

    # get plugin
    def get_plugin(self, queue_config):
        return self.pluginFactory.get_plugin(queue_config.workerMaker)

    # make workers
    def make_workers(self, jobchunk_list, queue_config, n_ready, job_type, resource_type, maker=None):
        tmpLog = core_utils.make_logger(
            _logger, "queue={0} jtype={1} rtype={2}".format(queue_config.queueName, job_type, resource_type), method_name="make_workers"
        )
        tmpLog.debug("start")
        try:
            # get plugin
            if maker is None:
                maker = self.pluginFactory.get_plugin(queue_config.workerMaker)
            if maker is None:
                # not found
                tmpLog.error("plugin for {0} not found".format(queue_config.queueName))
                return [], jobchunk_list
            # get ready workers
            readyWorkers = self.dbProxy.get_ready_workers(queue_config.queueName, n_ready)
            # loop over all chunks
            okChunks = []
            ngChunks = []
            for iChunk, jobChunk in enumerate(jobchunk_list):
                # make a worker
                if iChunk >= n_ready:
                    workSpec = maker.make_worker(jobChunk, queue_config, job_type, resource_type)
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
            tmpLog.debug("made {0} workers while {1} chunks failed".format(len(okChunks), len(ngChunks)))
            return okChunks, ngChunks
        except Exception:
            # dump error
            core_utils.dump_error_message(tmpLog)
            return [], jobchunk_list

    # get number of jobs per worker
    def get_num_jobs_per_worker(self, queue_config, n_workers, job_type, resource_type, maker=None):
        # get plugin
        if maker is None:
            maker = self.pluginFactory.get_plugin(queue_config.workerMaker)
        return maker.get_num_jobs_per_worker(n_workers)

    # get number of workers per job
    def get_num_workers_per_job(self, queue_config, n_workers, job_type, resource_type, maker=None):
        # get plugin
        if maker is None:
            maker = self.pluginFactory.get_plugin(queue_config.workerMaker)
        return maker.get_num_workers_per_job(n_workers)

    # check number of ready resources
    def num_ready_resources(self, queue_config, job_type, resource_type, maker=None):
        # get plugin
        if maker is None:
            maker = self.pluginFactory.get_plugin(queue_config.workerMaker)
        return maker.num_ready_resources()

    # get upper limit on the cumulative total of workers per job
    def get_max_workers_per_job_in_total(self, queue_config, job_type, resource_type, maker=None):
        # get plugin
        if maker is None:
            maker = self.pluginFactory.get_plugin(queue_config.workerMaker)
        return maker.get_max_workers_per_job_in_total()

    # get upper limit on the number of new workers per job in a cycle
    def get_max_workers_per_job_per_cycle(self, queue_config, job_type, resource_type, maker=None):
        # get plugin
        if maker is None:
            maker = self.pluginFactory.get_plugin(queue_config.workerMaker)
        return maker.get_max_workers_per_job_per_cycle()
