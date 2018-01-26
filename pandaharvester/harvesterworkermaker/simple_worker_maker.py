from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils


# simple maker

# logger
_logger = core_utils.setup_logger('simple_worker_maker')

class SimpleWorkerMaker(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # make a worker from jobs
    def make_worker(self, jobspec_list, queue_config):
        tmpLog = core_utils.make_logger(_logger, 'queue={0}'.format(queue_config.queueName),
                                        method_name='make_worker')

        tmpLog.debug('jobspec_list: {0}'.format(jobspec_list))

        workSpec = WorkSpec()
        if len(jobspec_list) > 0:
            # push case: we know the job and set the parameters of the job
            workSpec.nCore = 0
            workSpec.minRamCount = 0
            workSpec.maxDiskCount = 0
            workSpec.maxWalltime = 0
            for jobSpec in jobspec_list:
                try:
                    workSpec.nCore += jobSpec.jobParams['coreCount']
                except:
                    workSpec.nCore += 1
                try:
                    workSpec.minRamCount += jobSpec.jobParams['minRamCount']
                except:
                    pass
                try:
                    workSpec.maxDiskCount += jobSpec.jobParams['maxDiskCount']
                except:
                    pass
                try:
                    if jobSpec.jobParams['maxWalltime'] not in (None, "NULL"):
                        workSpec.maxWalltime = max(int(queue_config.walltimeLimit), jobSpec.jobParams['maxWalltime'])
                    else:
                        workSpec.maxWalltime = queue_config.walltimeLimit
                except:
                    pass
        else:
            # pull case: the job was not defined and we need to set the queue parameters
            workSpec.nCore = queue_config.submitter['nCore'] or 1
            workSpec.maxWalltime = queue_config.walltimeLimit
            workSpec.maxDiskCount = 0

        return workSpec

    # get number of jobs per worker.
    # N.B. n_worker is the number of available slots which may be useful for some workflow
    def get_num_jobs_per_worker(self, n_workers):
        try:
            return self.nJobsPerWorker
        except:
            return 1

    # get number of workers per job
    def get_num_workers_per_job(self, n_workers):
        try:
            return self.nWorkersPerJob
        except:
            return 1
