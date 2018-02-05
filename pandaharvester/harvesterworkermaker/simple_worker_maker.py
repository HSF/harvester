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
    def make_worker(self, jobspec_list, queue_config, resource_type):
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
            # pull case: there is no predefined job, so we need to set the parameters based on the queue definition
            # and the resource type of the job
            # get the queue configuration from the DB
            panda_queues_cache = self.dbInterface.get_cache('panda_queues.json')
            panda_queues_dict = dict() if not panda_queues_cache else panda_queues_cache.data
            queue_dict = panda_queues_dict.get(queue_config.queueName, {})

            unified_queue = 'unifiedPandaQueue' in queue_dict.get('catchall', '')
            # case of traditional (non-unified) queue: look at the queue configuration
            if not unified_queue:
                workSpec.nCore = queue_dict.get('corecount', 1) or 1
                workSpec.minRamCount = queue_dict.get('maxrss', 1) or 1

            # case of unified queue: look at the resource type and queue configuration
            else:
                site_corecount = queue_dict.get('corecount', 1) or 1
                site_maxrss = queue_dict.get('maxrss', 1) or 1

                if 'SCORE' in resource_type:
                    workSpec.nCore = 1
                    workSpec.minRamCount = site_maxrss / site_corecount
                else:
                    workSpec.nCore = site_corecount
                    workSpec.minRamCount = site_maxrss

            # parameters that are independent on traditional vs unified
            workSpec.maxWalltime = queue_dict.get('maxtime', 1)
            workSpec.maxDiskCount = queue_dict.get('maxwdir', 1)

        # TODO: this needs to be improved with real resource types
        if resource_type and resource_type != 'ANY':
            workSpec.resourceType = resource_type
        elif workSpec.nCore == 1:
            workSpec.resourceType = 'SCORE'
        else:
            workSpec.resourceType = 'MCORE'


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
