from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestercore.resource_type_mapper import ResourceTypeMapper
import datetime


# simple maker

# logger
_logger = core_utils.setup_logger('simple_worker_maker')


class SimpleWorkerMaker(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.jobAttributesToUse = ['nCore', 'minRamCount', 'maxDiskCount', 'maxWalltime']
        PluginBase.__init__(self, **kwarg)

        self.rt_mapper = ResourceTypeMapper()

    def get_job_core_and_memory(self, queue_dict, job_spec):

        job_memory = job_spec.jobParams.get('minRamCount', 0) or 0
        job_corecount = job_spec.jobParams.get('coreCount', 1) or 1

        unified_queue = 'unifiedPandaQueue' in queue_dict.get('catchall', '') \
                        or queue_dict.get('capability', '') == 'ucore'

        if not job_memory and unified_queue:
            site_maxrss = queue_dict.get('maxrss', 0) or 0
            site_corecount = queue_dict.get('corecount', 1) or 1

            if job_corecount == 1:
                job_memory = site_maxrss / site_corecount
            else:
                job_memory = site_maxrss

        return job_corecount, job_memory

    # make a worker from jobs
    def make_worker(self, jobspec_list, queue_config, resource_type):
        tmpLog = self.make_logger(_logger, 'queue={0}'.format(queue_config.queueName),
                                  method_name='make_worker')

        tmpLog.debug('jobspec_list: {0}'.format(jobspec_list))

        workSpec = WorkSpec()
        workSpec.creationTime = datetime.datetime.utcnow()

        # get the queue configuration from the DB
        panda_queues_dict = PandaQueuesDict()
        queue_dict = panda_queues_dict.get(queue_config.queueName, {})

        unified_queue = 'unifiedPandaQueue' in queue_dict.get('catchall', '')\
                        or queue_dict.get('capability', '') == 'ucore'
        # case of traditional (non-unified) queue: look at the queue configuration
        if not unified_queue:
            workSpec.nCore = queue_dict.get('corecount', 1) or 1
            workSpec.minRamCount = queue_dict.get('maxrss', 1) or 1

        # case of unified queue: look at the resource type and queue configuration
        else:
            workSpec.nCore, workSpec.minRamCount = self.rt_mapper.calculate_worker_requirements(resource_type, queue_dict)

            """
            site_corecount = queue_dict.get('corecount', 1) or 1
            site_maxrss = queue_dict.get('maxrss', 1) or 1

            # some cases need to overwrite those values
            if 'SCORE' in resource_type:
                # the usual pilot streaming use case
                workSpec.nCore = 1
                workSpec.minRamCount = site_maxrss / site_corecount
            else:
                # default values
                workSpec.nCore = site_corecount
                workSpec.minRamCount = site_maxrss
            """


        # parameters that are independent on traditional vs unified
        workSpec.maxWalltime = queue_dict.get('maxtime', 1)
        workSpec.maxDiskCount = queue_dict.get('maxwdir', 1)

        # get info from jobs
        if len(jobspec_list) > 0:
            nCore = 0
            minRamCount = 0
            maxDiskCount = 0
            maxWalltime = 0
            for jobSpec in jobspec_list:

                job_corecount, job_memory  = self.get_job_core_and_memory(queue_dict, jobSpec)
                nCore += job_corecount
                minRamCount += job_memory

                try:
                    maxDiskCount += jobSpec.jobParams['maxDiskCount']
                except:
                    pass

                try:
                    if jobSpec.jobParams['maxWalltime'] not in (None, "NULL"):
                        if hasattr(queue_config, 'maxWalltime'):
                            maxWalltime = max(int(queue_config.walltimeLimit), jobSpec.jobParams['maxWalltime'])
                        else:
                            maxWalltime = jobSpec.jobParams['maxWalltime']
                    else:
                        maxWalltime = queue_config.walltimeLimit
                except:
                    pass
            if nCore > 0 and 'nCore' in self.jobAttributesToUse:
                workSpec.nCore = nCore
            if minRamCount > 0 and 'minRamCount' in self.jobAttributesToUse:
                workSpec.minRamCount = minRamCount
            if maxDiskCount > 0 and 'maxDiskCount' in self.jobAttributesToUse:
                workSpec.maxDiskCount = maxDiskCount
            if maxWalltime > 0 and 'maxWalltime' in self.jobAttributesToUse:
                workSpec.maxWalltime = maxWalltime

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
