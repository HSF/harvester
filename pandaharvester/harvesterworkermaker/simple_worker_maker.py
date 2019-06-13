import random

from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestercore.resource_type_mapper import ResourceTypeMapper
from .base_worker_maker import BaseWorkerMaker
import datetime

# logger
_logger = core_utils.setup_logger('simple_worker_maker')


# simple maker
class SimpleWorkerMaker(BaseWorkerMaker):
    # constructor
    def __init__(self, **kwarg):
        self.jobAttributesToUse = ['nCore', 'minRamCount', 'maxDiskCount', 'maxWalltime', 'ioIntensity']
        BaseWorkerMaker.__init__(self, **kwarg)
        self.rt_mapper = ResourceTypeMapper()
        try:
            self.pilotTypeRandomWeightsPermille
        except AttributeError:
            self.pilotTypeRandomWeightsPermille = {}
        finally:
            self.pilotTypeRandomList = core_utils.make_choice_list(
                                            pdpm=self.pilotTypeRandomWeightsPermille, default='PR')

    def get_job_core_and_memory(self, queue_dict, job_spec):

        job_memory = job_spec.jobParams.get('minRamCount', 0) or 0
        job_corecount = job_spec.jobParams.get('coreCount', 1) or 1

        unified_queue = queue_dict.get('capability', '') == 'ucore'

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

        unified_queue = queue_dict.get('capability', '') == 'ucore'
        # case of traditional (non-unified) queue: look at the queue configuration
        if not unified_queue:
            workSpec.nCore = queue_dict.get('corecount', 1) or 1
            workSpec.minRamCount = queue_dict.get('maxrss', 1) or 1

        # case of unified queue: look at the resource type and queue configuration
        else:
            catchall = queue_dict.get('catchall', '')
            if 'useMaxRam' in catchall or queue_config.queueName in ('Taiwan-LCG2-HPC2_Unified',
                                                                       'Taiwan-LCG2-HPC_Unified', 'DESY-ZN_UCORE'):
                # temporary hack to debug killed workers in Taiwan queues
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
            else:
                workSpec.nCore, workSpec.minRamCount = self.rt_mapper.calculate_worker_requirements(resource_type,
                                                                                                    queue_dict)

        # parameters that are independent on traditional vs unified
        workSpec.maxWalltime = queue_dict.get('maxtime', 1)
        workSpec.maxDiskCount = queue_dict.get('maxwdir', 1)
        walltimeLimit_default = getattr(queue_config, 'walltimeLimit', 0)

        if len(jobspec_list) > 0:
            # get info from jobs
            nCore = 0
            minRamCount = 0
            maxDiskCount = 0
            maxWalltime = 0
            ioIntensity = 0
            for jobSpec in jobspec_list:
                job_corecount, job_memory = self.get_job_core_and_memory(queue_dict, jobSpec)
                nCore += job_corecount
                minRamCount += job_memory
                try:
                    maxDiskCount += jobSpec.jobParams['maxDiskCount']
                except Exception:
                    pass
                try:
                    ioIntensity += jobSpec.jobParams['ioIntensity']
                except Exception:
                    pass
            try:
                # maxWallTime from AGIS or qconf, not trusting job currently
                maxWalltime = queue_dict.get('maxtime', walltimeLimit_default)
            except Exception:
                pass

            if (nCore > 0 and 'nCore' in self.jobAttributesToUse) \
               or unified_queue:
                workSpec.nCore = nCore
            if (minRamCount > 0 and 'minRamCount' in self.jobAttributesToUse) \
               or unified_queue:
                workSpec.minRamCount = minRamCount
            if maxDiskCount > 0 and 'maxDiskCount' in self.jobAttributesToUse:
                workSpec.maxDiskCount = maxDiskCount
            if maxWalltime > 0 and 'maxWalltime' in self.jobAttributesToUse:
                workSpec.maxWalltime = maxWalltime
            if ioIntensity > 0 and 'ioIntensity' in self.jobAttributesToUse:
                workSpec.ioIntensity = ioIntensity
            workSpec.pilotType = jobspec_list[0].get_pilot_type()
        else:
            # when no job
            # randomize pilot type with weighting
            workSpec.pilotType = random.choice(self.pilotTypeRandomList)
            if workSpec.pilotType in ['RC', 'ALRB', 'PT']:
                tmpLog.info('a worker has pilotType={0}'.format(workSpec.pilotType))
        # TODO: this needs to be improved with real resource types
        if resource_type and resource_type != 'ANY':
            workSpec.resourceType = resource_type
        elif workSpec.nCore == 1:
            workSpec.resourceType = 'SCORE'
        else:
            workSpec.resourceType = 'MCORE'

        return workSpec
