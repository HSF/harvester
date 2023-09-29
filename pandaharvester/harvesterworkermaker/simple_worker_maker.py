from __future__ import division

import math
import random

from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.job_spec import JobSpec
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestercore.resource_type_mapper import ResourceTypeMapper
from .base_worker_maker import BaseWorkerMaker
import datetime

# logger
_logger = core_utils.setup_logger("simple_worker_maker")


# simple maker
class SimpleWorkerMaker(BaseWorkerMaker):
    # constructor
    def __init__(self, **kwarg):
        self.jobAttributesToUse = ["nCore", "minRamCount", "maxDiskCount", "maxWalltime", "ioIntensity"]
        BaseWorkerMaker.__init__(self, **kwarg)
        self.rt_mapper = ResourceTypeMapper()

    def get_job_core_and_memory(self, queue_dict, job_spec):
        job_memory = job_spec.jobParams.get("minRamCount", 0) or 0
        job_corecount = job_spec.jobParams.get("coreCount", 1) or 1

        is_ucore = queue_dict.get("capability", "") == "ucore"

        if not job_memory and is_ucore:
            site_maxrss = queue_dict.get("maxrss", 0) or 0
            site_corecount = queue_dict.get("corecount", 1) or 1

            if job_corecount == 1:
                job_memory = int(math.ceil(site_maxrss / site_corecount))
            else:
                job_memory = site_maxrss

        return job_corecount, job_memory

    def get_job_type(self, job_spec, job_type, queue_dict, tmp_prodsourcelabel=None):
        queue_type = queue_dict.get("type", None)

        # 1. get prodSourceLabel from job (PUSH)
        if job_spec and "prodSourceLabel" in job_spec.jobParams:
            job_type_final = job_spec.jobParams["prodSourceLabel"]

        # 2. get prodSourceLabel from the specified job_type (PULL UPS)
        elif job_type:
            job_type_final = job_type
            if tmp_prodsourcelabel:
                if queue_type != "analysis" and tmp_prodsourcelabel not in ("user", "panda", "managed"):
                    # for production, unified or other types of queues we need to run neutral prodsourcelabels
                    # with production proxy since they can't be distinguished and can fail
                    job_type_final = "managed"

        # 3. convert the prodSourcelabel from the queue configuration or leave it empty (PULL)
        else:
            # map CRIC types to PanDA types
            if queue_type == "analysis":
                job_type_final = "user"
            elif queue_type == "production":
                job_type_final = "managed"
            else:
                job_type_final = None

        return job_type_final

    def capability_to_rtype(self, capability):
        if capability == "score":
            return "SCORE"
        elif capability == "himem":
            return "SCORE_HIMEM"
        elif capability == "mcore":
            return "MCORE"
        elif capability == "mcorehimem":
            return "MCORE_HIMEM"
        else:
            return None

    # make a worker from jobs
    def make_worker(self, jobspec_list, queue_config, job_type, resource_type):
        tmpLog = self.make_logger(_logger, "queue={0}:{1}:{2}".format(queue_config.queueName, job_type, resource_type), method_name="make_worker")

        tmpLog.debug("jobspec_list: {0}".format(jobspec_list))

        workSpec = WorkSpec()
        workSpec.creationTime = datetime.datetime.utcnow()

        # get the queue configuration from CRIC
        panda_queues_dict = PandaQueuesDict()
        queue_dict = panda_queues_dict.get(queue_config.queueName, {})
        associated_params_dict = panda_queues_dict.get_harvester_params(queue_config.queueName)

        is_ucore = queue_dict.get("capability", "") == "ucore"
        # case of traditional (non-ucore) queue: look at the queue configuration
        if not is_ucore:
            workSpec.nCore = queue_dict.get("corecount", 1) or 1
            workSpec.minRamCount = queue_dict.get("maxrss", 1) or 1

        # case of unified queue: look at the job & resource type and queue configuration
        else:
            catchall = queue_dict.get("catchall", "")
            if "useMaxRam" in catchall or queue_config.queueName in ("Taiwan-LCG2-HPC2_Unified", "Taiwan-LCG2-HPC_Unified", "DESY-ZN_UCORE"):
                # temporary hack to debug killed workers in Taiwan queues
                site_corecount = queue_dict.get("corecount", 1) or 1
                site_maxrss = queue_dict.get("maxrss", 1) or 1

                # some cases need to overwrite those values
                if "SCORE" in resource_type:
                    # the usual pilot streaming use case
                    workSpec.nCore = 1
                    workSpec.minRamCount = int(math.ceil(site_maxrss / site_corecount))
                else:
                    # default values
                    workSpec.nCore = site_corecount
                    workSpec.minRamCount = site_maxrss
            else:
                if not len(jobspec_list) and resource_type not in ["SCORE", "SCORE_HIMEM", "MCORE", "MCORE_HIMEM"]:
                    # some testing PQs have ucore + pure pull, need to default to SCORE
                    tmpLog.warning(
                        'Invalid resource type "{resource_type}" (perhaps due to ucore with pure pull); default to SCORE'.format(resource_type=resource_type)
                    )
                    resource_type = "SCORE"
                workSpec.nCore, workSpec.minRamCount = self.rt_mapper.calculate_worker_requirements(resource_type, queue_dict)

        # parameters that are independent on traditional vs unified
        workSpec.maxWalltime = queue_dict.get("maxtime", 1)
        workSpec.maxDiskCount = queue_dict.get("maxwdir", 1)
        walltimeLimit_default = getattr(queue_config, "walltimeLimit", 0)

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
                    maxDiskCount += jobSpec.jobParams["maxDiskCount"]
                except Exception:
                    pass
                try:
                    maxWalltime += jobSpec.jobParams["maxWalltime"]
                except Exception:
                    pass
                try:
                    ioIntensity += jobSpec.jobParams["ioIntensity"]
                except Exception:
                    pass
            # fill in worker attributes
            if (nCore > 0 and "nCore" in self.jobAttributesToUse) or is_ucore:
                workSpec.nCore = nCore
            if (minRamCount > 0 and "minRamCount" in self.jobAttributesToUse) or is_ucore:
                workSpec.minRamCount = minRamCount
            if maxDiskCount > 0 and ("maxDiskCount" in self.jobAttributesToUse or associated_params_dict.get("job_maxdiskcount") is True):
                workSpec.maxDiskCount = maxDiskCount
            if maxWalltime > 0 and ("maxWalltime" in self.jobAttributesToUse or associated_params_dict.get("job_maxwalltime") is True):
                workSpec.maxWalltime = maxWalltime
            if ioIntensity > 0 and ("ioIntensity" in self.jobAttributesToUse or associated_params_dict.get("job_iointensity") is True):
                workSpec.ioIntensity = ioIntensity

            workSpec.pilotType = jobspec_list[0].get_pilot_type()
            workSpec.jobType = self.get_job_type(jobspec_list[0], job_type, queue_dict)

        else:
            # when no job
            # randomize pilot type with weighting
            pdpm = getattr(queue_config, "prodSourceLabelRandomWeightsPermille", {})
            choice_list = core_utils.make_choice_list(pdpm=pdpm, default="managed")
            tmp_prodsourcelabel = random.choice(choice_list)
            fake_job = JobSpec()
            fake_job.jobParams = {}
            fake_job.jobParams["prodSourceLabel"] = tmp_prodsourcelabel
            workSpec.pilotType = fake_job.get_pilot_type()
            del fake_job
            if workSpec.pilotType in ["RC", "ALRB", "PT"]:
                tmpLog.info("a worker has pilotType={0}".format(workSpec.pilotType))

            workSpec.jobType = self.get_job_type(None, job_type, queue_dict, tmp_prodsourcelabel)
            tmpLog.debug(
                "get_job_type decided for job_type: {0} (input job_type: {1}, queue_type: {2}, tmp_prodsourcelabel: {3})".format(
                    workSpec.jobType, job_type, queue_dict.get("type", None), tmp_prodsourcelabel
                )
            )

        # retrieve queue resource type
        capability = queue_dict.get("capability", "")
        queue_rtype = self.capability_to_rtype(capability)

        if resource_type and resource_type != "ANY":
            workSpec.resourceType = resource_type
        elif queue_rtype:
            workSpec.resourceType = queue_rtype
        elif workSpec.nCore == 1:
            workSpec.resourceType = "SCORE"
        else:
            workSpec.resourceType = "MCORE"

        return workSpec
