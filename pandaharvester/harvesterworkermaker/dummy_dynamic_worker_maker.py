import random
from pandaharvester.harvestercore.work_spec import WorkSpec
from .base_worker_maker import BaseWorkerMaker


# dummy worker maker


class DummyDynamicWorkerMaker(BaseWorkerMaker):
    # constructor
    def __init__(self, **kwarg):
        BaseWorkerMaker.__init__(self, **kwarg)

    # make a worker from jobs
    def make_worker(self, jobspec_list, queue_config, job_type, resource_type):
        workSpec = WorkSpec()
        workSpec.resourceType = resource_type
        if len(jobspec_list) > 0:
            workSpec.nCore = 0
            workSpec.minRamCount = 0
            workSpec.maxDiskCount = 0
            workSpec.maxWalltime = 0
            for jobSpec in jobspec_list:
                try:
                    workSpec.nCore += jobSpec.jobParams["coreCount"]
                except Exception:
                    workSpec.nCore += 1
                try:
                    workSpec.minRamCount += jobSpec.jobParams["minRamCount"]
                except Exception:
                    pass
                try:
                    workSpec.maxDiskCount += jobSpec.jobParams["maxDiskCount"]
                except Exception:
                    pass
                try:
                    if jobSpec.jobParams["maxWalltime"] not in (None, "NULL"):
                        workSpec.maxWalltime = max(int(queue_config.walltimeLimit), jobSpec.jobParams["maxWalltime"])
                    else:
                        workSpec.maxWalltime = queue_config.walltimeLimit
                except Exception:
                    pass
        return workSpec

    # get number of jobs per worker based on dynamic information such as # of free CPUs at that time.
    # N.B. n_worker is the number of available slots which may be useful for some workflow
    def get_num_jobs_per_worker(self, n_workers):
        # dummy. should use batch system info, etc
        return random.randint(self.minJobsPerWorker, self.maxJobsPerWorker)

    # get number of workers per job based on dynamic information
    def get_num_workers_per_job(self, n_workers):
        # dummy. should use batch system info, etc
        return random.randint(self.minWorkersPerJob, self.maxWorkersPerJob)
