from pandaharvester.harvestercore.plugin_base import PluginBase


# base worker maker
class BaseWorkerMaker(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    def get_num_jobs_per_worker(self, n_workers):
        try:
            return self.nJobsPerWorker
        except Exception:
            return 1

    # get number of workers per job which can run concurrently
    def get_num_workers_per_job(self, n_workers):
        try:
            return self.nWorkersPerJob
        except Exception:
            return 1

    # check number of ready resources
    def num_ready_resources(self):
        try:
            return self.nReadyResources
        except Exception:
            return 1

    # get upper limit on the cumulative total of workers per job
    def get_max_workers_per_job_in_total(self):
        try:
            return self.maxWorkersPerJobInTotal
        except Exception:
            return 10

    # get upper limit on the number of new workers per job in a cycle
    def get_max_workers_per_job_per_cycle(self):
        try:
            return self.maxWorkersPerJobPerCycle
        except Exception:
            return None
