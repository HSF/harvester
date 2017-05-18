from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase


# simple maker

class SimpleWorkerMaker(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # make a worker from a job with a disk access point
    def make_worker(self, jobspec_list, queue_conifg):
        workSpec = WorkSpec()
        if len(jobspec_list) > 0:
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
                    if not jobSpec.jobParams['maxWalltime'] == "NULL": # Will be better to heve real Null or 0, then string
                        workSpec.maxWalltime = max(int(queue_conifg.walltimeLimit), jobSpec.jobParams['maxWalltime'])
                    else:
                        workSpec.maxWalltime = queue_conifg.walltimeLimit
                except:
                    pass
        return workSpec

    # get number of jobs per worker
    def get_num_jobs_per_worker(self):
        return 1

    # get number of workers per job
    def get_num_workers_per_job(self):
        return 1
