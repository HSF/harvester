from pandaharvester.harvestercore.work_spec import WorkSpec
from .base_worker_maker import BaseWorkerMaker
from pandaharvester.harvestercore import core_utils

# multijob worker maker. one job per node. aprun as executor (initially)
# static parameters collected from queue config file
# pilot
baseLogger = core_utils.setup_logger("multijob_workermaker")


class MultiJobWorkerMaker(BaseWorkerMaker):
    # constructor
    def __init__(self, **kwarg):
        BaseWorkerMaker.__init__(self, **kwarg)
        tmpLog = self.make_logger(baseLogger, method_name="__init__")
        tmpLog.info("Multijob workermaker")

    def _get_executable(self, queue_config):
        # return string which contain body of script for scheduler: specific enviroment setup, executor with parameters
        exe_str = ""

        tmpLog = self.make_logger(baseLogger, method_name="_get_executable")

        # prepare static enviroment
        env_str = ""
        if self.env not in (None, "NULL"):
            env_str = "\n".join(map(lambda s: s.strip(), self.env.split(", ")))

        # prepare executor
        try:
            if self.executor == "aprun":  # "aprun -n [number of required nodes/jobs] -d [number of cpu per node/job]" - for one multicore job per node
                exe_str = self.executor + " -n {0} -d {1} ".format(self.nJobsPerWorker, queue_config.submitter["nCorePerNode"])
                exe_str += self.pilot
            else:
                exe_str = self.executor + " " + self.pilot
            if self.pilot_params:
                exe_str = " ".join([exe_str, self.pilot_params])
        except Exception:
            tmpLog.error("Unable to build executor command check configuration")
            exe_str = ""

        exe_str = "\n".join([env_str, exe_str])
        tmpLog.debug("Shell script body: \n%s" % exe_str)

        return exe_str

    # make a worker from a job with a disk access point
    def make_worker(self, jobspec_list, queue_config, job_type, resource_type):
        tmpLog = self.make_logger(baseLogger, method_name="make_worker")
        workSpec = WorkSpec()
        self.nJobsPerWorker = len(jobspec_list)
        tmpLog.info("Worker for {0} jobs will be prepared".format(self.nJobsPerWorker))
        if self.nJobsPerWorker > 0:
            workSpec.nCore = int(queue_config.submitter["nCorePerNode"]) * self.nJobsPerWorker
            workSpec.minRamCount = 0
            workSpec.maxDiskCount = 0
            workSpec.maxWalltime = 0
            if queue_config.walltimeLimit:
                workSpec.maxWalltime = queue_config.walltimeLimit
                tmpLog.debug("Wall time limit for worker: {0}".format(workSpec.maxWalltime))
            for jobSpec in jobspec_list:
                try:
                    workSpec.minRamCount = max(workSpec.minRamCount, jobSpec.jobParams["minRamCount"])
                except Exception:
                    pass
                try:
                    workSpec.maxDiskCount += jobSpec.jobParams["maxDiskCount"]
                except Exception:
                    pass
                # try:  we should not relay on job parameters yet (not relaible)
                #     if jobSpec.jobParams['maxWalltime'] not in (None, "NULL"):
                #         workSpec.maxWalltime = max(workSpec.maxWalltime, jobSpec.jobParams['maxWalltime'])
                # except Exception:
                #     pass

            workSpec.workParams = self._get_executable(queue_config)

        return workSpec
