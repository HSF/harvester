from pandaharvester.harvestercore.work_spec import WorkSpec
from .base_worker_maker import BaseWorkerMaker
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper

# multinode worker maker. one job per node. aprun as executor (initially)
# static parameters collected from queue config file
# dynamic parametrs from infrastructure through plugins


baseLogger = core_utils.setup_logger("multinode_workermaker")


class MultiNodeWorkerMaker(BaseWorkerMaker):
    # constructor
    def __init__(self, **kwarg):
        BaseWorkerMaker.__init__(self, **kwarg)
        self.pluginFactory = PluginFactory()
        self.queue_config_mapper = QueueConfigMapper()
        tmpLog = self.make_logger(baseLogger, method_name="__init__")
        tmpLog.info("Multinode workermaker: created.")
        tmpLog.debug("Queue name: {0}".format(self.queueName))
        if self.mode == "static":
            tmpLog.info("Static configuration")
        elif self.mode == "dynamic":
            tmpLog.info("Dynamic configuration")
            self.nNodes, self.walltimelimit = self.get_resources()
        self.nJobsPerWorker = self.nNodes * self.nJobsPerNode

    def _get_executable(self):
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
                exe_str = self.executor + " -n {0} -d {1} ".format(self.nJobsPerWorker, self.nCorePerJob)
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

    # make a worker from jobs
    def make_worker(self, jobspec_list, queue_config, job_type, resource_type):
        tmpLog = core_utils.make_logger(baseLogger, "queue={0}".format(queue_config.queueName), method_name="make_worker")

        tmpLog.info("Multi node worker preparation started.")
        tmpLog.info("Worker size: {0} jobs on {2} nodes for {1} sec.".format(self.nJobsPerWorker, self.walltimelimit, self.nNodes))

        workSpec = WorkSpec()
        workSpec.nCore = self.nNodes * queue_config.submitter["nCorePerNode"]
        workSpec.minRamCount = 0
        workSpec.maxDiskCount = 0
        workSpec.maxWalltime = self.walltimelimit
        workSpec.workParams = self._get_executable()

        if len(jobspec_list) > 0:
            # push case: we know the job and set the parameters of the job
            for jobSpec in jobspec_list:
                try:
                    workSpec.minRamCount += jobSpec.jobParams["minRamCount"]
                except Exception:
                    pass
                try:
                    workSpec.maxDiskCount += jobSpec.jobParams["maxDiskCount"]
                except Exception:
                    pass
                # try:
                #    if jobSpec.jobParams['maxWalltime'] not in (None, "NULL"):
                #        workSpec.maxWalltime = max(int(queue_config.walltimeLimit), jobSpec.jobParams['maxWalltime'])
                #    else:
                #        workSpec.maxWalltime = queue_config.walltimeLimit
                # except Exception:
                #    pass
        tmpLog.info("Worker for {0} nodes with {2} jobs with walltime {1} sec. defined".format(self.nNodes, workSpec.maxWalltime, self.nJobsPerWorker))

        return workSpec

    # def get_num_jobs_per_worker(self, n_workers):
    #     """
    #     Function to set 'size' of worker. Define number of jobs per worker
    #     """
    #     tmpLog = core_utils.make_logger(baseLogger, 'queue={0}'.format(self.queueName),
    #                                     method_name='get_num_jobs_per_worker')
    #     tmpLog.info("Get number of jobs per worker")
    #     self.nJobsPerWorker = 1
    #     if self.mode == "static":
    #         tmpLog.info("Static configuration")
    #         self.nJobsPerWorker = self.nNodes * self.nJobsPerNode
    #     elif self.mode == "dynamic":
    #         tmpLog.info("Dynamic configuration")
    #         self.nNodes, self.walltimelimit = self.get_resources()
    #         self.nJobsPerWorker = self.nNodes * self.nJobsPerNode
    #
    #     tmpLog.info("Get: {0} jobs to run for {1} sec.".format(self.nJobsPerWorker, self.walltimelimit))
    #     return self.nJobsPerWorker

    def get_resources(self):
        """
        Function to get resourcese and map them to number of jobs
        """
        tmpLog = core_utils.make_logger(baseLogger, "queue={0}".format(self.queueName), method_name="get_resources")
        njobs = 0
        walltime = self.walltimelimit
        queue_config = self.queue_config_mapper.get_queue(self.queueName)
        resource_utils = self.pluginFactory.get_plugin(queue_config.resource)
        if resource_utils:
            nodes, walltime = resource_utils.get_resources()
        else:
            tmpLog.info("Resource plugin is not defined")
            nodes = self.nNodes

        return nodes, walltime
