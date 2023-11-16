import os
import random

import radical.utils
import saga
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec as ws

# setup base logger
baseLogger = core_utils.setup_logger("saga_submitter")

# SAGA submitter


class SAGASubmitter(PluginBase):
    # constructor
    # constructor define job service with particular adaptor (can be extended to support remote execution)
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        tmpLog = self.make_logger(baseLogger, method_name="__init__")
        tmpLog.info(f"[{self.adaptor}] SAGA adaptor will be used")

    def workers_list(self):
        job_service = saga.job.Service(self.adaptor)
        workers = []
        for j in job_service.jobs():
            worker = self.job_service.get_job(j)
            workers.append((worker, worker.state))
        job_service.close()
        return workers

    def _get_executable(self, list_of_pandajobs):
        """
        Prepare command line to launch payload.

        TODO: In general will migrate to specific worker maker
        :param list_of_pandajobs - list of job objects, which should be used:
        :return:  string to execution which will be launched
        """
        executable_arr = ["module load python"]
        for pj in list_of_pandajobs:
            executable_arr.append("aprun -d 16 -n 1 " + pj.jobParams["transformation"] + " " + pj.jobParams["jobPars"])
        return executable_arr

    def _state_change_cb(self, src_obj, fire_on, value):
        tmpLog = self.make_logger(baseLogger, method_name="_state_change_cb")

        # self._workSpec.status = self.status_translator(value)
        self._workSpec.set_status(self.status_translator(value))
        self._workSpec.force_update("status")
        try:
            tmpLog.debug(f"Created time: {src_obj.created}")
            tmpLog.debug(f"src obj: {src_obj}")
        except BaseException:
            tmpLog.debug("FAILED")
        tmpLog.info(f"Worker with BatchID={self._workSpec.batchID} workerID={self._workSpec.workerID} change state to: {self._workSpec.status}")

        # for compatibility with dummy monitor
        f = open(os.path.join(self._workSpec.accessPoint, "status.txt"), "w")
        f.write(self._workSpec.status)
        f.close()

        return True

    def _execute(self, work_spec):
        tmpLog = self.make_logger(baseLogger, method_name="_execute")

        job_service = saga.job.Service(self.adaptor)

        # sagadateformat_str = 'Tue Nov  7 11:31:10 2017'
        # sagadateformat_str = '%a %b %d %H:%M:%S %Y'
        try:
            os.chdir(work_spec.accessPoint)
            tmpLog.info(f"Walltime: {work_spec.maxWalltime} sec. {work_spec.maxWalltime / 60} min.")
            tmpLog.info(f"Cores: {work_spec.nCore}")
            tmpLog.debug(f"Worker directory: {work_spec.accessPoint}")
            jd = saga.job.Description()
            if self.projectname:
                jd.project = self.projectname
            # launching job at HPC

            jd.wall_time_limit = work_spec.maxWalltime / 60  # minutes
            if work_spec.workParams in (None, "NULL"):
                jd.executable = "\n".join(self._get_executable(work_spec.jobspec_list))
            else:
                tmpLog.debug(f"Work params (executable templatae): \n{work_spec.workParams}")
                exe_str = work_spec.workParams
                exe_str = exe_str.format(work_dir=work_spec.accessPoint)
                jd.executable = exe_str
                # jd.executable = work_spec.workParams.format(work_dir=work_spec.accessPoint)

            tmpLog.debug(f"Command to be launched: \n{jd.executable}")
            jd.total_cpu_count = work_spec.nCore  # one node with 16 cores for one job
            jd.queue = self.localqueue
            jd.working_directory = work_spec.accessPoint  # working directory of task
            uq_prefix = f"{random.randint(0, 10000000):07}"
            jd.output = os.path.join(work_spec.accessPoint, f"MPI_pilot_stdout_{uq_prefix}")
            jd.error = os.path.join(work_spec.accessPoint, f"MPI_pilot_stderr_{uq_prefix}")
            work_spec.set_log_file("stdout", jd.output)
            work_spec.set_log_file("stderr", jd.error)

            # Create a new job from the job description. The initial state of
            # the job is 'New'.
            task = job_service.create_job(jd)

            self._workSpec = work_spec
            task.run()
            work_spec.batchID = task.id.split("-")[1][1:-1]  # SAGA have own representation, but real batch id easy to extract
            tmpLog.info(f"Worker ID={work_spec.workerID} with BatchID={work_spec.batchID} submitted")
            tmpLog.debug(f"SAGA status: {task.state}")

            # for compatibility with dummy monitor
            f = open(os.path.join(work_spec.accessPoint, "status.txt"), "w")
            f.write(self.status_translator(task.state))
            f.close()

            job_service.close()
            return 0

        except saga.SagaException as ex:
            # Catch all saga exceptions
            tmpLog.error(f"An exception occurred: ({ex.type}) {str(ex)} ")
            # Trace back the exception. That can be helpful for debugging.
            tmpLog.error(f"\n*** Backtrace:\n {ex.traceback}")
            work_spec.status = work_spec.ST_failed
            return -1

    @staticmethod
    def status_translator(saga_status):
        if saga_status == saga.job.PENDING:
            return ws.ST_submitted
        if saga_status == saga.job.RUNNING:
            return ws.ST_running
        if saga_status == saga.job.DONE:
            return ws.ST_finished
        if saga_status == saga.job.FAILED:
            return ws.ST_failed
        if saga_status == saga.job.CANCELED:
            return ws.ST_cancelled

    # submit workers
    def submit_workers(self, work_specs):
        tmpLog = self.make_logger(baseLogger, method_name="submit_workers")
        tmpLog.debug(f"start nWorkers={len(work_specs)}")
        retList = []

        for workSpec in work_specs:
            res = self._execute(workSpec)
            if res == 0:
                retList.append((True, ""))
            else:
                retList.append((False, "Failed to submit worker. Check logs"))

        tmpLog.debug("done")

        return retList
