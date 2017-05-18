import saga
import os

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec as ws

# setup base logger
baseLogger = core_utils.setup_logger()

# SAGA submitter
class SAGASubmitter (PluginBase):

    # constructor
    # constructor define job service with particular adaptor (can be extended to support remote execution)
    def __init__(self,**kwarg):
        PluginBase.__init__(self,**kwarg)
        self.job_service = saga.job.Service(self.adaptor)

    def __del__(self):
        self.job_service.close()

    def workers_list(self):

        workers = []
        for j in self.job_service.jobs():
            worker = self.job_service.get_job(j)
            workers.append((worker. worker.state))
        return workers

    def _get_executable(self, list_of_pandajobs):
        '''
        Prepare command line to launch payload.
        TODO: In general will migrate to specific worker maker
        :param list_of_pandajobs - list of job objects, which should be used: 
        :return:  string to execution which will be launched
        '''
        executable_arr =  [ 'source $MODULESHOME/init/bash',
                            'module load python']
        for pj in list_of_pandajobs:
            executable_arr.append('aprun -N 1 -d 16 -n 1 '+ pj.jobParams['transformation'] + ' ' + pj.jobParams['jobPars'])
        return executable_arr

    def _state_change_cb(self, src_obj, fire_on, value):

        tmpLog = core_utils.make_logger(baseLogger)

        self._workSpec.status = self.status_translator(value)
        tmpLog.debug('Worker with BatchID={0} change state to: {1}'.format(self._workSpec.batchID, self._workSpec.status))

        # for compatibility with dummy monitor
        f = open(os.path.join(self._workSpec.accessPoint, 'status.txt'), 'w')
        f.write(self._workSpec.status)
        f.close()

        return True

    def _execute(self, workSpec):

        tmpLog = core_utils.make_logger(baseLogger)

        try:
            jd = saga.job.Description()
            if self.projectname:
                jd.project = self.projectname  # an association with HPC allocation needful for bookkeeping system for Titan jd.project = "CSC108"
            # launching job at HPC

            jd.wall_time_limit = workSpec.maxWalltime / 60  # minutes
            jd.executable = "\n".join(self._get_executable(workSpec.jobspec_list))

            jd.total_cpu_count = 1 * self.nCorePerNode  # one node with 16 cores for one job
            jd.queue = self.localqueue

            jd.working_directory = workSpec.accessPoint  # working directory of all task
            jd.output = 'saga_task_stdout' #  file for stdout of payload
            jd.error = 'saga_task_stderr'  #  filr for stderr of payload

            # Create a new job from the job description. The initial state of
            # the job is 'New'.
            task = self.job_service.create_job(jd)

            self._workSpec = workSpec
            task.add_callback(saga.STATE, self._state_change_cb)
            task.run()
            workSpec.batchID = task.id.split('-')[1][1:-1] #SAGA have own representation, but real batch id easy to extract
            workSpec.submitTime = task.created

            task.wait()  # waiting till payload will be compleated.
            workSpec.startTime = task.started
            workSpec.endTime = task.finished
            tmpLog.debug(
                'Worker with BatchID={0} comleated with exit code {1}'.format(workSpec.batchID, task.exit_code))
            return 0

        except saga.SagaException, ex:
            # Catch all saga exceptions
            tmpLog.error("An exception occured: (%s) %s " % (ex.type, (str(ex))))
            # Trace back the exception. That can be helpful for debugging.
            tmpLog.error("\n*** Backtrace:\n %s" % ex.traceback)
            workSpec.status = workSpec.ST_failed
            return -1

    def status_translator(self, saga_status):
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
    def submit_workers(self,workSpecs):
        tmpLog = core_utils.make_logger(baseLogger)
        tmpLog.debug('start nWorkers={0}'.format(len(workSpecs)))
        retList = []

        for workSpec in workSpecs:
            self._execute(workSpec)

        retList.append((True,''))

        tmpLog.debug('done')

        return retList