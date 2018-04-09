import radical.utils
import saga
import os
from datetime import datetime
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestersubmitter.saga_submitter import SAGASubmitter

# logger
baseLogger = core_utils.setup_logger('saga_monitor')


# monitor through SAGA
class SAGAMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        tmpLog = self.make_logger(baseLogger, method_name='__init__')
        tmpLog.info("[{0}] SAGA adaptor will be used".format(self.adaptor))

    # check workers
    def check_workers(self, workspec_list):
        """Check status of workers. This method takes a list of WorkSpecs as input argument
        and returns a list of worker's statuses.
  
        :param workspec_list: a list of work specs instances
        :return: A tuple of return code (True for success, False otherwise) and a list of worker's statuses.
        :rtype: (bool, [string,])
        """
        job_service = saga.job.Service(self.adaptor)
        sagadateformat_str = '%a %b %d %H:%M:%S %Y'
        retList = []
        for workSpec in workspec_list:
            # make logger
            errStr = ''
            tmpLog = self.make_logger(baseLogger, 'workerID={0}'.format(workSpec.workerID),
                                      method_name='check_workers')
            tmpLog.debug("SAGA monitor started")
            if workSpec.batchID:
                saga_submission_id = '[{0}]-[{1}]'.format(self.adaptor, workSpec.batchID)
                try:
                    worker = job_service.get_job(saga_submission_id)
                    tmpLog.debug('SAGA State for submission with batchid: {0} is: {1}'.format(workSpec.batchID, worker.state))
                    harvester_job_state = SAGASubmitter.status_translator(worker.state)
                    tmpLog.debug(
                        'Worker state with batchid: {0} is: {1}'.format(workSpec.batchID, harvester_job_state))
                    workSpec.set_status(harvester_job_state)
                    if worker.created:
                        tmpLog.debug("Worker created (SAGA): {0}".format(worker.created))
                        workSpec.submitTime = datetime.strptime(worker.created, sagadateformat_str)
                    if worker.started:
                        tmpLog.debug("Worker started (SAGA): {0}".format(worker.started))
                        workSpec.startTime = datetime.strptime(worker.started, sagadateformat_str)
                    if worker.finished:
                        tmpLog.debug("Worker finished (SAGA): {0}".format(worker.finished))
                        workSpec.endTime = datetime.strptime(worker.finished, sagadateformat_str)

                    if workSpec.is_final_status():
                        tmpLog.info('Worker {2} with BatchID={0} completed with exit code {1}'.format(workSpec.batchID,
                                                                                                  worker.exit_code,
                                                                                                  workSpec.workerID))
                        tmpLog.debug('Started: [{0}] finished: [{1}]'.format(worker.started, worker.finished))

                except saga.SagaException as ex:
                    tmpLog.info('An exception occured during retriving worker information {0}'.format(workSpec.batchID))
                    tmpLog.info(ex.get_message())
                    # probably 'fnished' is not proper state in this case, 'undefined' looks a bit better
                    # some more work for SAGA to get proper state
                    harvester_job_state = workSpec.ST_finished
                    workSpec.set_status(harvester_job_state)
                    tmpLog.debug('Worker state set to: {0} ({1})'.format(workSpec.status, harvester_job_state))
                retList.append((harvester_job_state, errStr))
                # for compatibility with dummy monitor
                f = open(os.path.join(workSpec.accessPoint, 'status.txt'), 'w')
                f.write(workSpec.status)
                f.close()

            else:
                tmpLog.debug("SAGA monitor found worker [{0}] without batchID".format(workSpec.workerID))

        job_service.close()
        tmpLog.debug('Results: {0}'.format(retList))

        return True, retList
