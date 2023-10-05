import radical.utils
import os
import time
from datetime import datetime

import saga

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.plugin_factory import PluginFactory
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestersubmitter.saga_submitter import SAGASubmitter

# logger
baseLogger = core_utils.setup_logger("saga_monitor")


# monitor through SAGA
class SAGAMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.pluginFactory = PluginFactory()
        self.queue_config_mapper = QueueConfigMapper()
        tmpLog = self.make_logger(baseLogger, method_name="__init__")
        tmpLog.info("[{0}] SAGA adaptor will be used.".format(self.adaptor))

    # check workers
    def check_workers(self, workspec_list):
        """Check status of workers. This method takes a list of WorkSpecs as input argument
        and returns a list of worker's statuses.

        :param workspec_list: a list of work specs instances
        :return: A tuple of return code (True for success, False otherwise) and a list of worker's statuses.
        :rtype: (bool, [string,])
        """
        try:
            job_service = saga.job.Service(self.adaptor)
        except saga.SagaException as ex:
            time.sleep(10)
            self.check_workers(workspec_list)
        sagadateformat_str = "%a %b %d %H:%M:%S %Y"
        retList = []
        for workSpec in workspec_list:
            # make logger
            errStr = ""
            tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workSpec.workerID), method_name="check_workers")
            tmpLog.debug("SAGA monitor started")
            if workSpec.batchID:
                saga_submission_id = "[{0}]-[{1}]".format(self.adaptor, workSpec.batchID)
                try:
                    worker = job_service.get_job(saga_submission_id)
                    tmpLog.debug("SAGA State for submission with batchid: {0} is: {1}".format(workSpec.batchID, worker.state))
                    harvester_job_state = SAGASubmitter.status_translator(worker.state)
                    workSpec.nativeStatus = worker.state
                    workSpec.set_status(harvester_job_state)
                    tmpLog.debug("Worker state with batchid: {0} is: {1} exit code: {2}".format(workSpec.batchID, harvester_job_state, worker.exit_code))
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
                        workSpec.nativeExitCode = worker.exit_code
                        tmpLog.info("Worker in final status [{0}] exit code: {1}".format(workSpec.status, workSpec.nativeExitCode))
                        if workSpec.nativeExitCode != 0:  # let's try to find exit code, exit message etc...
                            tmpLog.info("Deep check to find exit code and exit status required")
                            harvester_job_state, workSpec.nativeExitCode, workSpec.nativeStatus, starttime, endtime, errStr = self.deep_checkjob(
                                workSpec.batchID, workSpec.workerID
                            )
                            if harvester_job_state == "":
                                harvester_job_state = workSpec.ST_finished
                            if not workSpec.startTime:
                                workSpec.startTime = starttime
                            if endtime:
                                workSpec.endTime = endtime
                            workSpec.set_status(harvester_job_state)
                        tmpLog.info(
                            "Worker {2} with BatchID={0} finished with exit code {1} and state {3}".format(
                                workSpec.batchID, worker.exit_code, workSpec.workerID, worker.state
                            )
                        )
                        tmpLog.debug("Started: [{0}] finished: [{1}]".format(worker.started, worker.finished))

                    if worker.state == saga.job.PENDING:
                        queue_time = (datetime.now() - workSpec.submitTime).total_seconds()
                        tmpLog.info("Worker queued for {0} sec.".format(queue_time))
                        if hasattr(self, "maxqueuetime") and queue_time > self.maxqueuetime:
                            tmpLog.info("Queue time {0} is longer than limit {1} worker will be canceled".format(queue_time, self.maxqueuetime))
                            worker.cancel()
                            worker.wait()
                            workSpec.nativeExitCode = worker.exit_code
                            cur_time = datetime.now()
                            workSpec.startTime = cur_time
                            workSpec.endTime = cur_time
                            workSpec.set_pilot_closed()
                            workSpec.set_status(workSpec.ST_cancelled)
                            harvester_job_state = workSpec.ST_cancelled
                            tmpLog.info("Worker state: {0} worker exit code: {1}".format(harvester_job_state, workSpec.nativeExitCode))
                            # proper processing of jobs for worker will be required, to avoid 'fake' fails

                except saga.SagaException as ex:
                    tmpLog.info("An exception occured during retriving worker information {0}".format(workSpec.batchID))
                    tmpLog.info(ex.get_message())
                    # probably 'fnished' is not proper state in this case, 'undefined' looks a bit better
                    # some more work for SAGA to get proper state
                    harvester_job_state, workSpec.nativeExitCode, workSpec.nativeStatus, starttime, endtime, errStr = self.deep_checkjob(
                        workSpec.batchID, workSpec.workerID
                    )
                    if harvester_job_state == "":
                        harvester_job_state = workSpec.ST_finished
                    if not workSpec.startTime:
                        workSpec.startTime = starttime
                    if endtime:
                        workSpec.endTime = endtime
                    workSpec.set_status(harvester_job_state)
                    tmpLog.debug("Worker state set to: {0} ({1})".format(workSpec.status, harvester_job_state))
                retList.append((harvester_job_state, errStr))
                # for compatibility with dummy monitor
                f = open(os.path.join(workSpec.accessPoint, "status.txt"), "w")
                f.write(workSpec.status)
                f.close()

            else:
                tmpLog.debug("SAGA monitor found worker [{0}] without batchID".format(workSpec.workerID))

        job_service.close()
        tmpLog.debug("Results: {0}".format(retList))

        return True, retList

    def deep_checkjob(self, batchid, workerid):
        """
        Get job state, exit code and some more parameters, from resources depending sources

        :param batchid:
        :return harvester_job_state, nativeExitCode, nativeStatus, startTime, endTime, diagMessage
        """
        tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workerid), method_name="deep_checkjob")
        harvester_job_state = None
        nativeexitcode = None
        nativestatus = None
        diagmessage = ""
        starttime = None
        endtime = None
        queue_config = self.queue_config_mapper.get_queue(self.queueName)
        if hasattr(queue_config, "resource"):
            resource_utils = self.pluginFactory.get_plugin(queue_config.resource)
        else:
            tmpLog.debug("Resource configuration missed for: {0}".format(self.queueName))
            resource_utils = None
        if resource_utils:
            batchjob_info = resource_utils.get_batchjob_info(batchid)
        if batchjob_info:
            tmpLog.info("Batch job info collected: {0}".format(batchjob_info))
            harvester_job_state = batchjob_info["status"]
            nativeexitcode = batchjob_info["nativeExitCode"]
            nativestatus = batchjob_info["nativeStatus"]
            diagmessage = batchjob_info["nativeExitMsg"]
            if batchjob_info["start_time"]:
                starttime = batchjob_info["start_time"]
            if batchjob_info["finish_time"]:
                endtime = batchjob_info["finish_time"]

        return harvester_job_state, nativeexitcode, nativestatus, starttime, endtime, diagmessage
