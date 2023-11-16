import os
import uuid

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec

# setup base logger
baseLogger = core_utils.setup_logger("dummy_submitter")


# dummy submitter
class DummySubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.logBaseURL = "http://localhost/test"
        PluginBase.__init__(self, **kwarg)

    # submit workers
    def submit_workers(self, workspec_list):
        """Submit workers to a scheduling system like batch systems and computing elements.
        This method takes a list of WorkSpecs as input argument, and returns a list of tuples.
        Each tuple is composed of a return code and a dialog message.
        The return code could be True (for success), False (for permanent failures), or None (for temporary failures).
        If the return code is None, submission is retried maxSubmissionAttempts times at most which is defined
        for each queue in queue_config.json.
        Nth tuple in the returned list corresponds to submission status and dialog message for Nth worker
        in the given WorkSpec list.
        A unique identifier is set to WorkSpec.batchID when submission is successful,
        so that they can be identified in the scheduling system. It would be useful to set other attributes
        like queueName (batch queue name), computingElement (CE's host name), and nodeID (identifier of the node
        where the worker is running).

        :param workspec_list: a list of work specs instances
        :return: A list of tuples. Each tuple is composed of submission status (True for success,
        False for permanent failures, None for temporary failures) and dialog message
        :rtype: [(bool, string),]
        """
        tmpLog = self.make_logger(baseLogger, method_name="submit_workers")
        tmpLog.debug(f"start nWorkers={len(workspec_list)}")
        retList = []
        for workSpec in workspec_list:
            workSpec.batchID = f"batch_ID_{uuid.uuid4().hex}"
            workSpec.queueName = "batch_queue_name"
            workSpec.computingElement = "CE_name"
            workSpec.set_log_file("batch_log", f"{self.logBaseURL}/{workSpec.batchID}.log")
            workSpec.set_log_file("stdout", f"{self.logBaseURL}/{workSpec.batchID}.out")
            workSpec.set_log_file("stderr", f"{self.logBaseURL}/{workSpec.batchID}.err")
            if workSpec.get_jobspec_list() is not None:
                tmpLog.debug(f"aggregated nCore={workSpec.nCore} minRamCount={workSpec.minRamCount} maxDiskCount={workSpec.maxDiskCount}")
                tmpLog.debug(f"max maxWalltime={workSpec.maxWalltime}")
                for jobSpec in workSpec.get_jobspec_list():
                    tmpLog.debug(f"PandaID={jobSpec.PandaID} nCore={jobSpec.jobParams['coreCount']} RAM={jobSpec.jobParams['minRamCount']}")
                    # using batchLog URL as pilot ID
                    jobSpec.set_one_attribute("pilotID", workSpec.workAttributes["batchLog"])
                for job in workSpec.jobspec_list:
                    tmpLog.debug(" ".join([job.jobParams["transformation"], job.jobParams["jobPars"]]))
            f = open(os.path.join(workSpec.accessPoint, "status.txt"), "w")
            f.write(WorkSpec.ST_submitted)
            f.close()
            retList.append((True, ""))
        tmpLog.debug("done")
        return retList
