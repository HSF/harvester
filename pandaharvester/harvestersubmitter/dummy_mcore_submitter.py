import uuid
import os

try:
    import subprocess32 as subprocess
except BaseException:
    import subprocess

from concurrent.futures import ProcessPoolExecutor as Pool

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec

# setup base logger
baseLogger = core_utils.setup_logger("dummy_mcore_submitter")


# submit a worker using subprocess
def submit_a_worker(workspec):
    tmpLog = core_utils.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="submit_a_worker")
    workspec.reset_changed_list()
    if workspec.get_jobspec_list() is not None:
        tmpLog.debug("aggregated nCore={0} minRamCount={1} maxDiskCount={2}".format(workspec.nCore, workspec.minRamCount, workspec.maxDiskCount))
        tmpLog.debug("max maxWalltime={0}".format(workspec.maxWalltime))
        for jobSpec in workspec.get_jobspec_list():
            tmpLog.debug("PandaID={0} nCore={1} RAM={2}".format(jobSpec.PandaID, jobSpec.jobParams["coreCount"], jobSpec.jobParams["minRamCount"]))
        for job in workspec.jobspec_list:
            tmpLog.debug(" ".join([job.jobParams["transformation"], job.jobParams["jobPars"]]))
    workspec.batchID = "batch_ID_{0}".format(uuid.uuid4().hex)
    workspec.queueName = "batch_queue_name"
    workspec.computingElement = "CE_name"
    f = open(os.path.join(workspec.accessPoint, "status.txt"), "w")
    f.write(WorkSpec.ST_submitted)
    f.close()
    # fake submission
    p = subprocess.Popen(["sleep", "3"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutStr, stderrStr = p.communicate()
    return (True, stdoutStr + stderrStr), workspec.get_changed_attributes()


# dummy submitter with multi-cores
class DummyMcoreSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.logBaseURL = "http://localhost/test"
        PluginBase.__init__(self, **kwarg)

    # submit workers with multiple cores
    def submit_workers(self, workspec_list):
        tmpLog = self.make_logger(baseLogger, method_name="submit_workers")
        tmpLog.debug("start nWorkers={0}".format(len(workspec_list)))
        with Pool() as pool:
            retValList = pool.map(submit_a_worker, workspec_list)
        # propagate changed attributes
        retList = []
        for workSpec, tmpVal in zip(workspec_list, retValList):
            retVal, tmpDict = tmpVal
            workSpec.set_attributes_with_dict(tmpDict)
            workSpec.set_log_file("batch_log", "{0}/{1}.log".format(self.logBaseURL, workSpec.batchID))
            workSpec.set_log_file("stdout", "{0}/{1}.out".format(self.logBaseURL, workSpec.batchID))
            workSpec.set_log_file("stderr", "{0}/{1}.err".format(self.logBaseURL, workSpec.batchID))
            retList.append(retVal)
        tmpLog.debug("done")
        return retList
