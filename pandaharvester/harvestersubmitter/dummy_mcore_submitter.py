import uuid
import os
import multiprocessing
import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec

# setup base logger
baseLogger = core_utils.setup_logger('dummy_mcore_submitter')


# submit a worker using subprocess
def submit_a_worker(workspec):
    tmpLog = core_utils.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
                                    method_name='submit_a_worker')
    if workspec.get_jobspec_list() is not None:
        tmpLog.debug('aggregated nCore={0} minRamCount={1} maxDiskCount={2}'.format(workspec.nCore,
                                                                                    workspec.minRamCount,
                                                                                    workspec.maxDiskCount))
        tmpLog.debug('max maxWalltime={0}'.format(workspec.maxWalltime))
        for jobSpec in workspec.get_jobspec_list():
            tmpLog.debug('PandaID={0} nCore={1} RAM={2}'.format(jobSpec.PandaID,
                                                                jobSpec.jobParams['coreCount'],
                                                                jobSpec.jobParams['minRamCount']))
        for job in workspec.jobspec_list:
            tmpLog.debug(" ".join([job.jobParams['transformation'], job.jobParams['jobPars']]))
    workspec.batchID = 'batch_ID_{0}'.format(uuid.uuid4().hex)
    workspec.queueName = 'batch_queue_name'
    workspec.computingElement = 'CE_name'
    f = open(os.path.join(workspec.accessPoint, 'status.txt'), 'w')
    f.write(WorkSpec.ST_submitted)
    f.close()
    # fake submission
    p = subprocess.Popen(['sleep', '30'],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdoutStr, stderrStr = p.communicate()
    return (True, stdoutStr + stderrStr)


# dummy submitter
class DummyMcoreSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # submit workers with multiple cores
    def submit_workers(self, workspec_list):
        tmpLog = core_utils.make_logger(baseLogger, method_name='submit_workers')
        tmpLog.debug('start nWorkers={0}'.format(len(workspec_list)))
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
        retList = pool.map(submit_a_worker, workspec_list)
        tmpLog.debug('done')
        return retList
