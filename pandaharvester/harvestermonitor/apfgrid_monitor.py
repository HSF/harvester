import logging
import sys
import threading
import traceback

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestersubmitter.apfgrid_submitter import APFGridSubmitter

try:
    from autopyfactory import condorlib
except ImportError:
    logging.error("Unable to import htcondor/condorlib. sys.path=%s" % sys.path)

# setup base logger
baseLogger = core_utils.setup_logger()


class APFGridMonitorSingleton(type):
    def __init__(self, *args, **kwargs):
        super(APFGridMonitorSingleton, self).__init__(*args, **kwargs)
        self.__instance = None

    def __call__(self, *args, **kwargs):
        if self.__instance is None:
            self.__instance = super(APFGridMonitorSingleton, self).__call__(*args, **kwargs)
        return self.__instance


class APFGridMonitor(object):
    """
    1  WorkSpec.ST_submitted = 'submitted'
    2  WorkSpec.ST_running = 'running'
    4  WorkSpec.ST_finished = 'finished'
    5  WorkSpec.ST_failed = 'failed'
    6  WorkSpec.ST_ready = 'ready'
    3  WorkSpec.ST_cancelled = 'cancelled '

    CONDOR_JOBSTATUS
    1    Idle       I
    2    Running    R
    3    Removed    X
    4    Completed  C
    5    Held       H
    6    Submission_err  E
    """

    __metaclass__ = APFGridMonitorSingleton
    STATUS_MAP = {
        1: WorkSpec.ST_submitted,
        2: WorkSpec.ST_running,
        3: WorkSpec.ST_cancelled,
        4: WorkSpec.ST_finished,
        5: WorkSpec.ST_failed,
        6: WorkSpec.ST_ready,
    }

    JOBQUERYATTRIBUTES = [
        "match_apf_queue",
        "jobstatus",
        "workerid",
        "apf_queue",
        "apf_logurl",
        "apf_outurl",
        "apf_errurl",
    ]

    def __init__(self, **kwarg):
        self.log = core_utils.make_logger(baseLogger)
        self.jobinfo = None
        self.historyinfo = None
        self.log.debug("APFGridMonitor initialized.")

    def _updateJobInfo(self):
        self.log.debug("Getting job info from Condor...")
        out = condorlib.condor_q(APFGridMonitor.JOBQUERYATTRIBUTES)
        self.log.debug("Got jobinfo %s" % out)
        self.jobinfo = out
        out = condorlib.condor_history(attributes=APFGridMonitor.JOBQUERYATTRIBUTES, constraints=[])
        self.log.debug("Got history info %s" % out)
        self.historyinfo = out
        alljobs = self.jobinfo + self.historyinfo
        for jobad in alljobs:
            try:
                workerid = jobad["workerid"]
                self.allbyworkerid[workerid] = jobad
            except KeyError:
                # some non-harvester jobs may not have workerids, ignore them
                pass
        self.log.debug("All jobs indexed by worker_id. %d entries." % len(self.allbyworkerid))

    # check workers
    def check_workers(self, workspec_list):
        """Check status of workers. This method takes a list of WorkSpecs as input argument
        and returns a list of worker's statuses.
        Nth element if the return list corresponds to the status of Nth WorkSpec in the given list. Worker's
        status is one of WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled, WorkSpec.ST_running,
        WorkSpec.ST_submitted.

        :param workspec_list: a list of work specs instances
        :return: A tuple of return code (True for success, False otherwise) and a list of worker's statuses.
        :rtype: (bool, [string,])

        """
        self.jobinfo = []
        self.historyinfo = []
        self.allbyworkerid = {}
        self._updateJobInfo()

        retlist = []
        for workSpec in workspec_list:
            self.log.debug(
                "Worker(workerId=%s queueName=%s computingSite=%s status=%s )"
                % (workSpec.workerID, workSpec.queueName, workSpec.computingSite, workSpec.status)
            )
            try:
                jobad = self.allbyworkerid[workSpec.workerID]
                self.log.debug("Found matching job: ID %s" % jobad["workerid"])
                jobstatus = int(jobad["jobstatus"])
                retlist.append((APFGridMonitor.STATUS_MAP[jobstatus], ""))
            except KeyError:
                self.log.error("No corresponding job for workspec %s" % workSpec)
                retlist.append((WorkSpec.ST_cancelled, ""))
        self.log.debug("retlist=%s" % retlist)
        return True, retlist
