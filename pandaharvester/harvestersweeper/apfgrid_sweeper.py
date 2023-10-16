import logging
import sys
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


class APFGridSweeperSingleton(type):
    def __init__(self, *args, **kwargs):
        super(APFGridSweeperSingleton, self).__init__(*args, **kwargs)
        self.__instance = None

    def __call__(self, *args, **kwargs):
        if self.__instance is None:
            self.__instance = super(APFGridSweeperSingleton, self).__call__(*args, **kwargs)
        return self.__instance


# dummy plugin for sweeper
class APFGridSweeper(object):
    __metaclass__ = APFGridSweeperSingleton

    STATUS_MAP = {
        1: WorkSpec.ST_submitted,
        2: WorkSpec.ST_running,
        3: WorkSpec.ST_cancelled,
        4: WorkSpec.ST_finished,
        5: WorkSpec.ST_failed,
        6: WorkSpec.ST_ready,
    }

    JOBQUERYATTRIBUTES = ["match_apf_queue", "jobstatus", "workerid", "apf_queue", "clusterid", "procid"]

    # constructor
    def __init__(self, **kwarg):
        self.log = core_utils.make_logger(baseLogger)
        self.jobinfo = None
        self.allbyworkerid = {}

        self.log.debug("APFGridSweeper initialized.")

    def _updateJobInfo(self):
        self.log.debug("Getting job info from Condor...")
        out = condorlib.condor_q(APFGridSweeper.JOBQUERYATTRIBUTES)
        self.log.debug("Got jobinfo %s" % out)
        self.jobinfo = out
        for jobad in self.jobinfo:
            try:
                workerid = jobad["workerid"]
                self.allbyworkerid[workerid] = jobad
            except KeyError:
                # some non-harvester jobs may not have workerids, ignore them
                pass
        self.log.debug("All jobs indexed by worker_id. %d entries." % len(self.allbyworkerid))

    # kill a worker

    def kill_worker(self, workspec):
        """Kill a single worker in a scheduling system like batch systems and computing elements.

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        self.allbyworkerid = {}
        self._updateJobInfo()
        try:
            jobad = self.allbyworkerid(workspec.workerID)
            clusterid = jobad["clusterid"]
            procid = jobad["procid"]
            killstr = "%s.%s" % (clusterid, procid)
            self.log.debug("Killing condor job %s ..." % killstr)
            condorlib.condor_rm([killstr])
            self.log.debug("Killed condor job %s with workerid %s" % (killstr, workspec.workerID))
        except KeyError:
            self.log.warning("kill_worker called on non-existent workerid: %s" % workspec.workerID)

        return True, ""

    # cleanup for a worker
    def sweep_worker(self, workspec):
        """Perform cleanup procedures for a single worker, such as deletion of work directory.

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        return True, ""
