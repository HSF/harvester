import datetime
import math
import traceback

try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

from pandaharvester.harvestercore.work_spec import WorkSpec
from .base_worker_maker import BaseWorkerMaker
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict


# simple backfill eventservice maker

# logger
_logger = core_utils.setup_logger("simple_bf_worker_maker")


class SimpleBackfillESWorkerMaker(BaseWorkerMaker):
    # constructor
    def __init__(self, **kwarg):
        self.jobAttributesToUse = ["nCore", "minRamCount", "maxDiskCount", "maxWalltime"]
        self.adjusters = None
        BaseWorkerMaker.__init__(self, **kwarg)
        self.init_adjusters_defaults()
        self.dyn_resources = None

    # make a worker from jobs
    def make_worker(self, jobspec_list, queue_config, job_type, resource_type):
        tmpLog = self.make_logger(_logger, "queue={0}".format(queue_config.queueName), method_name="make_worker")

        tmpLog.debug("jobspec_list: {0}".format(jobspec_list))

        workSpec = WorkSpec()
        workSpec.creationTime = datetime.datetime.utcnow()

        # get the queue configuration from the DB
        panda_queues_dict = PandaQueuesDict()
        queue_dict = panda_queues_dict.get(queue_config.queueName, {})
        workSpec.minRamCount = queue_dict.get("maxrss", 1) or 1
        workSpec.maxWalltime = queue_dict.get("maxtime", 1)
        workSpec.maxDiskCount = queue_dict.get("maxwdir", 1)

        # get info from jobs
        if len(jobspec_list) > 0:
            nRemainingEvents = 0
            for jobspec in jobspec_list:
                if jobspec.nRemainingEvents:
                    nRemainingEvents += jobspec.nRemainingEvents

            nCore, maxWalltime = self.calculate_worker_requirements(nRemainingEvents)
            workSpec.nCore = nCore
            workSpec.maxWalltime = maxWalltime

        # TODO: this needs to be improved with real resource types
        if resource_type and resource_type != "ANY":
            workSpec.resourceType = resource_type
        elif workSpec.nCore == 1:
            workSpec.resourceType = "SCORE"
        else:
            workSpec.resourceType = "MCORE"

        return workSpec

    # get number of workers per job
    def get_num_workers_per_job(self, n_workers):
        try:
            # return min(self.nWorkersPerJob, n_workers)
            return self.nWorkersPerJob
        except Exception:
            return 1

    # check number of ready resources
    def num_ready_resources(self):
        # make logger
        tmpLog = self.make_logger(_logger, "simple_bf_es_maker", method_name="num_ready_resources")

        try:
            resources = self.get_bf_resources()
            if resources:
                resources = self.adjust_resources(resources)
                if resources:
                    self.dyn_resources = resources
                    return len(self.dyn_resources)
            return 0
        except Exception:
            tmpLog.error("Failed to get num of ready resources: %s" % (traceback.format_exc()))
            return 0

    def init_adjusters_defaults(self):
        """
        adjusters: [{"minNodes": <minNodes>,
                     "maxNodes": <maxNodes>,
                     "minWalltimeSeconds": <minWalltimeSeconds>,
                     "maxWalltimeSeconds": <maxWalltimeSeconds>,
                     "nodesToDecrease": <nodesToDecrease>,
                     "walltimeSecondsToDecrease": <walltimeSecondsToDecrease>,
                     "minCapacity": <minWalltimeSeconds> * <minNodes>,
                     "maxCapacity": <maxWalltimeSeconds> * <maxNodes>}]
        """
        adj_defaults = {
            "minNodes": 1,
            "maxNodes": 125,
            "minWalltimeSeconds": 1800,
            "maxWalltimeSeconds": 7200,
            "nodesToDecrease": 1,
            "walltimeSecondsToDecrease": 60,
        }
        if self.adjusters:
            for adjuster in self.adjusters:
                for key, value in adj_defaults.items():
                    if key not in adjuster:
                        adjuster[key] = value
                    adjuster["minCapacity"] = adjuster["minWalltimeSeconds"] * adjuster["minNodes"]
                    adjuster["maxCapacity"] = adjuster["maxWalltimeSeconds"] * adjuster["maxNodes"]
            self.adjusters.sort(key=lambda my_dict: my_dict["minNodes"])

    # get backfill resources
    def get_bf_resources(self, blocking=True):
        # make logger
        tmpLog = self.make_logger(_logger, "simple_bf_es_maker", method_name="get_bf_resources")
        resources = []
        # command
        if blocking:
            comStr = "showbf -p {0} --blocking".format(self.partition)
        else:
            comStr = "showbf -p {0}".format(self.partition)
        # get backfill resources
        tmpLog.debug("Get backfill resources with {0}".format(comStr))
        p = subprocess.Popen(comStr.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # check return code
        stdOut, stdErr = p.communicate()
        retCode = p.returncode
        tmpLog.debug("retCode={0}".format(retCode))
        if retCode == 0:
            # extract batchID
            tmpLog.debug("Available backfill resources for partition(%s):\n%s" % (self.partition, stdOut))
            lines = stdOut.splitlines()
            for line in lines:
                line = line.strip()
                if line.startswith(self.partition):
                    try:
                        items = line.split()
                        nodes = int(items[2])
                        if nodes < self.minNodes:
                            continue
                        walltime = items[3]
                        resources.append({"nodes": nodes, "walltime": walltime})
                    except BaseException:
                        tmpLog.error("Failed to parse line: %s" % line)
        else:
            # failed
            errStr = stdOut + " " + stdErr
            tmpLog.error(errStr)
        tmpLog.info("Available backfill resources: %s" % resources)
        return resources

    def get_adjuster(self, nodes):
        for adj in self.adjusters:
            if nodes >= adj["minNodes"] and nodes <= adj["maxNodes"]:
                return adj
        return None

    def adjust_resources(self, resources):
        # make logger
        tmpLog = self.make_logger(_logger, "simple_bf_es_maker", method_name="adjust_resources")
        ret_resources = []
        for resource in resources:
            if resource["nodes"] > self.maxNodes:
                nodes = self.maxNodes
            else:
                nodes = resource["nodes"]

            adjuster = self.get_adjuster(nodes)
            if adjuster:
                if (resource["nodes"] - adjuster["nodesToDecrease"]) < nodes:
                    nodes = resource["nodes"] - adjuster["nodesToDecrease"]
                if nodes <= 0:
                    continue
                walltime = resource["walltime"]
                if walltime == "INFINITY":
                    walltime = adjuster["maxWalltimeSeconds"]
                    ret_resources.append({"nodes": nodes, "walltime": walltime, "nCore": nodes * self.nCorePerNode})
                else:
                    h, m, s = walltime.split(":")
                    walltime = int(h) * 3600 + int(m) * 60 + int(s)
                    if walltime >= adjuster["minWalltimeSeconds"] and walltime < adjuster["maxWalltimeSeconds"]:
                        walltime -= adjuster["walltimeSecondsToDecrease"]
                        ret_resources.append({"nodes": nodes, "walltime": walltime, "nCore": nodes * self.nCorePerNode})
                    elif walltime >= adjuster["maxWalltimeSeconds"]:
                        walltime = adjuster["maxWalltimeSeconds"] - adjuster["walltimeSecondsToDecrease"]
                        ret_resources.append({"nodes": nodes, "walltime": walltime, "nCore": nodes * self.nCorePerNode})
        ret_resources.sort(key=lambda my_dict: my_dict["nodes"] * my_dict["walltime"], reverse=True)
        tmpLog.info("Available backfill resources after adjusting: %s" % ret_resources)
        return ret_resources

    def get_dynamic_resource(self, queue_name, job_type, resource_type):
        resources = self.get_bf_resources()
        if resources:
            resources = self.adjust_resources(resources)
            if resources:
                return {"nNewWorkers": 1, "resources": resources}
        return {}

    def get_needed_nodes_walltime(self, availNodes, availWalltime, neededCapacity):
        tmpLog = self.make_logger(_logger, "simple_bf_es_maker", method_name="get_needed_nodes_walltime")
        solutions = []
        spareNodes = 1  # one Yoda node which doesn't process any events
        for adj in self.adjusters:
            if availNodes < adj["minNodes"]:
                continue
            solutionNodes = min(availNodes, adj["maxNodes"])
            solutionWalltime = min(availWalltime, adj["maxWalltimeSeconds"] - adj["walltimeSecondsToDecrease"])
            if neededCapacity >= (solutionNodes - spareNodes) * solutionWalltime:
                solutions.append({"solutionNodes": solutionNodes, "solutionWalltime": solutionWalltime})
            else:
                solutionNodes = neededCapacity / solutionWalltime + spareNodes
                if solutionNodes >= adj["minNodes"]:
                    solutions.append({"solutionNodes": solutionNodes, "solutionWalltime": solutionWalltime})
                else:
                    solutionNodes = adj["minNodes"]
                    requiredWalltime = neededCapacity / (solutionNodes - spareNodes)
                    if requiredWalltime >= adj["minWalltimeSeconds"]:
                        # walltime can be bigger than the requiredWalltime, will exit automatically
                        solutions.append({"solutionNodes": solutionNodes, "solutionWalltime": solutionWalltime})

        def solution_compare(x, y):
            if x["solutionWalltime"] - y["solutionWalltime"] != 0:
                return x["solutionWalltime"] - y["solutionWalltime"]
            else:
                return x["solutionNodes"] - y["solutionNodes"]

        solutions.sort(cmp=solution_compare, reverse=True)
        tmpLog.info("Available solutions: %s" % solutions)
        if solutions:
            return solutions[0]["solutionNodes"], solutions[0]["solutionWalltime"]
        else:
            None

    # calculate needed cores and maxwalltime
    def calculate_worker_requirements(self, nRemainingEvents):
        tmpLog = self.make_logger(_logger, "simple_bf_es_maker", method_name="calculate_worker_requirements")
        if not hasattr(self, "nSecondsPerEvent") or self.nSecondsPerEvent < 100:
            tmpLog.warn("nSecondsPerEvent is not set, will use default value 480 seconds(8 minutes)")
            nSecondsPerEvent = 480
        else:
            nSecondsPerEvent = self.nSecondsPerEvent

        nCore = None
        walltime = None
        if self.dyn_resources:
            resource = self.dyn_resources.pop(0)
            tmpLog.debug("Selected dynamic resources: %s" % resource)
            walltime = resource["walltime"]
            if nRemainingEvents <= 0:
                if resource["nodes"] < self.defaultNodes:
                    nCore = resource["nodes"] * self.nCorePerNode
                else:
                    tmpLog.warn("nRemainingEvents is not correctly propagated or delayed, will not submit big jobs, shrink number of nodes to default")
                    nCore = self.defaultNodes * self.nCorePerNode
            else:
                neededCapacity = nRemainingEvents * nSecondsPerEvent * 1.0 / self.nCorePerNode
                tmpLog.info(
                    "nRemainingEvents: %s, nSecondsPerEvent: %s, nCorePerNode: %s, neededCapacity(nodes*walltime): %s"
                    % (nRemainingEvents, nSecondsPerEvent, self.nCorePerNode, neededCapacity)
                )

                neededNodes, neededWalltime = self.get_needed_nodes_walltime(resource["nodes"], walltime, neededCapacity)
                tmpLog.info("neededNodes: %s, neededWalltime: %s" % (neededNodes, neededWalltime))
                neededNodes = int(math.ceil(neededNodes))
                walltime = int(neededWalltime)
                if neededNodes < 2:
                    neededNodes = 2

                nCore = neededNodes * self.nCorePerNode
        else:
            nCore = self.defaultNodes * self.nCorePerNode
            walltime = self.defaultWalltimeSeconds
        return nCore, walltime
