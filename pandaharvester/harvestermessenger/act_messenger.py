import datetime
import os
import json

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from .base_messenger import BaseMessenger
from pandaharvester.harvesterconfig import harvester_config

from act.atlas.aCTDBPanda import aCTDBPanda

# json for outputs
jsonOutputsFileName = harvester_config.payload_interaction.eventStatusDumpJsonFile

# json to update events
jsonEventsUpdateFileName = harvester_config.payload_interaction.updateEventsFile

# suffix to read json
suffixReadJson = ".read"

# logger
baseLogger = core_utils.setup_logger("act_messenger")


class ACTMessenger(BaseMessenger):
    """Mechanism for passing information about completed jobs back to harvester."""

    def __init__(self, **kwarg):
        BaseMessenger.__init__(self, **kwarg)

        # Set up aCT DB connection
        self.log = core_utils.make_logger(baseLogger, "aCT messenger", method_name="__init__")
        self.actDB = aCTDBPanda(self.log)

    # get access point
    def get_access_point(self, workspec, panda_id):
        if workspec.mapType == WorkSpec.MT_MultiJobs:
            accessPoint = os.path.join(workspec.get_access_point(), str(panda_id))
        else:
            accessPoint = workspec.get_access_point()
        return accessPoint

    def post_processing(self, workspec, jobspec_list, map_type):
        """Now done in stager"""
        return True

    def get_work_attributes(self, workspec):
        """Get info from the job to pass back to harvester"""
        # Just return existing attributes. Attributes are added to workspec for
        # finished jobs in post_processing
        return workspec.workAttributes

    def events_requested(self, workspec):
        """Used to tell harvester that the worker requests events."""

        # Not yet implemented, dynamic event fetching not supported yet
        return {}

    def feed_events(self, workspec, events_dict):
        """
        Harvester has an event range to pass to job
        events_dict is {pandaid: [{eventrange1}, {eventrange2}, ..]}
        """

        # get logger
        tmpLog = core_utils.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="feed_events")
        retVal = True
        if workspec.mapType in [WorkSpec.MT_OneToOne, WorkSpec.MT_MultiWorkers]:
            # insert the event range into aCT DB and mark the job ready to go
            for pandaid, eventranges in events_dict.items():
                desc = {"eventranges": json.dumps(eventranges), "actpandastatus": "sent", "pandastatus": "sent", "arcjobid": None}
                tmpLog.info("Inserting {0} events for job {1}".format(len(eventranges), pandaid))
                try:
                    self.actDB.updateJob(pandaid, desc)
                except Exception as e:
                    core_utils.dump_error_message(tmpLog)
                    retVal = False
        elif workspec.mapType == WorkSpec.MT_MultiJobs:
            # TOBEFIXED
            pass
        tmpLog.debug("done")
        return retVal

    def events_to_update(self, workspec):
        """Report events processed for harvester to update"""

        # get logger
        tmpLog = core_utils.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="events_to_update")
        # look for the json just under the access point
        retDict = dict()
        for pandaID in workspec.pandaid_list:
            # look for the json just under the access point
            accessPoint = self.get_access_point(workspec, pandaID)

            jsonFilePath = os.path.join(accessPoint, jsonEventsUpdateFileName)
            readJsonPath = jsonFilePath + suffixReadJson
            # first look for json.read which is not yet acknowledged
            tmpLog.debug("looking for event update file {0}".format(readJsonPath))
            if os.path.exists(readJsonPath):
                pass
            else:
                tmpLog.debug("looking for event update file {0}".format(jsonFilePath))
                if not os.path.exists(jsonFilePath):
                    # not found
                    tmpLog.debug("not found")
                    continue
                try:
                    # rename to prevent from being overwritten
                    os.rename(jsonFilePath, readJsonPath)
                except Exception:
                    tmpLog.error("failed to rename json")
                    continue
            # load json
            nData = 0
            try:
                with open(readJsonPath) as jsonFile:
                    tmpOrigDict = json.load(jsonFile)
                    newDict = dict()
                    # change the key from str to int
                    for tmpPandaID, tmpDict in tmpOrigDict.items():
                        tmpPandaID = long(tmpPandaID)
                        retDict[tmpPandaID] = tmpDict
                        nData += len(tmpDict)
            except Exception as x:
                tmpLog.error("failed to load json: {0}".format(str(x)))
            # delete empty file
            if nData == 0:
                try:
                    os.remove(readJsonPath)
                except Exception:
                    pass
            tmpLog.debug("got {0} events for PandaID={1}".format(nData, pandaID))
        return retDict

    def acknowledge_events_files(self, workspec):
        """Acknowledge that events were picked up by harvester"""

        # get logger
        tmpLog = core_utils.make_logger(baseLogger, "workerID={0}".format(workspec.workerID), method_name="acknowledge_events_files")
        # remove request file
        for pandaID in workspec.pandaid_list:
            accessPoint = self.get_access_point(workspec, pandaID)
            try:
                jsonFilePath = os.path.join(accessPoint, jsonEventsUpdateFileName)
                jsonFilePath += suffixReadJson
                jsonFilePath_rename = jsonFilePath + "." + str(datetime.datetime.utcnow())
                os.rename(jsonFilePath, jsonFilePath_rename)
            except Exception:
                pass
            try:
                jsonFilePath = os.path.join(accessPoint, jsonOutputsFileName)
                jsonFilePath += suffixReadJson
                jsonFilePath_rename = jsonFilePath + "." + str(datetime.datetime.utcnow())
                os.rename(jsonFilePath, jsonFilePath_rename)
            except Exception:
                pass
        tmpLog.debug("done")
        return

    # setup access points
    def setup_access_points(self, workspec_list):
        try:
            for workSpec in workspec_list:
                accessPoint = workSpec.get_access_point()
                # make the dir if missing
                if not os.path.exists(accessPoint):
                    os.makedirs(accessPoint)
                jobSpecs = workSpec.get_jobspec_list()
                if jobSpecs is not None:
                    for jobSpec in jobSpecs:
                        subAccessPoint = self.get_access_point(workSpec, jobSpec.PandaID)
                        if accessPoint != subAccessPoint:
                            if not os.path.exists(subAccessPoint):
                                os.mkdir(subAccessPoint)
            return True
        except Exception:
            # get logger
            tmpLog = core_utils.make_logger(_logger, method_name="setup_access_points")
            core_utils.dump_error_message(tmpLog)
            return False

    # The remaining methods do not apply to ARC

    def feed_jobs(self, workspec, jobspec_list):
        """Pass job to worker. No-op for Grid"""
        return True

    def get_files_to_stage_out(self, workspec):
        """Not required in Grid case"""
        return {}

    def job_requested(self, workspec):
        """Used in pull model to say that worker is ready for a job"""
        return False

    def setup_access_points(self, workspec_list):
        """Access is through CE so nothing to set up here"""
        pass

    def get_panda_ids(self, workspec):
        """For pull model, get panda IDs assigned to jobs"""
        return []

    def kill_requested(self, workspec):
        """Worker wants to kill itself (?)"""
        return False

    def is_alive(self, workspec, time_limit):
        """Check if worker is alive, not for Grid"""
        return True


def test():
    pass


if __name__ == "__main__":
    test()
