from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.plugin_base import PluginBase


# get payload interaction attributes from harvester config
def get_payload_interaction_attr(attr, default=None):
    return getattr(harvester_config.payload_interaction, attr, default)


# base messenger
class BaseMessenger(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self._load_default_attrs()
        PluginBase.__init__(self, **kwarg)

    # load default messenger attributes
    def _load_default_attrs(self):
        # json for worker attributes
        self.jsonAttrsFileName = get_payload_interaction_attr("workerAttributesFile")
        # json for job report
        self.jsonJobReport = get_payload_interaction_attr("jobReportFile")
        # json for outputs
        self.jsonOutputsFileName = get_payload_interaction_attr("eventStatusDumpJsonFile")
        # xml for outputs
        self.xmlOutputsBaseFileName = get_payload_interaction_attr("eventStatusDumpXmlFile")
        # json for job request
        self.jsonJobRequestFileName = get_payload_interaction_attr("jobRequestFile")
        # json for job spec
        self.jobSpecFileName = get_payload_interaction_attr("jobSpecFile", "pandaJobData.out")
        # json for event request
        self.jsonEventsRequestFileName = get_payload_interaction_attr("eventRequestFile")
        # json to feed events
        self.jsonEventsFeedFileName = get_payload_interaction_attr("eventRangesFile")
        # json to update events
        self.jsonEventsUpdateFileName = get_payload_interaction_attr("updateEventsFile")
        # PFC for input files
        self.xmlPoolCatalogFileName = get_payload_interaction_attr("xmlPoolCatalogFile")
        # json to get PandaIDs
        self.pandaIDsFile = get_payload_interaction_attr("pandaIDsFile")
        # json to kill worker itself
        self.killWorkerFile = get_payload_interaction_attr("killWorkerFile", "kill_worker.json")
        # json for heartbeats from the worker
        self.heartbeatFile = get_payload_interaction_attr("heartbeatFile", "worker_heartbeat.json")
        # task specific persistent dir
        self.taskWorkBaseDir = get_payload_interaction_attr("taskWorkBaseDir", "/tmp/workdir")
        # task-level work state file
        self.taskWorkStateFile = get_payload_interaction_attr("taskWorkStateFile", "state.json")

    # get access point
    def get_access_point(self, workspec, panda_id):
        pass

    # get attributes of a worker which should be propagated to job(s).
    #  * the worker needs to put a json under the access point
    def get_work_attributes(self, workspec):
        return dict()

    # get files to stage-out.
    #  * the worker needs to put a json under the access point
    def get_files_to_stage_out(self, workspec):
        return dict()

    # check if job is requested.
    # * the worker needs to put a json under the access point
    def job_requested(self, workspec):
        return 0

    # feed jobs
    # * worker_jobspec.json is put under the access point
    def feed_jobs(self, workspec, jobspec_list):
        return False

    # request events.
    # * the worker needs to put a json under the access point
    def events_requested(self, workspec):
        return dict()

    # feed events
    # * worker_events.json is put under the access point
    def feed_events(self, workspec, events_dict):
        return False

    # update events.
    # * the worker needs to put a json under the access point
    def events_to_update(self, workspec):
        return dict()

    # acknowledge events and files
    # * delete json.read files
    def acknowledge_events_files(self, workspec):
        pass

    # setup access points
    def setup_access_points(self, workspec_list):
        pass

    # filter for log.tar.gz
    def filter_log_tgz(self, name):
        return False

    # post-processing (archiving log files and collecting job metrics)
    def post_processing(self, workspec, jobspec_list, map_type):
        return True

    # get PandaIDs for pull model
    def get_panda_ids(self, workspec):
        return list()

    # check if requested to kill the worker itself
    def kill_requested(self, workspec):
        return False

    # check if the worker is alive
    def is_alive(self, workspec, time_limit):
        return None

    # clean up. Called by sweeper agent to clean up stuff made by messenger for the worker
    def clean_up(self, workspec):
        return (None, "skipped")
