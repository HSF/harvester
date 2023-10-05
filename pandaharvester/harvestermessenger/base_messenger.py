from pandaharvester.harvestercore.plugin_base import PluginBase


# base messenger
class BaseMessenger(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

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
