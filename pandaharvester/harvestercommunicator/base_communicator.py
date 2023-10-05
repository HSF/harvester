"""
Base class to communicate with WMS

"""

import abc
from future.utils import with_metaclass

from pandaharvester.harvestercore import core_utils

# logger
_logger = core_utils.setup_logger("communicator")


# base class for communication with WMS
class BaseCommunicator(with_metaclass(abc.ABCMeta, object)):
    # constructor
    def __init__(self):
        pass

    # make logger
    def make_logger(self, tag=None, method_name=None):
        return core_utils.make_logger(_logger, token=tag, method_name=method_name)

    # get jobs
    def get_jobs(self, site_name, node_name, prod_source_label, computing_element, n_jobs, additional_criteria):
        return [], ""

    # update jobs
    def update_jobs(self, jobspec_list, id):
        return [{"StatusCode": 0, "ErrorDiag": "", "command": ""}] * len(jobspec_list)

    # get events
    def get_event_ranges(self, data_map, scattered, base_path):
        return True, {}

    # update events
    def update_event_ranges(self, event_ranges, tmp_log):
        retMap = dict()
        retMap["StatusCode"] = 0
        retMap["Returns"] = [True] * len(event_ranges)
        return retMap

    # get commands
    def get_commands(self, n_commands):
        return []

    # send ACKs
    def ack_commands(self, command_ids):
        return True

    # update workers
    def update_workers(self, workspec_list):
        return [True] * len(workspec_list), ""

    # send heartbeat of harvester instance
    def is_alive(self, key_values):
        return True, ""

    # update worker stats
    def update_worker_stats(self, site_name, stats):
        return True, ""

    # check jobs
    def check_jobs(self, jobspec_list):
        return [{"StatusCode": 0, "ErrorDiag": "", "command": ""}] * len(jobspec_list)

    # send dialog messages
    def send_dialog_messages(self, dialog_list):
        return True, ""

    # update service metrics
    def update_service_metrics(self, service_metrics_list):
        return True, ""
