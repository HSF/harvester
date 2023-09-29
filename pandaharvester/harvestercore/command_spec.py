"""
Command spec class: a panda poller will retrieve commands from panda server and store cache them internally
"""

from .spec_base import SpecBase


class CommandSpec(SpecBase):
    # attributes
    attributesWithTypes = ("command_id:integer primary key", "command:text", "receiver:text", "params:blob", "ack_requested:integer", "processed:integer")
    # commands
    COM_reportWorkerStats = "REPORT_WORKER_STATS"
    COM_setNWorkers = "SET_N_WORKERS_JOBTYPE"
    COM_killWorkers = "KILL_WORKERS"
    COM_syncWorkersKill = "SYNC_WORKERS_KILL"

    # mapping between command and receiver
    receiver_map = {COM_reportWorkerStats: "propagator", COM_setNWorkers: "submitter", COM_killWorkers: "sweeper", COM_syncWorkersKill: "sweeper"}

    # constructor
    def __init__(self):
        SpecBase.__init__(self)

    # convert from Command JSON
    def convert_command_json(self, data):
        # mandatory fields
        self.command_id = data["command_id"]
        self.command = data["command"]
        self.params = data["params"]
        self.ack_requested = data["ack_requested"]
        # For the day we want to parse the creation_date
        # from datetime import datetime
        # c = datetime.strptime(b, "%Y-%m-%dT%H:%M:%S.%f")

        # optional field
        try:
            self.processed = data["processed"]
        except KeyError:
            self.processed = 0
