"""
Command spec class: a panda poller will retrieve commands from panda server and store cache them internally
"""

from spec_base import SpecBase


class CommandSpec(SpecBase):
    # attributes
    attributesWithTypes = ('command_id:integer primary key',
                           'command:text',
                           'params:blob',
                           'ack_requested:integer',
                           'processed:integer'
                           )
    # constructor
    def __init__(self):
        SpecBase.__init__(self)

    # convert from Command JSON
    def convert_command_json(self, data):
        self.command_id = data['command_id']
        self.command = data['command']
        self.params = data['params']
        self.params = data['params']
        self.ack_requested = data['ack_requested']
        self.processed = data['processed']

