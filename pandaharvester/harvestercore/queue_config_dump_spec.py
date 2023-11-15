"""
Queue Config dump class

"""
import copy
import hashlib
import json

from .spec_base import SpecBase


class QueueConfigDumpSpec(SpecBase):
    # attributes
    attributesWithTypes = (
        "configID:integer primary key",
        "queueName:text / index",
        "checksum:text",
        "dumpUniqueName:text / unique",
        "creationTime:timestamp / index",
        "data:blob",
    )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)

    # set data
    def set_data(self, data):
        self.data = copy.deepcopy(data)
        # don't record status
        try:
            del self.data["queueStatus"]
        except Exception:
            pass
        # get checksum
        m = hashlib.md5()
        m.update(json.dumps(self.data).encode("utf-8"))
        self.checksum = m.hexdigest()
        # set unique name
        self.dumpUniqueName = f"{self.queueName}_{self.checksum}"
