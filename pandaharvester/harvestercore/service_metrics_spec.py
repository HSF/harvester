"""
service metric spec class

"""
import json
from .spec_base import SpecBase
import datetime
import json
import socket


class ServiceMetricSpec(SpecBase):
    # attributes
    attributesWithTypes = (
        "creationTime:timestamp / index",
        "hostName:text",
        "metrics:blob",
    )

    # constructor
    def __init__(self, service_metrics):
        SpecBase.__init__(self)

        self.creationTime = datetime.datetime.utcnow()
        self.hostName = socket.getfqdn()
        self.metrics = service_metrics  # blobs are automatically translated to json
