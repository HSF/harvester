"""
Lancium python API wrapper functions

"""

from pandaharvester.harvesterconfig import harvester_config
import os

try:
    api_key = harvester_config.lancium.api_key
except AttributeError:
    raise RuntimeError('The configuration is missing the [lancium] section and/or the api_key entry')

# The key needs to be set before importing the lancium API
os.environ['LANCIUM_API_KEY'] = api_key

from lancium.api.Job import Job
from lancium.api.Data import Data

class LanciumClient(object):

    def __init__(self, queue_name=None):
        self.queue_name = queue_name


if __name__ == "__main__":
    lancium_client = LanciumClient()
    lancium_client.test()


