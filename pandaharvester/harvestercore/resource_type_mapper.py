import datetime
import math
import threading

from pandaharvester.harvestercore import core_utils

from .db_proxy_pool import DBProxyPool as DBProxy


class ResourceType(object):
    def __init__(self, resource_type_dict):
        """
        Initialize resource type name and attributes
        """
        # name
        self.resource_name = resource_type_dict["resource_name"]
        # cores
        self.min_core = resource_type_dict["mincore"]
        self.max_core = resource_type_dict["maxcore"]
        # memory
        self.min_ram_per_core = resource_type_dict["minrampercore"]
        self.max_ram_per_core = resource_type_dict["maxrampercore"]


class ResourceTypeMapper(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.resource_types = {}
        self.last_update = None

    def load_data(self):
        with self.lock:
            # check interval
            time_now = core_utils.naive_utcnow()
            if self.last_update is not None and time_now - self.last_update < datetime.timedelta(minutes=10):
                return

            db_proxy = DBProxy()
            resource_type_cache = db_proxy.get_cache("resource_types.json")
            if resource_type_cache:
                resource_type_list = resource_type_cache.data
            else:
                resource_type_list = []

            for resource_type_dict in resource_type_list:
                try:
                    resource_type = ResourceType(resource_type_dict)
                    resource_name = resource_type_dict["resource_name"]
                    self.resource_types[resource_name] = resource_type
                except KeyError:
                    continue

            self.last_update = core_utils.naive_utcnow()
            return

    def is_valid_resource_type(self, resource_name):
        """
        Checks if the resource type is valid (exists in the dictionary of resource types)
        :param resource_name: string with the resource type name (e.g. SCORE, SCORE_HIMEM,...)
        :return: boolean
        """
        if resource_name in self.resource_types:
            return True
        return False

    def calculate_worker_requirements(self, resource_name, queue_config):
        """
        Calculates worker requirements (cores and memory) to request in pilot streaming mode/unified pull queue
        """
        worker_cores = 1
        worker_memory = 1

        self.load_data()
        try:
            # retrieve the resource type definition
            resource_type = self.resource_types[resource_name]

            # retrieve the queue configuration
            site_max_rss = queue_config.get("maxrss", 0) or 0
            site_core_count = queue_config.get("corecount", 1) or 1

            unified_queue = queue_config.get("capability", "") == "ucore"
            if not unified_queue:
                # site is not unified, just request whatever is configured in AGIS
                return site_max_rss, site_core_count

            if resource_type.max_core:
                worker_cores = min(resource_type.max_core, site_core_count)
            else:
                worker_cores = site_core_count

            if resource_type.max_ram_per_core:
                worker_memory = min(resource_type.max_ram_per_core * worker_cores, (site_max_rss / site_core_count) * worker_cores)
            else:
                worker_memory = (site_max_rss / site_core_count) * worker_cores
            worker_memory = int(math.ceil(worker_memory))

        except KeyError:
            pass

        return worker_cores, worker_memory
