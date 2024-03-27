import datetime
import math
import threading

from pandaharvester.harvestercore import core_utils

from .core_utils import SingletonWithID
from .db_proxy_pool import DBProxyPool as DBProxy
from .resource_type_constants import (
    CAPABILITY_TAG,
    CRIC_CORE_TAG,
    CRIC_RAM_TAG,
    UNIFIED_QUEUE_TAG,
)


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

    def match(self, core_count, ram_count):
        """
        Checks if the resource type matches the core count and ram count
        :param core_count: number of cores
        :param ram_count: amount of memory
        :return: boolean
        """

        # basic validation that the values are not None or 0
        if not core_count or not ram_count:
            return False

        # normalize ram count
        ram_per_core = ram_count / core_count

        # check if the resource type matches the core count and ram count
        if (
            (self.max_core and core_count > self.max_core)
            or (self.min_core and core_count < self.min_core)
            or (self.max_ram_per_core and ram_per_core > self.max_ram_per_core)
            or (self.min_ram_per_core and ram_per_core < self.min_ram_per_core)
        ):
            return False

        return True


class ResourceTypeMapper(object, metaclass=SingletonWithID):
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
        :param resource_name: string with the resource type name
        :return: boolean
        """
        self.load_data()
        if resource_name in self.resource_types:
            return True
        return False

    def calculate_worker_requirements(self, resource_name, queue_dict):
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
            site_max_rss = queue_dict.get(CRIC_RAM_TAG, 0) or 0
            site_core_count = queue_dict.get(CRIC_CORE_TAG, 1) or 1

            unified_queue = queue_dict.get(CAPABILITY_TAG, "") == UNIFIED_QUEUE_TAG
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

    def is_single_core_resource_type(self, resource_name):
        """
        Validates whether the resource type is single core by looking at the min and max core definitions
        :param resource_name: string with the resource type name
        :return: boolean
        """
        self.load_data()
        if resource_name in self.resource_types:
            min_core = self.resource_types[resource_name].min_core
            max_core = self.resource_types[resource_name].max_core

            if min_core == max_core == 1:
                return True

        return False

    def get_single_core_resource_types(self):
        """
        Returns a list of resource types that are single core
        :return: list of strings with the resource type names
        """
        self.load_data()
        # iterate over the resource types and find the ones that are single core
        single_core_resource_types = [resource_name for resource_name in self.resource_types if self.is_single_core_resource_type(resource_name)]
        return single_core_resource_types

    def get_multi_core_resource_types(self):
        """
        Returns a list of resource types that are multi core
        :return: list of strings with the resource type names
        """
        self.load_data()
        # iterate over the resource types and find the ones that are multi core (not single core)
        single_core_resource_types = [resource_name for resource_name in self.resource_types if not self.is_single_core_resource_type(resource_name)]
        return single_core_resource_types

    def get_all_resource_types(self):
        """
        Returns a list with all resource types
        :return: list of strings with the resource type names
        """
        self.load_data()
        return list(self.resource_types.keys())

    def get_rtype_for_queue(self, queue_dict):
        """
        Returns the resource type name for a given queue configuration
        :param queue_dict: queue configuration
        :return: string with the resource type name
        """
        self.load_data()

        # retrieve the queue configuration
        site_max_rss = queue_dict.get(CRIC_RAM_TAG, 0) or 0
        site_core_count = queue_dict.get(CRIC_CORE_TAG, 1) or 1
        capability = queue_dict.get(CAPABILITY_TAG, "")

        # unified queues are not mapped to any particular resource type
        if capability == UNIFIED_QUEUE_TAG:
            return ""

        # loop over the resource types and find the one that matches the queue configuration
        for resource_name, resource_type in self.resource_types.items():
            if resource_type.match(site_core_count, site_max_rss):
                return resource_name

        # no match found
        return ""

    def is_high_memory_resource_type(self, resource_name: str) -> bool:
        """
        Validates whether the resource type is high memory
        This is a temporary function as *_HIMEM resource types will be replaced
        :param resource_name: string with the resource type name
        :return: boolean
        """
        self.load_data()
        if resource_name in self.resource_types:
            min_ram_per_core = self.resource_types[resource_name].min_ram_per_core
            if resource_name.endswith("_HIMEM") or min_ram_per_core > 2000:
                return True
        return False
