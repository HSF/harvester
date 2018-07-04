import threading
import datetime

from .db_proxy_pool import DBProxyPool as DBProxy


class ResourceType:

    def __init__(self, resource_type_dict):
        """
        Initialize resource type name and attributes
        """
        # name
        self.resource_name = resource_type_dict['resource_name']
        # cores
        self.min_core = resource_type_dict['mincore']
        self.max_core = resource_type_dict['maxcore']
        # memory
        self.min_ram_per_core = resource_type_dict['minrampercore']
        self.max_ram_per_core = resource_type_dict['maxrampercore']


class ResourceTypeMapper:

    def __init__(self):
        self.lock = threading.Lock()
        self.resource_types = {}
        self.last_update = None

    def load_data(self):

        with self.lock:

            # check interval
            time_now = datetime.datetime.utcnow()
            if self.last_update is not None and time_now - self.last_update < datetime.timedelta(minutes=10):
                return

            db_proxy = DBProxy()
            resource_type_list = db_proxy.get_cache('resource_types')

            for resource_type_dict in resource_type_list:
                try:
                    resource_type = ResourceType(resource_type_dict)
                    resource_name = resource_type_dict['resource_name']
                    self.resource_types[resource_name] = resource_type
                except KeyError:
                    continue

            self.last_update = datetime.datetime.utcnow()
            return

    def calculate_worker_requirements(self, resource_name, queue_config):
        """
        Calculates worker requirements (cores and memory) to request in pilot streaming mode/unified pull queue
        """
        worker_cores = None
        worker_memory = None

        self.load_data()
        try:
            # retrieve the resource type definition
            resource_type = self.resource_types[resource_name]

            # retrieve the queue configuration
            site_maxrss = queue_config.get('maxrss', 0) or 0
            site_corecount = queue_config.get('corecount', 1) or 1

            unified_queue = queue_config.get('capability', '') == 'ucore'
            if not unified_queue:
                # site is not unified, just request whatever is configured in AGIS
                return site_maxrss, site_corecount

            if resource_type.max_core:
                worker_cores = resource_type.max_core
            else:
                worker_cores = site_corecount

            if resource_type.max_ram_per_core:
                worker_memory = resource_type.max_ram_per_core * worker_cores
            else:
                worker_memory = (site_maxrss / site_corecount) * worker_cores

        except KeyError:
            pass

        return worker_cores, worker_memory

