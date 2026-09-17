import re
import threading
import time

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.core_utils import SingletonWithID, setup_logger
from pandaharvester.harvestercore.db_interface import DBInterface
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
_logger = setup_logger("info_utils")

harvesterID = harvester_config.master.harvester_id
resolver_config = getattr(harvester_config.qconf, "resolverConfig", {})


def to_refresh(func):
    """
    Decorator to refresh before accessing the data
    """

    def wrapped_func(self, *args, **kwargs):
        self._refresh()
        return func(self, *args, **kwargs)

    return wrapped_func


class CachedDictBase(dict, PluginBase, metaclass=SingletonWithID):
    """
    Base class of dictionary of information taken from DB cache filled by cacher
    Derived classes are to set default_cacher_key, and to override _update_from_cache
    if the cached data need to be reshaped or postprocessed
    """

    # key of the cached data in DB; overridden by cacher_key in kwargs
    default_cacher_key = None

    def __init__(self, **kwargs):
        dict.__init__(self)
        PluginBase.__init__(self, **kwargs)
        self.lock = threading.Lock()
        self.dbInterface = DBInterface()
        self.cacher_key = kwargs.get("cacher_key", self.default_cacher_key)
        self.refresh_period = resolver_config.get("refreshPeriod", 300)
        self.last_refresh_ts = 0
        self._refresh()

    def _is_fresh(self):
        now_ts = time.time()
        if self.last_refresh_ts + self.refresh_period > now_ts:
            return True
        return False

    def _update_from_cache(self, cache_data):
        """
        Fill self with the data from cache; to be overridden in derived classes
        """
        self.update(cache_data)

    def _retry_soon(self):
        """
        Shorten next refresh period into 5 sec, to retry soon
        """
        self.last_refresh_ts = time.time() - self.refresh_period + 5

    def _refresh(self):
        with self.lock:
            if self._is_fresh():
                return
            tmp_log = self.make_logger(_logger, f"cacher_key={self.cacher_key}", method_name="_refresh", send_dialog=False)
            # get cached data; note the key can be absent in DB, e.g. when it is not in the data of the cacher section in the cfg
            data_cache = None
            if not self.cacher_key:
                tmp_log.warning("cacher_key is not set; skipped")
            else:
                try:
                    data_cache = self.dbInterface.get_cache(self.cacher_key)
                except Exception as e:
                    tmp_log.warning(f"failed to get cache with {e.__class__.__name__}: {e} ; skipped")
            cache_data = getattr(data_cache, "data", None)
            if not isinstance(cache_data, dict):
                # no valid data cached (yet); keep whatever is already filled and retry soon
                tmp_log.debug("no valid cached data; to retry soon")
                self._retry_soon()
                return
            # fill self with the cached data
            try:
                self._update_from_cache(cache_data)
            except Exception as e:
                tmp_log.error(f"failed to update from cache with {e.__class__.__name__}: {e} ; to retry soon")
                self._retry_soon()
                return
            # successfully refreshed from cache
            self.last_refresh_ts = time.time()

    @to_refresh
    def __getitem__(self, key):
        return dict.__getitem__(self, key)

    @to_refresh
    def get(self, key, default=None):
        return dict.get(self, key, default)

    @to_refresh
    def get_all_names(self):
        """
        Return the set of all keys
        """
        return set(self.keys())


class PandaQueuesDict(CachedDictBase):
    """
    Dictionary of PanDA queue info from DB by cacher
    Key is PanDA Resource name (rather than PanDA Queue name)
    Able to query with either PanDA Queue name or PanDA Resource name
    """

    default_cacher_key = "panda_queues.json"

    candidate_per_core_attrs = (
        "maxrss",
        "minrss",
        "maxwdir",
    )

    @staticmethod
    def has_value_in_catchall(panda_queues_dict, key):
        """
        Check if specific value is in catchall attributes
        """
        catchall_str = panda_queues_dict.get("catchall")
        if catchall_str is None:
            return False
        for tmp_key in catchall_str.split(","):
            tmp_match = re.search(f"^{key}(=|)*", tmp_key)
            if tmp_match is not None:
                return True
        return False

    @staticmethod
    def use_per_core_attr(panda_queues_dict):
        """
        Check if treating all attributes as per-core
        """
        return PandaQueuesDict.has_value_in_catchall(panda_queues_dict, "per_core_attr")

    def _update_from_cache(self, cache_data):
        panda_queues_dict = cache_data
        for k, v in panda_queues_dict.items():
            try:
                panda_resource = v["panda_resource"]
                assert k == v["nickname"]
            except Exception:
                pass
            else:
                self[panda_resource] = v
            # handle per-core attributes: scale with corecount if per-core
            if PandaQueuesDict.use_per_core_attr(v):
                core_count = v.get("corecount", 1)
                for attr in self.candidate_per_core_attrs:
                    if attr in v and core_count > 0:
                        v[attr] = v[attr] * core_count

    @to_refresh
    def __getitem__(self, panda_resource):
        if panda_resource in self:
            return dict.__getitem__(self, panda_resource)
        else:
            panda_queue = self.get_panda_queue_name(panda_resource)
            return dict.__getitem__(self, panda_queue)

    @to_refresh
    def get(self, panda_resource, default=None):
        if panda_resource in self:
            return dict.get(self, panda_resource, default)
        else:
            panda_queue = self.get_panda_queue_name(panda_resource)
            return dict.get(self, panda_queue, default)

    def get_panda_queue_name(self, panda_resource):
        """
        Return PanDA Queue name with specified PanDA Resource name
        """
        try:
            panda_queue = self.get(panda_resource).get("nickname")
            return panda_queue
        except Exception:
            return None

    # get queue status for auto blacklisting
    def get_queue_status(self, panda_resource):
        panda_queue_dict = self.get(panda_resource)
        if panda_queue_dict is None:
            return None
        # offline if not with harvester or not of this harvester instance
        if panda_queue_dict.get("pilot_manager") not in ["Harvester"] or panda_queue_dict.get("harvester") != harvesterID:
            return "offline"
        return panda_queue_dict["status"]

    # get all queue names of this harvester instance
    @to_refresh
    def get_all_queue_names(self):
        names = set()
        for queue_name, queue_dict in self.items():
            if queue_dict.get("pilot_manager") in ["Harvester"] and queue_dict.get("harvester") == harvesterID:
                names.add(queue_name)
        return names

    # is UPS queue
    def is_ups_queue(self, panda_resource):
        panda_queue_dict = self.get(panda_resource)
        if panda_queue_dict is None:
            return False
        if panda_queue_dict.get("capability") == "ucore" and panda_queue_dict.get("workflow") == "pull_ups":
            return True
        return False

    # is grandly unified queue, i.e. runs analysis and production
    def is_grandly_unified_queue(self, panda_resource):
        panda_queue_dict = self.get(panda_resource)
        if panda_queue_dict is None:
            return False
        # initial, temporary nomenclature
        if "grandly_unified" in panda_queue_dict.get("catchall") or panda_queue_dict.get("type") == "unified":
            return True
        return False

    # get harvester params
    def get_harvester_params(self, panda_resource):
        panda_queue_dict = self.get(panda_resource)
        if panda_queue_dict is None:
            return dict()
        else:
            return panda_queue_dict.get("params", dict())

    # get harvester_template
    def get_harvester_template(self, panda_resource):
        panda_queue_dict = self.get(panda_resource)
        if panda_queue_dict is None:
            return None
        else:
            return panda_queue_dict.get("harvester_template", "")

    # get a tuple of type (production, analysis, etc.) and workflow
    def get_type_workflow(self, panda_resource):
        panda_queue_dict = self.get(panda_resource)
        if panda_queue_dict is None:
            pq_type = None
            workflow = None
        else:
            pq_type = panda_queue_dict.get("type")
            if pq_type == "unified":  # use production templates
                pq_type = "production"
            workflow = panda_queue_dict.get("workflow")
        return pq_type, workflow

    def get_prorated_maxwdir_GiB(self, panda_resource, worker_corecount):
        try:
            panda_queue_dict = self.get(panda_resource)
            maxwdir = panda_queue_dict.get("maxwdir") / 1024  # convert to GiB
            corecount = panda_queue_dict.get("corecount")
            if panda_queue_dict.get("capability") == "ucore":
                maxwdir_prorated = maxwdir * worker_corecount / corecount
            else:
                maxwdir_prorated = maxwdir
        except Exception:
            maxwdir_prorated = 0

        return maxwdir_prorated


class GridServicesDict(CachedDictBase):
    """
    Dictionary of grid service info from DB by cacher
    Key is the name of the service (e.g. CE, SE) as in CRIC
    """

    default_cacher_key = "grid_services.json"

    # get the type of a service (e.g. CE, SE)
    def get_type(self, service_name):
        service_dict = self.get(service_name)
        if service_dict is None:
            return None
        return service_dict.get("type")

    # get the flavour of a service (e.g. HTCONDOR-CE, ARC-CE)
    def get_flavour(self, service_name):
        service_dict = self.get(service_name)
        if service_dict is None:
            return None
        return service_dict.get("flavour")

    # get the endpoint of a service
    def get_endpoint(self, service_name):
        service_dict = self.get(service_name)
        if service_dict is None:
            return None
        return service_dict.get("endpoint")

    # get the state of a service (e.g. ACTIVE, DISABLED)
    def get_state(self, service_name):
        service_dict = self.get(service_name)
        if service_dict is None:
            return None
        return service_dict.get("state")
