import re
import threading
import time

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.core_utils import SingletonWithID
from pandaharvester.harvestercore.db_interface import DBInterface
from pandaharvester.harvestercore.plugin_base import PluginBase

harvesterID = harvester_config.master.harvester_id
resolver_config = getattr(harvester_config.qconf, "resolverConfig", {})


class PandaQueuesDict(dict, PluginBase, metaclass=SingletonWithID):
    """
    Dictionary of PanDA queue info from DB by cacher
    Key is PanDA Resource name (rather than PanDA Queue name)
    Able to query with either PanDA Queue name or PanDA Resource name
    """

    candidate_per_core_attrs = (
        "maxrss",
        "minrss",
        "maxwdir",
    )

    def __init__(self, **kwargs):
        dict.__init__(self)
        PluginBase.__init__(self, **kwargs)
        self.lock = threading.Lock()
        self.dbInterface = DBInterface()
        self.cacher_key = kwargs.get("cacher_key", "panda_queues.json")
        self.refresh_period = resolver_config.get("refreshPeriod", 300)
        self.last_refresh_ts = 0
        self._refresh()

    def _is_fresh(self):
        now_ts = time.time()
        if self.last_refresh_ts + self.refresh_period > now_ts:
            return True
        return False

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

    def _refresh(self):
        with self.lock:
            if self._is_fresh():
                return
            panda_queues_cache = self.dbInterface.get_cache(self.cacher_key)
            if panda_queues_cache and isinstance(panda_queues_cache.data, dict):
                panda_queues_dict = panda_queues_cache.data
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
                # successfully refreshed from cache
                self.last_refresh_ts = time.time()
            else:
                # not getting cache; shorten next period into 5 sec
                self.last_refresh_ts = time.time() - self.refresh_period + 5

    def to_refresh(func):
        """
        Decorator to refresh
        """

        def wrapped_func(self, *args, **kwargs):
            self._refresh()
            return func(self, *args, **kwargs)

        return wrapped_func

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
