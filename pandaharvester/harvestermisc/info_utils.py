import time
import threading
from future.utils import iteritems

import six

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.core_utils import SingletonWithID
from pandaharvester.harvestercore.db_interface import DBInterface


harvesterID = harvester_config.master.harvester_id
resolver_config = getattr(harvester_config.qconf, "resolverConfig", {})


class PandaQueuesDict(six.with_metaclass(SingletonWithID, dict, PluginBase)):
    """
    Dictionary of PanDA queue info from DB by cacher
    Key is PanDA Resource name (rather than PanDA Queue name)
    Able to query with either PanDA Queue name or PanDA Resource name
    """

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

    def _refresh(self):
        with self.lock:
            if self._is_fresh():
                return
            panda_queues_cache = self.dbInterface.get_cache(self.cacher_key)
            self.last_refresh_ts = time.time()
            if panda_queues_cache and isinstance(panda_queues_cache.data, dict):
                panda_queues_dict = panda_queues_cache.data
                for k, v in iteritems(panda_queues_dict):
                    try:
                        panda_resource = v["panda_resource"]
                        assert k == v["nickname"]
                    except Exception:
                        pass
                    else:
                        self[panda_resource] = v

    def __getitem__(self, panda_resource):
        if not self._is_fresh():
            self._refresh()
        if panda_resource in self:
            return dict.__getitem__(self, panda_resource)
        else:
            panda_queue = self.get_panda_queue_name(panda_resource)
            return dict.__getitem__(self, panda_queue)

    def get(self, panda_resource, default=None):
        if not self._is_fresh():
            self._refresh()
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
    def get_all_queue_names(self):
        names = set()
        for queue_name, queue_dict in iteritems(self):
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
