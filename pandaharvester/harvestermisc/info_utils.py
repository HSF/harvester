
from pandaharvester.harvestercore.db_interface import DBInterface


class PandaQueuesDict(dict):
    """
    Dictionary of PanDA queue info from DB by cacher
    Able to query with either PanDA Queue name or PanDA Resource name
    """
    def __init__(self, cacher_key='panda_queues.json'):
        dbInterface = DBInterface()
        panda_queues_cache = dbInterface.get_cache(cacher_key)
        if panda_queues_cache:
            self.update(panda_queues_cache.data)

    def __getitem__(self, panda_resource):
        if panda_resource in self:
            return dict.__getitem__(self, panda_resource)
        else:
            panda_queue = self.get_PQ_from_PR(panda_resource)
            return dict.__getitem__(self, panda_queue)

    def get(self, panda_resource, default=None):
        if panda_resource in self:
            return dict.get(self, panda_resource, default)
        else:
            panda_queue = self.get_PQ_from_PR(panda_resource)
            return dict.get(self, panda_queue, default)

    def get_PQ_from_PR(self, panda_resource):
        """
        Return PanDA Queue name with specified PanDA Resource name
        """
        panda_queues_set = { _k for (_k, _v) in self.items() if _v['panda_resource'] == panda_resource }
        if len(panda_queues_set) >= 1:
            panda_queue = panda_queues_set.pop()
        else:
            panda_queue = panda_resource
        return panda_queue
