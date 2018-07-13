from future.utils import iteritems

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.db_interface import DBInterface


class PandaQueuesDict(dict, PluginBase):
    """
    Dictionary of PanDA queue info from DB by cacher
    Able to query with either PanDA Queue name or PanDA Resource name
    """
    def __init__(self, **kwarg):
        dict.__init__(self)
        PluginBase.__init__(self, **kwarg)
        dbInterface = DBInterface()
        cacher_key = kwarg.get('cacher_key', 'panda_queues.json')
        panda_queues_cache = dbInterface.get_cache(cacher_key)
        if panda_queues_cache:
            self.update(panda_queues_cache.data)

    def __getitem__(self, panda_resource):
        if panda_resource in self:
            return dict.__getitem__(self, panda_resource)
        else:
            panda_queue = self.get_panda_queue_name(panda_resource)
            return dict.__getitem__(self, panda_queue)

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
        panda_queues_set = {_k for (_k, _v) in self.items() if _v['panda_resource'] == panda_resource}
        if len(panda_queues_set) >= 1:
            panda_queue = panda_queues_set.pop()
        else:
            panda_queue = panda_resource
        return panda_queue

    # get queue status for auto blacklisting
    def get_queue_status(self, panda_resource):
        panda_queue_dict = self.get(panda_resource)
        if panda_queue_dict is None:
            return None
        # offline if not with harvester
        if panda_queue_dict['pilot_manager'] not in ['Harvester']:
            return 'offline'
        return panda_queue_dict['status']

    # get all queue names
    def get_all_queue_names(self):
        names = dict()
        for queue_name, queue_dict in iteritems(self):
            if queue_dict['pilot_manager'] in ['Harvester']:
                # FIXME once template name is available in AGIS
                names[queue_name] = None
        return names

    # is UPS queue
    def is_ups_queue(self, panda_resource):
        panda_queue_dict = self.get(panda_resource)
        if panda_queue_dict is None:
            return False
        if panda_queue_dict['capability'] == 'ucore' and \
                'Pull' in panda_queue_dict['catchall'].split(','):
            return True
        return False
