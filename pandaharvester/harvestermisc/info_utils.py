from future.utils import iteritems

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.db_interface import DBInterface

harvesterID = harvester_config.master.harvester_id

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
        # offline if not with harvester or not of this harvester instance
        if panda_queue_dict.get('pilot_manager') not in ['Harvester'] \
            or panda_queue_dict.get('harvester') != harvesterID:
            return 'offline'
        return panda_queue_dict['status']

    # get all queue names of this harvester instance
    def get_all_queue_names(self):
        names = set()
        for queue_name, queue_dict in iteritems(self):
            if queue_dict.get('pilot_manager') in ['Harvester'] \
                and queue_dict.get('harvester') == harvesterID:
                names.add(queue_name)
        return names

    # is UPS queue
    def is_ups_queue(self, panda_resource):
        panda_queue_dict = self.get(panda_resource)
        if panda_queue_dict is None:
            return False
        if panda_queue_dict.get('capability') == 'ucore' and \
                'Pull' in panda_queue_dict.get('catchall').split(','):
            return True
        return False

    # get harvester params
    def get_harvester_params(self, panda_resource):
        panda_queue_dict = self.get(panda_resource)
        if panda_queue_dict is None:
            return dict()
        else:
            return panda_queue_dict.get('params', dict())

    # get harvester_template
    def get_harvester_template(self, panda_resource):
        panda_queue_dict = self.get(panda_resource)
        if panda_queue_dict is None:
            return None
        else:
            return panda_queue_dict.get('harvester_template', '')

    # get a tuple of type (production, analysis, etc.) and workflow
    def get_type_workflow(self, panda_resource):
        panda_queue_dict = self.get(panda_resource)
        if panda_queue_dict is None:
            pq_type = None
            workflow = None
        else:
            pq_type = panda_queue_dict.get('type')
            workflow = panda_queue_dict.get('workflow')
        return pq_type, workflow
