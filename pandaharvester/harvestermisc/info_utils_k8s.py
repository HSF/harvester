from pandaharvester.harvestermisc.info_utils import PandaQueuesDict


class PandaQueuesDictK8s(PandaQueuesDict):

    def get_k8s_scheduler_settings(self, panda_resource):
        # this is how the affinity settings are declared in CRIC
        key_affinity = 'k8s.scheduler.use_score_mcore_affinity'
        key_anti_affinity = 'k8s.scheduler.use_score_mcore_anti_affinity'

        params = self.get_harvester_params(panda_resource)
        ret_map = {}

        try:
            ret_map['use_affinity'] = params[key_affinity]
        except KeyError:
            # return default value
            ret_map['use_affinity'] = True

        try:
            ret_map['use_anti_affinity'] = params[key_anti_affinity]
        except KeyError:
            # return default value
            ret_map['use_anti_affinity'] = False

        # this is how the affinity settings are declared in CRIC
        key_priority_class_score = 'k8s.scheduler.priorityClassName.score'
        key_priority_class_score_himem = 'k8s.scheduler.priorityClassName.score_himem'
        key_priority_class_mcore = 'k8s.scheduler.priorityClassName.mcore'
        key_priority_class_mcore_himem = 'k8s.scheduler.priorityClassName.mcore_himem'

        try:
            ret_map['priority_class_score'] = params[key_priority_class_score]
        except KeyError:
            # return default value
            ret_map['priority_class_score'] = None

        try:
            ret_map['priority_class_score_himem'] = params[key_priority_class_score_himem]
        except KeyError:
            # return default value
            ret_map['priority_class_score_himem'] = None

        try:
            ret_map['priority_class_mcore'] = params[key_priority_class_mcore]
        except KeyError:
            # return default value
            ret_map['priority_class_mcore'] = None

        try:
            ret_map['priority_class_mcore_himem'] = params[key_priority_class_mcore_himem]
        except KeyError:
            # return default value
            ret_map['priority_class_mcore_himem'] = None

        return ret_map

    def get_k8s_resource_settings(self, panda_resource):
        params = self.get_harvester_params(panda_resource)
        ret_map = {}

        # this is how the CPU parameters are declared in CRIC
        key_cpu_scheduling_ratio = 'k8s.resources.requests.cpu_scheduling_ratio'

        try:
            cpu_scheduling_ratio = params[key_cpu_scheduling_ratio]
        except KeyError:
            # return default value
            cpu_scheduling_ratio = 90
        ret_map['cpu_scheduling_ratio'] = cpu_scheduling_ratio

        # this is how the memory parameters are declared in CRIC
        key_memory_limit = 'k8s.resources.limits.use_memory_limit'
        key_memory_limit_safety_factor = 'k8s.resources.limits.memory_limit_safety_factor'
        key_memory_limit_min_offset = 'k8s.resources.limits.memory_limit_min_offset'

        try:
            use_memory_limit = params[key_memory_limit]
        except KeyError:
            # return default value
            use_memory_limit = False
        ret_map['use_memory_limit'] = use_memory_limit

        try:
            memory_limit_safety_factor = params[key_memory_limit_safety_factor]
        except KeyError:
            # return default value
            memory_limit_safety_factor = 100
        ret_map['memory_limit_safety_factor'] = memory_limit_safety_factor

        try:
            memory_limit_min_offset = params[key_memory_limit_min_offset]  # in MiB to be consistent with minRamCount
        except KeyError:
            # return default value
            memory_limit_min_offset = 0
        ret_map['memory_limit_min_offset'] = memory_limit_min_offset

        # this is how the ephemeral storage parameters are declared in CRIC
        key_ephemeral_storage = 'k8s.resources.use_ephemeral_storage_resource_specs'
        key_ephemeral_storage_resources_offset = 'k8s.resources.ephemeral_storage_offset'
        key_ephemeral_storage_limit_safety_factor = 'k8s.resources.limits.ephemeral_storage_limit_safety_factor'

        try:
            use_ephemeral_storage = params[key_ephemeral_storage]
        except KeyError:
            # use ephemeral storage unless explicitly disabled
            use_ephemeral_storage = True
        ret_map['use_ephemeral_storage'] = use_ephemeral_storage

        try:
            ephemeral_storage_limit_safety_factor = params[key_ephemeral_storage_limit_safety_factor]
        except KeyError:
            # return default value
            ephemeral_storage_limit_safety_factor = 100
        ret_map['ephemeral_storage_limit_safety_factor'] = ephemeral_storage_limit_safety_factor

        try:
            ephemeral_storage_offset = params[key_ephemeral_storage_resources_offset]
        except KeyError:
            # return default value
            ephemeral_storage_offset = 0  # should come in MiB
        ret_map['ephemeral_storage_offset'] = ephemeral_storage_offset

        return ret_map

    def get_k8s_namespace(self, panda_resource):
        default_namespace = 'default'

        # 1. check if there is an associated CE and use the queue name as namespace
        panda_queue_dict = self.get(panda_resource, {})
        try:
            namespace = panda_queue_dict['queues'][0]['ce_queue_name']
            return namespace
        except (KeyError, TypeError, IndexError, ValueError):
            pass

        # 2. alternatively, check if namespace defined in the associated parameter section
        key_namespace = 'k8s.namespace'
        params = self.get_harvester_params(panda_resource)

        try:
            namespace = params[key_namespace]
        except KeyError:
            # return default value
            namespace = default_namespace

        return namespace

    def get_k8s_host_image(self, panda_resource):

        # check if host_image defined in the associated parameter section
        key_host_image = 'k8s.host_image'
        params = self.get_harvester_params(panda_resource)

        host_image = None
        try:
            host_image = params[key_host_image]
        except KeyError:
            pass

        return host_image

    def get_k8s_pilot_dir(self, panda_resource):

        # check if host_image defined in the associated parameter section
        key_pilot_dir = 'k8s.volumes.pilot_dir_mount'
        params = self.get_harvester_params(panda_resource)

        pilot_dir = '/pilotdir'
        try:
            pilot_dir = params[key_pilot_dir]
        except KeyError:
            pass

        return pilot_dir