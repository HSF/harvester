from pandaharvester.harvestermisc.info_utils import PandaQueuesDict


class PandaQueuesDictK8s(PandaQueuesDict):
    def get_k8s_scheduler_settings(self, panda_resource):
        # this is how the affinity settings are declared in CRIC
        key_affinity = "k8s.scheduler.use_score_mcore_affinity"
        key_anti_affinity = "k8s.scheduler.use_score_mcore_anti_affinity"

        params = self.get_harvester_params(panda_resource)
        ret_map = {}

        try:
            ret_map["use_affinity"] = params[key_affinity]
        except KeyError:
            # return default value
            ret_map["use_affinity"] = True

        try:
            ret_map["use_anti_affinity"] = params[key_anti_affinity]
        except KeyError:
            # return default value
            ret_map["use_anti_affinity"] = False

        # this is how the affinity settings are declared in CRIC
        key_priority_class = "k8s.scheduler.priorityClassName"
        key_priority_class_score = "k8s.scheduler.priorityClassName.score"
        key_priority_class_score_himem = "k8s.scheduler.priorityClassName.score_himem"
        key_priority_class_mcore = "k8s.scheduler.priorityClassName.mcore"
        key_priority_class_mcore_himem = "k8s.scheduler.priorityClassName.mcore_himem"

        ret_map["priority_class"] = params.get(key_priority_class, None)
        ret_map["priority_class_score"] = params.get(key_priority_class_score, None)
        ret_map["priority_class_score_himem"] = params.get(key_priority_class_score_himem, None)
        ret_map["priority_class_mcore"] = params.get(key_priority_class_mcore, None)
        ret_map["priority_class_mcore_himem"] = params.get(key_priority_class_mcore_himem, None)

        return ret_map

    def get_k8s_resource_settings(self, panda_resource):
        params = self.get_harvester_params(panda_resource)
        ret_map = {}

        # this is how the CPU parameters are declared in CRIC
        key_cpu_scheduling_ratio = "k8s.resources.requests.cpu_scheduling_ratio"
        ret_map["cpu_scheduling_ratio"] = params.get(key_cpu_scheduling_ratio, 90)

        # this is how the memory parameters are declared in CRIC
        key_memory_limit = "k8s.resources.limits.use_memory_limit"
        key_memory_limit_safety_factor = "k8s.resources.limits.memory_limit_safety_factor"
        key_memory_limit_min_offset = "k8s.resources.limits.memory_limit_min_offset"
        key_memory_scheduling_ratio = "k8s.resources.requests.memory_scheduling_ratio"

        ret_map["use_memory_limit"] = params.get(key_memory_limit, False)
        ret_map["memory_limit_safety_factor"] = params.get(key_memory_limit_safety_factor, 100)
        ret_map["memory_limit_min_offset"] = params.get(key_memory_limit_min_offset, 0)  # in MiB to be consistent with minRamCount
        ret_map["memory_scheduling_ratio"] = params.get(key_memory_scheduling_ratio, 100)

        # this is how the ephemeral storage parameters are declared in CRIC
        key_ephemeral_storage = "k8s.resources.use_ephemeral_storage_resource_specs"
        key_ephemeral_storage_resources_offset = "k8s.resources.ephemeral_storage_offset"
        key_ephemeral_storage_limit_safety_factor = "k8s.resources.limits.ephemeral_storage_limit_safety_factor"

        ret_map["use_ephemeral_storage"] = params.get(key_ephemeral_storage, True)  # use ephemeral storage unless explicitly disabled
        ret_map["ephemeral_storage_limit_safety_factor"] = params.get(key_ephemeral_storage_limit_safety_factor, 100)
        ret_map["ephemeral_storage_offset"] = params.get(key_ephemeral_storage_resources_offset, 0)  # should come in MiB

        # decide whether to kill on maxtime
        use_active_deadline_seconds = "k8s.use_active_deadline_seconds"

        ret_map["use_active_deadline_seconds"] = params.get(use_active_deadline_seconds, True)  # kill on max time

        return ret_map

    def get_k8s_namespace(self, panda_resource):
        default_namespace = "default"

        # 1. check if there is an associated CE and use the queue name as namespace
        panda_queue_dict = self.get(panda_resource, {})
        try:
            namespace = panda_queue_dict["queues"][0]["ce_queue_name"]
            return namespace
        except (KeyError, TypeError, IndexError, ValueError):
            pass

        # 2. alternatively, check if namespace defined in the associated parameter section
        key_namespace = "k8s.namespace"
        params = self.get_harvester_params(panda_resource)

        try:
            namespace = params[key_namespace]
        except KeyError:
            # return default value
            namespace = default_namespace

        return namespace

    def get_k8s_host_image(self, panda_resource):
        # check if host_image defined in the associated parameter section
        key_host_image = "k8s.host_image"
        params = self.get_harvester_params(panda_resource)
        host_image = params.get(key_host_image, None)

        return host_image

    def get_k8s_pilot_dir(self, panda_resource):
        # TODO: check if it can be replaced by an existing PQ field like tmpdir or wntmpdir
        # check if pilot_dir_mount defined in the associated parameter section
        key_pilot_dir = "k8s.volumes.pilot_dir_mount"
        params = self.get_harvester_params(panda_resource)
        pilot_dir = params.get(key_pilot_dir, "/pilotdir")

        return pilot_dir

    def get_k8s_annotations(self, panda_resource):
        # check if there are annotations to be specified
        key_safe_to_evict = "k8s.annotations.safe_to_evict"

        params = self.get_harvester_params(panda_resource)
        safe_to_evict = params.get(key_safe_to_evict, None)

        return safe_to_evict

    def get_k8s_pilot_proxy_check(self, panda_resource):
        # by default we tell the pilot not to run the proxy checks
        # but some sites insist on activating it
        key_pilot_proxy_check = "k8s.pilot_proxy_check"

        params = self.get_harvester_params(panda_resource)
        pilot_proxy_check = params.get(key_pilot_proxy_check, False)

        return pilot_proxy_check
