"""
worker node and worker node gpu classes

"""

from .spec_base import SpecBase


class WorkerNodeSpec(SpecBase):
    # attributes
    attributesWithTypes = (
        "site:text",  # the ATLAS site
        "host_name:text",
        "cpu_model:text",
        "panda_queue:text",  # the PanDA queue
        "n_logical_cpus:integer",
        "n_sockets:integer",
        "cores_per_socket:integer",
        "threads_per_core:integer",
        "cpu_architecture:text",
        "cpu_architecture_level:text",
        "clock_speed:integer",  # in MHz
        "total_memory:integer",  # in MB
        "total_local_disk:integer",  # in GB
    )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)

    # convert to propagate
    def convert_to_propagate(self):
        """Collect attributes from `attributesWithTypes` and coerce floats to ints."""
        attrs = [spec.split(":", 1)[0] for spec in getattr(self, "attributesWithTypes", ())]

        def _normalize(v):
            return int(v) if isinstance(v, float) else v

        return {name: _normalize(getattr(self, name, None)) for name in attrs}


class WorkerNodeGpuSpec(SpecBase):
    # attributes
    attributesWithTypes = (
        "site:text",  # the ATLAS site
        "host_name:text",
        "vendor:text",
        "model:text",
        "count:integer",
        "vram:integer",  # in MB
        "architecture:text",
        "framework:text",
        "framework_version:text",
        "driver_version:text",
    )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)

    # convert to propagate
    def convert_to_propagate(self):
        """Collect attributes from `attributesWithTypes` and coerce floats to ints."""
        attrs = [spec.split(":", 1)[0] for spec in getattr(self, "attributesWithTypes", ())]
        return {name: _normalize(getattr(self, name, None)) for name in attrs}
