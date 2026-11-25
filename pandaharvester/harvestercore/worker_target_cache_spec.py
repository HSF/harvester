"""
Worker target cache class: cached state of the targets that workers are mapped to on the resource
(e.g. batch jobs or pods), as queried from the resource by monitor plugins

One row per target, plus one refresh record per target host (targetID is None) holding the time of the last
successful refresh in updateTime.
"""

from .spec_base import SpecBase


class WorkerTargetCacheSpec(SpecBase):
    # attributes
    attributesWithTypes = (
        "targetKey:text primary key",
        "targetHost:text / index",
        "targetID:text",
        "workerID:integer",
        "statusTime:timestamp",
        "targetAttributes:blob",
        "digest:text",
        "updateTime:timestamp",
    )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)

    # key of a target
    @staticmethod
    def make_key(target_host, target_id):
        return f"{target_host}#{target_id}"

    # key of the refresh record of a target host
    @staticmethod
    def make_refresh_key(target_host):
        return f"{target_host}#"
