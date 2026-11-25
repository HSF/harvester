"""
database connection

The methods are implemented in db_proxy_mods/; DBProxy composes them.
"""

from pandaharvester.harvestercore.db_proxy_mods import (
    file_module,
    job_module,
    job_worker_module,
    misc_module,
    queue_module,
    schema_module,
    stats_module,
    worker_check_module,
    worker_module,
)

# re-exported for backward compatibility
from pandaharvester.harvestercore.db_proxy_mods.base_module import (  # noqa: F401
    _logger,
    cacheTableName,
    commandTableName,
    conLock,
    diagTableName,
    eventTableName,
    fileTableName,
    jobTableName,
    jobWorkerTableName,
    pandaQueueTableName,
    processLockTableName,
    queueConfigDumpTableName,
    seqNumberTableName,
    serviceMetricsTableName,
    workTableName,
)


# connection class
class DBProxy(
    schema_module.SchemaModule,
    job_module.JobModule,
    worker_module.WorkerModule,
    job_worker_module.JobWorkerModule,
    queue_module.QueueModule,
    stats_module.StatsModule,
    file_module.FileModule,
    misc_module.MiscModule,
    worker_check_module.WorkerCheckModule,
):
    pass
