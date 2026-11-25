"""
Worker check class: schedule and bookkeeping of the monitor to check a worker

The worker record itself stays in work_table; this only holds when and how to check it.
"""

from .spec_base import SpecBase


class WorkerCheckSpec(SpecBase):
    # priority; higher is claimed first among due workers. Spaced to leave room for new levels
    PRIO_ROUTINE = 0
    PRIO_EVENT = 10
    PRIO_POSTPROC = 20
    PRIO_ADMIN = 30

    # what set the current schedule
    TRIG_submit = "submit"
    TRIG_poll = "poll"
    TRIG_event = "event"
    TRIG_postproc = "postproc"
    TRIG_reconcile = "reconcile"
    TRIG_admin = "admin"
    TRIG_retry = "retry"

    # attributes
    attributesWithTypes = (
        "workerID:integer primary key",
        "priority:integer",
        "priorityTime:timestamp",
        "nextCheckTime:timestamp / index",
        "lockedBy:text",
        "lockedUntil:timestamp",
        "lastCheckTime:timestamp",
        "lastEventTime:timestamp",
        "firstFailTime:timestamp",
        "lastTrigger:text",
    )

    # attributes set to zero instead of None
    zeroAttrs = ("priority",)

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
