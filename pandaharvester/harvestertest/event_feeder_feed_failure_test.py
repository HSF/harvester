# tests for event_feeder when a feed fails.
#
# feed_events used to be checked with "is False", but the middleware messengers return
# None when the remote side is unreachable, so the guard was skipped and the worker was
# committed as fed. The event ranges were already taken from panda by then and only
# lived in a local variable, so they were lost, and the worker was never looked at again
# since get_workers_to_feed_events looks for EV_requestEvents.
#
# run with
#     python -m pytest pandaharvester/harvestertest/event_feeder_feed_failure_test.py

import datetime
import os
import tempfile

import pytest

from pandaharvester.harvesterconfig import harvester_config

# use a throwaway db, not the one this instance is configured with
testDir = tempfile.mkdtemp(prefix="harvester_event_feeder_test_")
harvester_config.db.engine = "sqlite"
harvester_config.db.database_filename = os.path.join(testDir, "harvester_test.db")

# db_proxy_pool first, since core_utils and work_spec import each other
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool  # noqa: E402,F401  isort:skip
from pandaharvester.harvesterbody.event_feeder import EventFeeder  # noqa: E402
from pandaharvester.harvestercore import core_utils  # noqa: E402
from pandaharvester.harvestercore.db_proxy import DBProxy, workTableName  # noqa: E402
from pandaharvester.harvestercore.work_spec import WorkSpec  # noqa: E402
from pandaharvester.harvestermiddleware.rpc_herder import RpcHerder  # noqa: E402

queueName = "TEST-QUEUE"
pandaID = 4242
workerID = 777
lockInterval = harvester_config.eventfeeder.lockInterval

eventRanges = {pandaID: [{"eventRangeID": f"9000-{pandaID}-1-{i}-1", "LFN": "in.pool.root", "startEvent": i, "lastEvent": i} for i in range(1, 6)]}


class DummyQueueConfig:
    def __init__(self):
        self.queueName = queueName
        self.messenger = {"module": "dummy", "name": "dummy"}


class DummyQueueConfigMapper:
    def __init__(self):
        self.queueConfig = DummyQueueConfig()

    def has_queue(self, queue_name, config_id=None):
        return queue_name == queueName

    def get_queue(self, queue_name, config_id=None):
        return self.queueConfig


class DummyCommunicator:
    # keeps every acquire_event_ranges call so the test can count them
    def __init__(self):
        self.calls = []

    def get_event_ranges(self, events_request_params, scattered, base_path):
        self.calls.append(events_request_params)
        return True, eventRanges


class DummyMessenger:
    # returns the scripted values in turn, then True
    def __init__(self, feed_results):
        self.results = list(feed_results)
        self.nFeeds = 0

    def feed_events(self, workspec, events_dict):
        self.nFeeds += 1
        return self.results.pop(0) if self.results else True


def make_feeder(communicator, messenger):
    feeder = EventFeeder(communicator, DummyQueueConfigMapper(), single_mode=True)
    feeder.pluginFactory.get_plugin = lambda plugin_conf: messenger
    return feeder


@pytest.fixture
def dbProxy():
    # a plain DBProxy, not the pool. conLock is module level and a write holds it until
    # commit is called on the same object, while the pool gives out a connection per call.
    # the agent under test still uses the pool, as it does in production
    proxy = DBProxy()
    proxy.make_table(WorkSpec, workTableName)
    proxy.execute(f"DELETE FROM {workTableName} ")
    proxy.commit()
    return proxy


def insert_worker(proxy):
    workSpec = WorkSpec()
    workSpec.workerID = workerID
    workSpec.computingSite = queueName
    workSpec.status = WorkSpec.ST_running
    workSpec.hasJob = 1
    workSpec.mapType = WorkSpec.MT_OneToOne
    workSpec.accessPoint = os.path.join(testDir, "access_point")
    workSpec.eventsRequest = WorkSpec.EV_requestEvents
    workSpec.eventsRequestParams = {pandaID: {"pandaID": pandaID, "taskID": 9000, "jobsetID": 1, "nRanges": 5}}
    workSpec.eventFeedTime = None
    workSpec.modificationTime = core_utils.naive_utcnow()
    sqlI = f"INSERT INTO {workTableName} ({WorkSpec.column_names()}) " + WorkSpec.bind_values_expression()
    proxy.execute(sqlI, workSpec.values_list())
    proxy.commit()
    return workSpec


def get_worker(proxy):
    sqlG = f"SELECT {WorkSpec.column_names()} FROM {workTableName} WHERE workerID=:workerID "
    proxy.execute(sqlG, {":workerID": workerID})
    res = proxy.cur.fetchone()
    proxy.commit()
    workSpec = WorkSpec()
    workSpec.pack(res)
    return workSpec


def expire_feed_lock(proxy, seconds):
    timeOld = core_utils.naive_utcnow() - datetime.timedelta(seconds=seconds)
    sqlU = f"UPDATE {workTableName} SET eventFeedTime=:timeOld WHERE workerID=:workerID "
    proxy.execute(sqlU, {":timeOld": timeOld, ":workerID": workerID})
    proxy.commit()


def test_herder_returns_none_when_not_connected():
    # what the guard has to cope with
    herder = object.__new__(RpcHerder)  # no __init__, so no ssh
    herder.bareFunctions = []
    herder.is_connected = False
    assert herder.feed_events(WorkSpec(), eventRanges) is None


@pytest.mark.parametrize(
    "feedResult, isFed",
    [(True, True), (False, False), (None, False)],
    ids=["ok", "False", "None"],
)
def test_worker_fed_only_when_feed_succeeded(dbProxy, feedResult, isFed):
    insert_worker(dbProxy)
    communicator = DummyCommunicator()
    messenger = DummyMessenger([feedResult])
    make_feeder(communicator, messenger).run()
    assert messenger.nFeeds == 1
    workSpec = get_worker(dbProxy)
    if isFed:
        assert workSpec.eventsRequest == WorkSpec.EV_useEvents
        assert workSpec.eventsRequestParams is None
    else:
        # the ranges are gone if this flips, since nothing wrote them to the event table
        assert workSpec.eventsRequest == WorkSpec.EV_requestEvents
        assert workSpec.eventsRequestParams is not None


def test_worker_is_picked_up_again_after_a_dropped_connection(dbProxy):
    insert_worker(dbProxy)
    communicator = DummyCommunicator()
    messenger = DummyMessenger([None, True])
    # connection is down
    make_feeder(communicator, messenger).run()
    workSpec = get_worker(dbProxy)
    assert workSpec.eventsRequest == WorkSpec.EV_requestEvents
    assert workSpec.eventsRequestParams is not None
    assert len(communicator.calls) == 1
    # skipped while the feed lock is still fresh
    assert dbProxy.get_workers_to_feed_events(10, lockInterval, "eventfeeder-test") == {}
    # and taken again once it is old
    expire_feed_lock(dbProxy, lockInterval + 60)
    workersToFeed = dbProxy.get_workers_to_feed_events(10, lockInterval, "eventfeeder-test")
    assert [w.workerID for w in workersToFeed[queueName]] == [workerID]
    # connection is back
    expire_feed_lock(dbProxy, lockInterval + 60)
    make_feeder(communicator, messenger).run()
    workSpec = get_worker(dbProxy)
    assert workSpec.eventsRequest == WorkSpec.EV_useEvents
    assert workSpec.eventsRequestParams is None
    assert len(communicator.calls) == 2
