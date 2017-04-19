import os
import pwd
import grp
import sys
import signal
import daemon.pidfile
import argparse
import datetime
import threading

from pandaharvester.harvesterconfig import harvester_config


# the master class which runs the main process
class Master:
    # constructor
    def __init__(self, single_mode=False, stop_event=None):
        # initialize database and config
        self.singleMode = single_mode
        self.stopEvent = stop_event
        from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
        self.communicatorPool = CommunicatorPool()
        from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
        self.queueConfigMapper = QueueConfigMapper()
        from pandaharvester.harvestercore.db_proxy import DBProxy
        dbProxy = DBProxy()
        dbProxy.make_tables(self.queueConfigMapper)

    # main loop
    def start(self):
        # send first heartbeat
        self.communicatorPool.is_alive({'startTime': datetime.datetime.utcnow()})
        # thread list
        thrList = []
        # Command manager
        from pandaharvester.harvesterbody.command_manager import CommandManager
        thr = CommandManager(self.communicatorPool, single_mode=True)
        thr.set_stop_event(self.stopEvent)
        thr.run()
        thr.set_single_mode(self.singleMode)
        thr.start()
        thrList.append(thr)
        # Cacher
        from pandaharvester.harvesterbody.cacher import Cacher
        thr = Cacher(single_mode=True)
        thr.set_stop_event(self.stopEvent)
        thr.run()
        thr.set_single_mode(self.singleMode)
        thr.start()
        thrList.append(thr)
        # Credential Manager
        from pandaharvester.harvesterbody.cred_manager import CredManager
        thr = CredManager(single_mode=self.singleMode)
        thr.set_stop_event(self.stopEvent)
        thr.execute()
        thr.start()
        thrList.append(thr)
        # Job Fetcher
        from pandaharvester.harvesterbody.job_fetcher import JobFetcher
        nThr = harvester_config.jobfetcher.nThreads
        for iThr in range(nThr):
            thr = JobFetcher(self.communicatorPool,
                             self.queueConfigMapper,
                             single_mode=self.singleMode)
            thr.set_stop_event(self.stopEvent)
            thr.start()
            thrList.append(thr)
        # Propagator
        from pandaharvester.harvesterbody.propagator import Propagator
        nThr = harvester_config.propagator.nThreads
        for iThr in range(nThr):
            thr = Propagator(self.communicatorPool,
                             single_mode=self.singleMode)
            thr.set_stop_event(self.stopEvent)
            thr.start()
            thrList.append(thr)
        # Monitor
        from pandaharvester.harvesterbody.monitor import Monitor
        nThr = harvester_config.monitor.nThreads
        for iThr in range(nThr):
            thr = Monitor(self.queueConfigMapper,
                          single_mode=self.singleMode)
            thr.set_stop_event(self.stopEvent)
            thr.start()
            thrList.append(thr)
        # Preparator
        from pandaharvester.harvesterbody.preparator import Preparator
        nThr = harvester_config.preparator.nThreads
        for iThr in range(nThr):
            thr = Preparator(self.communicatorPool,
                             self.queueConfigMapper,
                             single_mode=self.singleMode)
            thr.set_stop_event(self.stopEvent)
            thr.start()
            thrList.append(thr)
        # Submitter
        from pandaharvester.harvesterbody.submitter import Submitter
        nThr = harvester_config.submitter.nThreads
        for iThr in range(nThr):
            thr = Submitter(self.queueConfigMapper,
                            single_mode=self.singleMode)
            thr.set_stop_event(self.stopEvent)
            thr.start()
            thrList.append(thr)
        # Stager
        from pandaharvester.harvesterbody.stager import Stager
        nThr = harvester_config.stager.nThreads
        for iThr in range(nThr):
            thr = Stager(self.queueConfigMapper,
                         single_mode=self.singleMode)
            thr.set_stop_event(self.stopEvent)
            thr.start()
            thrList.append(thr)
        # EventFeeder
        from pandaharvester.harvesterbody.event_feeder import EventFeeder
        nThr = harvester_config.eventfeeder.nThreads
        for iThr in range(nThr):
            thr = EventFeeder(self.communicatorPool,
                              self.queueConfigMapper,
                              single_mode=self.singleMode)
            thr.set_stop_event(self.stopEvent)
            thr.start()
            thrList.append(thr)
        ##################
        # loop on stop event to be interruptable since thr.join blocks signal capture in python 2.7
        while True:
            if self.singleMode:
                break
            self.stopEvent.wait(1)
            if self.stopEvent.is_set():
                break
        ##################
        # join
        for thr in thrList:
            thr.join()


# main
if __name__ == "__main__":
    # parse option
    parser = argparse.ArgumentParser()
    parser.add_argument('--pid', action='store', dest='pid', default=None,
                        help='pid filename')
    parser.add_argument('--single', action='store_true', dest='singleMode', default=False,
                        help='use single mode')

    options = parser.parse_args()
    uid = pwd.getpwuid(harvester_config.master.uname).pw_uid
    gid = grp.getgrgid(harvester_config.master.gname).gr_gid
    timeNow = datetime.datetime.utcnow()
    print "{0} Master: INFO    start".format(str(timeNow))

    # make daemon context
    dc = daemon.DaemonContext(stdout=sys.stdout,
                              stderr=sys.stderr,
                              uid=uid,
                              gid=gid,
                              pidfile=daemon.pidfile.PIDLockFile(options.pid))
    with dc:
        # stop event
        stopEvent = threading.Event()

        # signal handlers
        def catch_sigkill(sig, frame):
            os.killpg(os.getpgrp(), signal.SIGKILL)

        def catch_sigterm(sig, frame):
            stopEvent.set()

        # set handler
        signal.signal(signal.SIGINT, catch_sigkill)
        signal.signal(signal.SIGHUP, catch_sigkill)
        signal.signal(signal.SIGTERM, catch_sigterm)
        signal.signal(signal.SIGUSR2, catch_sigterm)

        # start master
        master = Master(single_mode=options.singleMode, stop_event=stopEvent)
        master.start()

    timeNow = datetime.datetime.utcnow()
    print "{0} Master: INFO    terminated".format(str(timeNow))
