import os
import pwd
import grp
import sys
import signal
import daemon
import argparse
import datetime

from pandaharvester.harvesterconfig import harvester_config


# the master class which runs the main process
class Master:
    # constrictor
    def __init__(self, single_mode=False):
        # initialize database and config
        self.singleMode = single_mode
        from pandaharvester.harvestercore.CommunicatorPool import CommunicatorPool
        self.communicatorPool = CommunicatorPool()
        from pandaharvester.harvestercore.QueueConfigMapper import QueueConfigMapper
        self.queueConfigMapper = QueueConfigMapper()
        from pandaharvester.harvestercore.DBProxy import DBProxy
        dbProxy = DBProxy()
        dbProxy.makeTables(self.queueConfigMapper)

    # main loop
    def start(self):
        # thread list
        thrList = []
        # Credential Manager
        from pandaharvester.harvesterbody.CredManager import CredManager
        thr = CredManager(single_mode=self.singleMode)
        thr.execute()
        thr.start()
        thrList.append(thr)
        # Job Fetcher
        from pandaharvester.harvesterbody.JobFetcher import JobFetcher
        nThr = harvester_config.jobfetcher.nThreads
        for iThr in range(nThr):
            thr = JobFetcher(self.communicatorPool,
                             self.queueConfigMapper,
                             single_mode=self.singleMode)
            thr.start()
            thrList.append(thr)
        # Propagator
        from pandaharvester.harvesterbody.Propagator import Propagator
        nThr = harvester_config.propagator.nThreads
        for iThr in range(nThr):
            thr = Propagator(self.communicatorPool,
                             single_mode=self.singleMode)
            thr.start()
            thrList.append(thr)
        # Monitor
        from pandaharvester.harvesterbody.Monitor import Monitor
        nThr = harvester_config.monitor.nThreads
        for iThr in range(nThr):
            thr = Monitor(self.queueConfigMapper,
                          single_mode=self.singleMode)
            thr.start()
            thrList.append(thr)
        # Preparator
        from pandaharvester.harvesterbody.Preparator import Preparator
        nThr = harvester_config.preparator.nThreads
        for iThr in range(nThr):
            thr = Preparator(self.communicatorPool,
                             self.queueConfigMapper,
                             single_mode=self.singleMode)
            thr.start()
            thrList.append(thr)
        # Submitter
        from pandaharvester.harvesterbody.Submitter import Submitter
        nThr = harvester_config.submitter.nThreads
        for iThr in range(nThr):
            thr = Submitter(self.queueConfigMapper,
                            single_mode=self.singleMode)
            thr.start()
            thrList.append(thr)
        # Stager
        from pandaharvester.harvesterbody.Stager import Stager
        nThr = harvester_config.stager.nThreads
        for iThr in range(nThr):
            thr = Stager(self.queueConfigMapper,
                         single_mode=self.singleMode)
            thr.start()
            thrList.append(thr)
        # EventFeeder
        from pandaharvester.harvesterbody.EventFeeder import EventFeeder
        nThr = harvester_config.eventfeeder.nThreads
        for iThr in range(nThr):
            thr = EventFeeder(self.communicatorPool,
                              self.queueConfigMapper,
                              single_mode=self.singleMode)
            thr.start()
            thrList.append(thr)
        ##################
        # join
        for thr in thrList:
            thr.join()


# kill whole process
def catch_sig(sig, frame):
    # kill
    os.killpg(os.getpgrp(), signal.SIGKILL)


# main
if __name__ == "__main__":
    # parse option
    parser = argparse.ArgumentParser()
    parser.add_argument('--pid', action='store', dest='pid', default=None,
                        help='pid filename')
    parser.add_argument('--single', action='store_true', dest='singleMode', default=False,
                        help='use single mode')

    options = parser.parse_args()
    uid = pwd.getpwnam(harvester_config.master.uname).pw_uid
    gid = grp.getgrnam(harvester_config.master.gname).gr_gid
    timeNow = datetime.datetime.utcnow()
    print "{0} Master: INFO    start".format(str(timeNow))
    # make daemon context
    dc = daemon.DaemonContext(stdout=sys.stdout,
                              stderr=sys.stderr,
                              uid=uid,
                              gid=gid)
    with dc:
        # record PID
        pidFile = open(options.pid, 'w')
        pidFile.write('{0}'.format(os.getpid()))
        pidFile.close()
        # set handler
        signal.signal(signal.SIGINT, catch_sig)
        signal.signal(signal.SIGHUP, catch_sig)
        signal.signal(signal.SIGTERM, catch_sig)
        # start master
        master = Master(single_mode=options.singleMode)
        master.start()
