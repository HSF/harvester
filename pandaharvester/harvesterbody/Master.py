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
    def __init__(self,singleMode=False):
        # initialize database and config
        self.singleMode = singleMode
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
        # JobFetcher
        from pandaharvester.harvesterbody.JobFetcher import JobFetcher
        nThr = harvester_config.jobfetch.nThreads
        for iThr in range(nThr):
            thr = JobFetcher(self.communicatorPool,
                             self.queueConfigMapper,
                             singleMode=self.singleMode)
            thr.start()
            thrList.append(thr)
        # Propagator
        from pandaharvester.harvesterbody.Propagator import Propagator
        nThr = harvester_config.prop.nThreads
        for iThr in range(nThr):
            thr = Propagator(self.communicatorPool,
                             singleMode=self.singleMode)
            thr.start()
            thrList.append(thr)
        # join
        for thr in thrList:
            thr.join()



# kill whole process
def catch_sig(sig,frame):
    # kill
    os.killpg(os.getpgrp(),signal.SIGKILL)



# main
if __name__ == "__main__":
    # parse option
    parser = argparse.ArgumentParser()
    parser.add_argument('--pid',action='store',dest='pid',default=None,
                        help='pid filename')
    parser.add_argument('--single',action='store_true',dest='singleMode',default=False,
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
        pidFile = open(options.pid,'w')
        pidFile.write('{0}'.format(os.getpid()))
        pidFile.close()
        # set handler
        signal.signal(signal.SIGINT, catch_sig)
        signal.signal(signal.SIGHUP, catch_sig)
        signal.signal(signal.SIGTERM,catch_sig)
        # start master
        master = Master(singleMode=options.singleMode)
        master.start()
