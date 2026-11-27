import argparse
import contextlib
import cProfile
import grp
import logging
import os
import pwd
import signal
import socket
import sys
import threading
import time

import daemon.pidfile

try:
    import pprofile
except Exception:
    pass

from pandalogger import logger_config

from pandaharvester import commit_timestamp, panda_pkg_info
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc.apfmon import Apfmon

# logger
_logger = core_utils.setup_logger("master")

# for singleton
master_instance = False
master_lock = threading.Lock()


# the master class which runs the main process
class Master(object):
    # constructor
    def __init__(self, single_mode=False, stop_event=None, daemon_mode=True, pid_file=None):
        # initialize database and config
        self.singleMode = single_mode
        self.stopEvent = stop_event
        self.daemonMode = daemon_mode
        self.pidFile = pid_file
        from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
        from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
        from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper

        self.communicatorPool = CommunicatorPool()
        self.queueConfigMapper = QueueConfigMapper()
        # make tables by the process that gets the lock, or anyway in the last retry
        make_tables_lock_sec = 3
        make_tables_retry = 10
        dbProxy = DBProxy()
        for i_try in range(1, make_tables_retry + 1):
            if dbProxy.get_process_lock("master", os.getpid(), make_tables_lock_sec) or i_try == make_tables_retry:
                dbProxy.make_tables(self.queueConfigMapper, self.communicatorPool)
                break
            time.sleep(0.5)

    # start threads of an agent made with args and single mode; prerun is called with each agent before its thread starts
    def start_agent(self, agent_name, agent_class, *args, n_threads=1, prerun=None):
        self.startLog.debug(f"starting {n_threads} {agent_name} threads")
        for _ in range(n_threads):
            thr = agent_class(*args, single_mode=self.singleMode)
            thr.set_stop_event(self.stopEvent)
            if prerun is not None:
                self.startLog.debug(f"prerunning {agent_name}")
                prerun(thr)
            thr.start()
            self.thrList.append(thr)

    # main loop
    def start(self):
        self.startLog = core_utils.make_logger(_logger, f"pid={os.getpid()}", method_name="start")
        self.thrList = []
        communicator = self.communicatorPool
        mapper = self.queueConfigMapper
        # agents are imported one by one just before they start
        # credential manager, run once before the other agents
        from pandaharvester.harvesterbody.cred_manager import CredManager

        self.start_agent("Credential Manager", CredManager, mapper, prerun=lambda thr: thr.execute())
        # trigger credential renewal in communicator
        self.startLog.debug("triggering credential renewal in communicator")
        communicator.force_credential_renewal()
        from pandaharvester.harvesterbody.command_manager import CommandManager

        self.start_agent("Command Manager", CommandManager, communicator, mapper)
        # cacher, run once before the other agents to fill the cache
        from pandaharvester.harvesterbody.cacher import Cacher

        self.start_agent("Cacher", Cacher, communicator, prerun=lambda thr: thr.execute(force_update=True, skip_lock=True))
        from pandaharvester.harvesterbody.watcher import Watcher

        self.start_agent("Watcher", Watcher)
        from pandaharvester.harvesterbody.job_fetcher import JobFetcher

        self.start_agent("Job Fetcher", JobFetcher, communicator, mapper, n_threads=harvester_config.jobfetcher.nThreads)
        from pandaharvester.harvesterbody.propagator import Propagator

        self.start_agent("Propagator", Propagator, communicator, mapper, n_threads=harvester_config.propagator.nThreads)
        from pandaharvester.harvesterbody.monitor import Monitor

        self.start_agent("Monitor", Monitor, mapper, n_threads=harvester_config.monitor.nThreads)
        from pandaharvester.harvesterbody.preparator import Preparator

        self.start_agent("Preparator", Preparator, communicator, mapper, n_threads=harvester_config.preparator.nThreads)
        from pandaharvester.harvesterbody.submitter import Submitter

        self.start_agent("Submitter", Submitter, mapper, n_threads=harvester_config.submitter.nThreads)
        from pandaharvester.harvesterbody.stager import Stager

        self.start_agent("Stager", Stager, mapper, n_threads=harvester_config.stager.nThreads)
        from pandaharvester.harvesterbody.event_feeder import EventFeeder

        self.start_agent("Event Feeder", EventFeeder, communicator, mapper, n_threads=harvester_config.eventfeeder.nThreads)
        from pandaharvester.harvesterbody.sweeper import Sweeper

        self.start_agent("Sweeper", Sweeper, mapper, n_threads=harvester_config.sweeper.nThreads)
        # file syncer, only when configured
        if hasattr(harvester_config, "file_syncer"):
            from pandaharvester.harvesterbody.file_syncer import FileSyncer

            self.start_agent("File Syncer", FileSyncer, mapper, prerun=lambda thr: thr.execute())
        # service monitor, only when active
        if getattr(getattr(harvester_config, "service_monitor", None), "active", False):
            from pandaharvester.harvesterbody.service_monitor import ServiceMonitor

            self.start_agent("Service Monitor", ServiceMonitor, self.pidFile)
        # report itself to APF Mon
        self.startLog.debug("APF Mon setup")
        apf_mon = Apfmon(mapper)
        apf_mon.create_factory()
        apf_mon.create_labels()
        self.startLog.info("All agents have started")
        if not self.daemonMode:
            return
        # wait on stop event in short steps to stay interruptible by signals, since thread join blocks signal handling
        if not self.singleMode:
            while not self.stopEvent.wait(1):
                pass
        # join
        for thr in self.thrList:
            thr.join()


# wrapper for stderr
class StdErrWrapper(object):
    def write(self, message):
        # set a header and footer to the message to make it easier to parse
        wrapped_message = f"#####START#####\n{message}#####END#####\n"
        _logger.error(wrapped_message)

    def flush(self):
        _logger.handlers[0].flush()

    def fileno(self):
        return _logger.handlers[0].stream.fileno()

    def isatty(self):
        return _logger.handlers[0].stream.isatty()


# profiler
prof = None


# main
def main(daemon_mode=True):
    global prof
    # parse option
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", action="store", dest="pid", default=None, help="pid filename")
    parser.add_argument("--single", action="store_true", dest="singleMode", default=False, help="use single mode")
    parser.add_argument("--hostname_file", action="store", dest="hostNameFile", default=None, help="to record the hostname where harvester is launched")
    parser.add_argument("--rotate_log", action="store_true", dest="rotateLog", default=False, help="rollover log files before launching harvester")
    parser.add_argument("--version", action="store_true", dest="showVersion", default=False, help="show version information and exit")
    parser.add_argument("--profile_output", action="store", dest="profileOutput", default=None, help="filename to save the results of profiler")
    parser.add_argument(
        "--profile_mode", action="store", dest="profileMode", default="s", help="profile mode. s (statistic), d (deterministic), or t (thread-aware)"
    )
    parser.add_argument(
        "--memory_logging", action="store_true", dest="memLogging", default=False, help="add information of memory usage in each logging message"
    )
    parser.add_argument("--foreground", action="store_true", dest="foreground", default=False, help="run in the foreground not to be daemonized")
    options = parser.parse_args()
    # show version information
    if options.showVersion:
        print(f"Version : {panda_pkg_info.release_version}")
        print(f"Last commit : {commit_timestamp.timestamp}")
        return
    # check pid
    if options.pid is not None and os.path.exists(options.pid):
        print(f"ERROR: Cannot start since lock file {options.pid} already exists")
        return
    # uid and gid
    uid = pwd.getpwnam(harvester_config.master.uname).pw_uid
    gid = grp.getgrnam(harvester_config.master.gname).gr_gid
    # get umask
    umask = os.umask(0)
    os.umask(umask)
    # memory logging
    if options.memLogging:
        core_utils.enable_memory_profiling()
    # hostname
    if options.hostNameFile is not None:
        with open(options.hostNameFile, "w") as f:
            f.write(socket.getfqdn())
    # rollover log files
    if options.rotateLog:
        core_utils.do_log_rollover()
        if hasattr(_logger.handlers[0], "doRollover"):
            _logger.handlers[0].doRollover()
    if daemon_mode and not options.foreground:
        # redirect messages to stdout
        stdoutHandler = logging.StreamHandler(sys.stdout)
        stdoutHandler.setFormatter(_logger.handlers[0].formatter)
        _logger.addHandler(stdoutHandler)
        # collect streams not to be closed by daemon
        files_preserve = []
        for loggerName, loggerObj in logging.Logger.manager.loggerDict.items():
            if loggerName.startswith("panda"):
                for handler in loggerObj.handlers:
                    if hasattr(handler, "stream"):
                        files_preserve.append(handler.stream)
        sys.stderr = StdErrWrapper()
        # make daemon context
        dc = daemon.DaemonContext(
            stdout=sys.stdout, stderr=sys.stderr, uid=uid, gid=gid, umask=umask, files_preserve=files_preserve, pidfile=daemon.pidfile.PIDLockFile(options.pid)
        )
    else:
        dc = contextlib.nullcontext()
    with dc:
        # remove pidfile to prevent child processes crashing in atexit
        if not options.singleMode:
            dc.pidfile = None
        if options.pid:
            core_utils.set_file_permission(options.pid)
        core_utils.set_file_permission(logger_config.daemon["logdir"])
        tmp_log = core_utils.make_logger(_logger, f"pid={os.getpid()}", method_name="main")
        tmp_log.info(f"start : version = {panda_pkg_info.release_version}, last_commit = {commit_timestamp.timestamp}")

        # stop event
        stopEvent = threading.Event()

        # profiler
        prof = None
        if options.profileOutput is not None:
            # run with profiler
            if options.profileMode == "d":
                # deterministic
                prof = pprofile.Profile()
            elif options.profileMode == "t":
                # thread-aware
                prof = pprofile.ThreadProfile()
            else:
                # statistic
                prof = cProfile.Profile()

        # post process for profiler
        def disable_profiler():
            global prof
            if prof is not None:
                # disable profiler
                prof.disable()
                # dump results
                prof.dump_stats(options.profileOutput)
                prof = None

        # signal handlers
        def catch_sigkill(sig, frame):
            disable_profiler()
            tmp_log.info(f"got signal={sig} to be killed")
            try:
                os.remove(options.pid)
            except Exception:
                pass
            try:
                if os.getppid() == 1:
                    os.killpg(os.getpgrp(), signal.SIGKILL)
                else:
                    os.kill(os.getpid(), signal.SIGKILL)
            except Exception:
                core_utils.dump_error_message(tmp_log)
                tmp_log.error("failed to be killed")

        def catch_debug(sig, frame):
            tmp_log.info(f"got signal={sig} to go into debugger mode")
            from trepan.api import debug
            from trepan.interfaces import server

            portNum = getattr(harvester_config.master, "debugger_port", 19550)
            connection_opts = {"IO": "TCP", "PORT": portNum}
            interface = server.ServerInterface(connection_opts=connection_opts)
            dbg_opts = {"interface": interface}
            tmp_log.info(f"starting debugger on port {portNum}")
            debug(dbg_opts=dbg_opts)

        # set handler
        if daemon_mode:
            signal.signal(signal.SIGINT, catch_sigkill)
            signal.signal(signal.SIGHUP, catch_sigkill)
            signal.signal(signal.SIGTERM, catch_sigkill)
            signal.signal(signal.SIGALRM, catch_sigkill)
            signal.signal(signal.SIGUSR1, catch_debug)
            signal.signal(signal.SIGUSR2, catch_sigkill)
        # start master
        master = Master(single_mode=options.singleMode, stop_event=stopEvent, daemon_mode=daemon_mode, pid_file=options.pid)
        # enable profiler
        if prof is not None:
            prof.enable()
        # run master
        master.start()
        # disable profiler
        disable_profiler()
        if daemon_mode:
            tmp_log.info("terminated")


if __name__ == "__main__":
    main()
else:
    # started by WSGI
    with master_lock:
        if not master_instance:
            main(daemon_mode=False)
            master_instance = True
    # import application entry for WSGI
    from pandaharvester.harvestermessenger.apache_messenger import application
