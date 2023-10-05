import os
import sys
import time
import json
import random
import argparse
import logging

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore import fifos as harvesterFifos
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestermisc.selfcheck import harvesterPackageInfo

# === Logger ===================================================


def setupLogger(logger):
    logger.setLevel(logging.DEBUG)
    hdlr = logging.StreamHandler()

    def emit_decorator(fn):
        def func(*args):
            levelno = args[0].levelno
            if levelno >= logging.CRITICAL:
                color = "\033[35;1m"
            elif levelno >= logging.ERROR:
                color = "\033[31;1m"
            elif levelno >= logging.WARNING:
                color = "\033[33;1m"
            elif levelno >= logging.INFO:
                color = "\033[32;1m"
            elif levelno >= logging.DEBUG:
                color = "\033[36;1m"
            else:
                color = "\033[0m"
            formatter = logging.Formatter("{0}[%(asctime)s %(levelname)s] %(message)s\033[0m".format(color))
            hdlr.setFormatter(formatter)
            return fn(*args)

        return func

    hdlr.emit = emit_decorator(hdlr.emit)
    logger.addHandler(hdlr)


mainLogger = logging.getLogger("HarvesterAdminTool")
setupLogger(mainLogger)

# === Operation functions ========================================================


def json_print(data):
    print(json.dumps(data, sort_keys=True, indent=4))


def multithread_executer(func, n_object, n_thread):
    with ThreadPoolExecutor(n_thread) as _pool:
        retIterator = _pool.map(func, range(n_object))
    return retIterator


def get_harvester_attributes():
    attr_list = [
        "harvesterID",
        "version",
        "commit_info",
        "harvester_config",
    ]
    return attr_list


def repopulate_fifos(*names):
    fifo_class_name_map = {
        "monitor": "MonitorFIFO",
    }
    if len(names) > 0:
        fifo_class_name_list = [fifo_class_name_map.get(name) for name in names]
    else:
        fifo_class_name_list = fifo_class_name_map.values()
    for fifo_class_name in fifo_class_name_list:
        if fifo_class_name is None:
            continue
        fifo = getattr(harvesterFifos, fifo_class_name)()
        if not fifo.enabled:
            continue
        fifo.populate(clear_fifo=True)
        print("Repopulated {0} fifo".format(fifo.titleName))


# TODO

# === Command functions ========================================================


def test(arguments):
    mainLogger.critical("Harvester Admin Tool: test CRITICAL")
    mainLogger.error("Harvester Admin Tool: test ERROR")
    mainLogger.warning("Harvester Admin Tool: test WARNING")
    mainLogger.info("Harvester Admin Tool: test INFO")
    mainLogger.debug("Harvester Admin Tool: test DEBUG")
    print("Harvester Admin Tool: test")


def get(arguments):
    attr = arguments.attribute
    hpi = harvesterPackageInfo(None)
    if attr not in get_harvester_attributes():
        mainLogger.error("Invalid attribute: {0}".format(attr))
        return
    elif attr == "version":
        print(hpi.version)
    elif attr == "commit_info":
        print(hpi.commit_info)
    elif attr == "harvesterID":
        print(harvester_config.master.harvester_id)
    elif attr == "harvester_config":
        json_print(harvester_config.config_dict)


def fifo_benchmark(arguments):
    n_object = arguments.n_object
    n_thread = arguments.n_thread
    mq = harvesterFifos.BenchmarkFIFO()
    sw = core_utils.get_stopwatch()
    sum_dict = {
        "put_n": 0,
        "put_time": 0.0,
        "get_time": 0.0,
        "get_protective_time": 0.0,
        "clear_time": 0.0,
    }

    def _put_object(i_index):
        workspec = WorkSpec()
        workspec.workerID = i_index
        data = {"random": [(i_index**2) % 2**16, random.random()]}
        workspec.workAttributes = data
        mq.put(workspec)

    def _get_object(i_index):
        return mq.get(timeout=3, protective=False)

    def _get_object_protective(i_index):
        return mq.get(timeout=3, protective=True)

    def put_test():
        sw.reset()
        multithread_executer(_put_object, n_object, n_thread)
        sum_dict["put_time"] += sw.get_elapsed_time_in_sec(True)
        sum_dict["put_n"] += 1
        print("Put {0} objects by {1} threads".format(n_object, n_thread) + sw.get_elapsed_time())
        print("Now fifo size is {0}".format(mq.size()))

    def get_test():
        sw.reset()
        multithread_executer(_get_object, n_object, n_thread)
        sum_dict["get_time"] = sw.get_elapsed_time_in_sec(True)
        print("Get {0} objects by {1} threads".format(n_object, n_thread) + sw.get_elapsed_time())
        print("Now fifo size is {0}".format(mq.size()))

    def get_protective_test():
        sw.reset()
        multithread_executer(_get_object_protective, n_object, n_thread)
        sum_dict["get_protective_time"] = sw.get_elapsed_time_in_sec(True)
        print("Get {0} objects protective dequeue by {1} threads".format(n_object, n_thread) + sw.get_elapsed_time())
        print("Now fifo size is {0}".format(mq.size()))

    def clear_test():
        sw.reset()
        mq.fifo.clear()
        sum_dict["clear_time"] = sw.get_elapsed_time_in_sec(True)
        print("Cleared fifo" + sw.get_elapsed_time())
        print("Now fifo size is {0}".format(mq.size()))

    # Benchmark
    print("Start fifo benchmark ...")
    mq.fifo.clear()
    print("Cleared fifo")
    put_test()
    get_test()
    put_test()
    get_protective_test()
    put_test()
    clear_test()
    print("Finished fifo benchmark")
    # summary
    print("Summary:")
    print("FIFO plugin is: {0}".format(mq.fifo.__class__.__name__))
    print("Benchmark with {0} objects by {1} threads".format(n_object, n_thread))
    print("Put            : {0:.3f} ms / obj".format(1000.0 * sum_dict["put_time"] / (sum_dict["put_n"] * n_object)))
    print("Get            : {0:.3f} ms / obj".format(1000.0 * sum_dict["get_time"] / n_object))
    print("Get protective : {0:.3f} ms / obj".format(1000.0 * sum_dict["get_protective_time"] / n_object))
    print("Clear          : {0:.3f} ms / obj".format(1000.0 * sum_dict["clear_time"] / n_object))


def fifo_repopulate(arguments):
    if "ALL" in arguments.name_list:
        repopulate_fifos()
    else:
        repopulate_fifos(*arguments.name_list)


def cacher_refresh(arguments):
    from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
    from pandaharvester.harvesterbody.cacher import Cacher

    communicatorPool = CommunicatorPool()
    cacher = Cacher(communicatorPool)
    cacher.execute(force_update=True, skip_lock=True, n_thread=4)


def qconf_list(arguments):
    from pandaharvester.harvesterscripts import queue_config_tool

    if arguments.all:
        queue_config_tool.list_config_ids()
    else:
        queue_config_tool.list_active_queues()


def qconf_refresh(arguments):
    from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper

    qcm = QueueConfigMapper()
    qcm._update_last_reload_time()
    qcm.lastUpdate = None
    qcm.load_data(refill_table=arguments.refill)


def qconf_dump(arguments):
    from pandaharvester.harvesterscripts import queue_config_tool

    to_print = not arguments.json
    try:
        if arguments.id_list:
            res_list = [vars(queue_config_tool.dump_queue_with_config_id(configID, to_print)) for configID in arguments.id_list]
            resObj = {obj.get("queueName"): obj for obj in res_list}
        elif arguments.all:
            res_list = queue_config_tool.dump_all_active_queues(to_print)
            if res_list is None or to_print:
                resObj = {}
            else:
                resObj = {vars(qm).get("queueName", ""): vars(qm) for qm in res_list}
        else:
            resObj = {queue: vars(queue_config_tool.dump_active_queue(queue, to_print)) for queue in arguments.queue_list}
    except TypeError as e:
        if str(e) == "vars() argument must have __dict__ attribute":
            resObj = {}
        else:
            raise
    if arguments.json:
        json_print(resObj)


def qconf_purge(arguments):
    queueName = arguments.queue
    dbProxy = DBProxy()
    retVal = dbProxy.purge_pq(queueName)
    if retVal:
        print("Purged {0} from harvester DB".format(queueName))
    else:
        mainLogger.critical("Failed to purge {0} . See panda-db_proxy.log".format(queueName))


def kill_workers(arguments):
    status_in = "ALL" if (len(arguments.status) == 1 and arguments.status[0] == "ALL") else arguments.status
    computingSite_in = "ALL" if (len(arguments.sites) == 1 and arguments.sites[0] == "ALL") else arguments.sites
    computingElement_in = "ALL" if (len(arguments.ces) == 1 and arguments.ces[0] == "ALL") else arguments.ces
    submissionHost_in = "ALL" if (len(arguments.submissionhosts) == 1 and arguments.submissionhosts[0] == "ALL") else arguments.submissionhosts
    dbProxy = DBProxy()
    retVal = dbProxy.mark_workers_to_kill_by_query(
        {"status": status_in, "computingSite": computingSite_in, "computingElement": computingElement_in, "submissionHost": submissionHost_in}
    )
    if retVal is not None:
        msg_temp = (
            "Sweeper will soon kill {n_workers} workers, with "
            "status in {status_in}, "
            "computingSite in {computingSite_in}, "
            "computingElement in {computingElement_in}, "
            "submissionHost in {submissionHost_in}"
        )
        print(
            msg_temp.format(
                n_workers=retVal,
                status_in=status_in,
                computingSite_in=computingSite_in,
                computingElement_in=computingElement_in,
                submissionHost_in=submissionHost_in,
            )
        )
    else:
        mainLogger.critical("Failed to kill workers. See panda-db_proxy.log")


def query_workers(arguments):
    dbProxy = DBProxy()
    try:
        if arguments.all:
            res_obj = dbProxy.get_worker_stats_full()
        else:
            res_obj = dbProxy.get_worker_stats_full(filter_site_list=arguments.queue_list)
        json_print(res_obj)
    except TypeError as e:
        raise


# === Command map =======================================================


commandMap = {
    # test commands
    "test": test,
    # get commands
    "get": get,
    # fifo commands
    "fifo_benchmark": fifo_benchmark,
    "fifo_repopulate": fifo_repopulate,
    # cacher commands
    "cacher_refresh": cacher_refresh,
    # qconf commands
    "qconf_list": qconf_list,
    "qconf_dump": qconf_dump,
    "qconf_refresh": qconf_refresh,
    "qconf_purge": qconf_purge,
    # kill commands
    "kill_workers": kill_workers,
    # query commands
    "query_workers": query_workers,
}

# === Main ======================================================


def main():
    # main parser
    oparser = argparse.ArgumentParser(prog="harvester-admin", add_help=True)
    subparsers = oparser.add_subparsers()
    oparser.add_argument("-v", "--verbose", "--debug", action="store_true", dest="debug", help="Print more verbose output. (Debug mode !)")

    # test command
    test_parser = subparsers.add_parser("test", help="for testing only")
    test_parser.set_defaults(which="test")

    # get command
    get_parser = subparsers.add_parser("get", help="get attributes of this harvester")
    get_parser.set_defaults(which="get")
    get_parser.add_argument("attribute", type=str, action="store", metavar="<attribute>", choices=get_harvester_attributes(), help="attribute")

    # fifo parser
    fifo_parser = subparsers.add_parser("fifo", help="fifo related")
    fifo_subparsers = fifo_parser.add_subparsers()
    # fifo benchmark command
    fifo_benchmark_parser = fifo_subparsers.add_parser("benchmark", help="benchmark fifo backend")
    fifo_benchmark_parser.set_defaults(which="fifo_benchmark")
    fifo_benchmark_parser.add_argument("-n", type=int, dest="n_object", action="store", default=500, metavar="<N>", help="Benchmark with N objects")
    fifo_benchmark_parser.add_argument("-t", type=int, dest="n_thread", action="store", default=1, metavar="<N>", help="Benchmark with N threads")
    # fifo repopuate command
    fifo_repopulate_parser = fifo_subparsers.add_parser("repopulate", help="Repopulate agent fifo")
    fifo_repopulate_parser.set_defaults(which="fifo_repopulate")
    fifo_repopulate_parser.add_argument(
        "name_list", nargs="+", type=str, action="store", metavar="<agent_name>", help='Name of agent fifo, e.g. "monitor" ("ALL" for all)'
    )

    # cacher parser
    cacher_parser = subparsers.add_parser("cacher", help="cacher related")
    cacher_subparsers = cacher_parser.add_subparsers()
    # cacher refresh command
    cacher_refresh_parser = cacher_subparsers.add_parser("refresh", help="refresh cacher immediately")
    cacher_refresh_parser.set_defaults(which="cacher_refresh")

    # qconf (queue configuration) parser
    qconf_parser = subparsers.add_parser("qconf", help="queue configuration")
    qconf_subparsers = qconf_parser.add_subparsers()
    # qconf list command
    qconf_list_parser = qconf_subparsers.add_parser("list", help="List queues. Only active queues listed by default")
    qconf_list_parser.set_defaults(which="qconf_list")
    qconf_list_parser.add_argument("-a", "--all", dest="all", action="store_true", help="List name and configID of all queues")
    # qconf dump command
    qconf_dump_parser = qconf_subparsers.add_parser("dump", help="Dump queue configurations")
    qconf_dump_parser.set_defaults(which="qconf_dump")
    qconf_dump_parser.add_argument("-J", "--json", dest="json", action="store_true", help="Dump configuration in JSON format")
    qconf_dump_parser.add_argument("-a", "--all", dest="all", action="store_true", help="Dump configuration of all active queues")
    qconf_dump_parser.add_argument("queue_list", nargs="*", type=str, action="store", metavar="<queue_name>", help="Name of active queue")
    qconf_dump_parser.add_argument(
        "-i", "--id", dest="id_list", nargs="+", type=int, action="store", metavar="<configID>", help="Dump configuration of queue with configID"
    )
    # qconf refresh command
    qconf_refresh_parser = qconf_subparsers.add_parser("refresh", help="refresh queue configuration immediately")
    qconf_refresh_parser.set_defaults(which="qconf_refresh")
    qconf_refresh_parser.add_argument("-R", "--refill", dest="refill", action="store_true", help="Refill pq_table before refresh (cleaner)")
    # qconf purge command
    qconf_purge_parser = qconf_subparsers.add_parser("purge", help="Purge the queue thoroughly from harvester DB (Be careful !!)")
    qconf_purge_parser.set_defaults(which="qconf_purge")
    qconf_purge_parser.add_argument("queue", type=str, action="store", metavar="<queue_name>", help="Name of panda queue to purge")

    # kill parser
    kill_parser = subparsers.add_parser("kill", help="kill something alive")
    kill_subparsers = kill_parser.add_subparsers()
    # kill workers command
    kill_workers_parser = kill_subparsers.add_parser("workers", help="Kill active workers by query")
    kill_workers_parser.set_defaults(which="kill_workers")
    kill_workers_parser.add_argument(
        "--status",
        nargs="+",
        dest="status",
        action="store",
        metavar="<status>",
        default=["submitted"],
        help='worker status (only "submitted", "idle", "running" are valid)',
    )
    kill_workers_parser.add_argument(
        "--sites", nargs="+", dest="sites", action="store", metavar="<site>", required=True, help='site (computingSite); "ALL" for all sites'
    )
    kill_workers_parser.add_argument(
        "--ces", nargs="+", dest="ces", action="store", metavar="<ce>", required=True, help='CE (computingElement); "ALL" for all CEs'
    )
    kill_workers_parser.add_argument(
        "--submissionhosts",
        nargs="+",
        dest="submissionhosts",
        action="store",
        metavar="<submission_host>",
        required=True,
        help='submission host (submissionHost); "ALL" for all submission hosts',
    )

    # query parser
    query_parser = subparsers.add_parser("query", help="query current status about harvester")
    query_subparsers = query_parser.add_subparsers()
    # query worker_stats command
    query_workers_parser = query_subparsers.add_parser("workers", help="Query statistiscs of workers in queues")
    query_workers_parser.set_defaults(which="query_workers")
    query_workers_parser.add_argument("-a", "--all", dest="all", action="store_true", help="Show results of all queues")
    query_workers_parser.add_argument("queue_list", nargs="*", type=str, action="store", metavar="<queue_name>", help="Name of active queue")

    # start parsing
    if len(sys.argv) == 1:
        oparser.print_help()
        sys.exit(1)
    arguments = oparser.parse_args(sys.argv[1:])
    # log level
    if arguments.debug:
        # Debug mode of logger
        mainLogger.setLevel(logging.DEBUG)
    else:
        mainLogger.setLevel(logging.WARNING)
    # Run command functions
    try:
        command = commandMap.get(arguments.which)
    except AttributeError:
        oparser.print_help()
        sys.exit(1)
    try:
        start_time = time.time()
        result = command(arguments)
        end_time = time.time()
        if arguments.debug:
            mainLogger.debug("ARGS: {arguments} ; RESULT: {result} ".format(arguments=arguments, result=result))
            mainLogger.debug("Action completed in {0:.3f} seconds".format(end_time - start_time))
        sys.exit(result)
    except (RuntimeError, NotImplementedError) as e:
        mainLogger.critical("ERROR: {0}".format(e))
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        mainLogger.critical("Command Interrupted !!")
        sys.exit(1)
