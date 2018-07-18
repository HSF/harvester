import os
import sys
import time
import json

import argparse
import logging

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import fifos as harvesterFifos
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy

#=== Logger ===================================================

def setupLogger(logger):
    logger.setLevel(logging.DEBUG)
    hdlr = logging.StreamHandler()
    def emit_decorator(fn):
        def func(*args):
            levelno = args[0].levelno
            if(levelno >= logging.CRITICAL):
                color = '\033[35;1m'
            elif(levelno >= logging.ERROR):
                color = '\033[31;1m'
            elif(levelno >= logging.WARNING):
                color = '\033[33;1m'
            elif(levelno >= logging.INFO):
                color = '\033[32;1m'
            elif(levelno >= logging.DEBUG):
                color = '\033[36;1m'
            else:
                color = '\033[0m'
            formatter = logging.Formatter('{0}[%(asctime)s %(levelname)s] %(message)s\033[0m'.format(color))
            hdlr.setFormatter(formatter)
            return fn(*args)
        return func
    hdlr.emit = emit_decorator(hdlr.emit)
    logger.addHandler(hdlr)


mainLogger = logging.getLogger('HarvesterAdminTool')
setupLogger(mainLogger)

#=== Operation functions ========================================================

def json_print(data):
    print(json.dumps(data, sort_keys=True, indent=4))

def repopulate_fifos(*names):
    agent_fifo_class_name_map = {
            'monitor': 'MonitorFIFO',
        }
    if len(names) > 0:
        agent_fifo_class_name_list = [ agent_fifo_class_name_map.get(name) for name in names ]
    else:
        agent_fifo_class_name_list = agent_fifo_class_name_map.values()
    for agent_fifo_class_name in agent_fifo_class_name_list:
        if agent_fifo_class_name is None:
            continue
        fifo = getattr(harvesterFifos, agent_fifo_class_name)()
        if not fifo.enabled:
            continue
        fifo.populate(clear_fifo=True)
        print('Repopulated {0} fifo'.format(fifo.agentName))

# TODO

#=== Command functions ========================================================

def test(arguments):
    mainLogger.critical('Harvester Admin Tool: test CRITICAL')
    mainLogger.error('Harvester Admin Tool: test ERROR')
    mainLogger.warning('Harvester Admin Tool: test WARNING')
    mainLogger.info('Harvester Admin Tool: test INFO')
    mainLogger.debug('Harvester Admin Tool: test DEBUG')
    print('Harvester Admin Tool: test')

def fifo_repopulate(arguments):
    if 'ALL' in arguments.name_list:
        repopulate_fifos()
    else:
        repopulate_fifos(*arguments.name_list)

def qconf_list(arguments):
    from pandaharvester.harvesterscripts import queue_config_tool
    if arguments.all:
        queue_config_tool.list_config_ids()
    else:
        queue_config_tool.list_active_queues()

def qconf_dump(arguments):
    from pandaharvester.harvesterscripts import queue_config_tool
    to_print = not arguments.json
    try:
        if arguments.id_list:
            res_list = [ vars(queue_config_tool.dump_queue_with_config_id(configID, to_print))
                        for configID in arguments.id_list ]
            resObj = { obj.get('queueName'): obj for obj in res_list }
        elif arguments.all:
            res_dict = queue_config_tool.dump_all_active_queues(to_print)
            resObj = { k: vars(v) for k, v in res_dict.items() }
        else:
            resObj = { queue: vars(queue_config_tool.dump_active_queue(queue, to_print))
                        for queue in arguments.queue_list }
    except TypeError as e:
        if str(e) == 'vars() argument must have __dict__ attribute':
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
        print('Purged {0} from harvester DB'.format(queueName))
    else:
        mainLogger.critical('Failed to purge {0} . See panda-db_proxy.log'.format(queueName))

#=== Command map =======================================================

commandMap = {
            # test commands
            'test': test,
            # fifo commands
            'fifo_repopulate': fifo_repopulate,
            # qconf commands
            'qconf_list': qconf_list,
            'qconf_dump': qconf_dump,
            'qconf_purge': qconf_purge,
            }

#=== Main ======================================================

def main():
    # main parser
    oparser = argparse.ArgumentParser(prog='harvester-admin', add_help=True)
    subparsers = oparser.add_subparsers()
    oparser.add_argument('-v', '--verbose', '--debug', default=False, action='store_true', dest='debug', help="Print more verbose output. (Debug mode !)")
    # test command
    test_parser = subparsers.add_parser('test', help='Test')
    test_parser.set_defaults(which='test')
    # fifo parser
    fifo_parser = subparsers.add_parser('fifo', help='fifo related')
    fifo_subparsers = fifo_parser.add_subparsers()
    # fifo repopuate command
    fifo_repopulate_parser = fifo_subparsers.add_parser('repopulate', help='Repopulate agent fifo')
    fifo_repopulate_parser.set_defaults(which='fifo_repopulate')
    fifo_repopulate_parser.add_argument('name_list', nargs='+', type=str, action='store', metavar='<agent_name>', help='Name of agent fifo, e.g. "monitor" ("ALL" for all)')
    # qconf (queue configuration) parser
    qconf_parser = subparsers.add_parser('qconf', help='queue configuration')
    qconf_subparsers = qconf_parser.add_subparsers()
    # qconf list command
    qconf_list_parser = qconf_subparsers.add_parser('list', help='List queues. Only active queues listed by default')
    qconf_list_parser.set_defaults(which='qconf_list')
    qconf_list_parser.add_argument('-a', '--all', dest='all', action='store_true', help='List name and configID of all queues')
    # qconf dump command
    qconf_dump_parser = qconf_subparsers.add_parser('dump', help='Dump queue configurations')
    qconf_dump_parser.set_defaults(which='qconf_dump')
    qconf_dump_parser.add_argument('-J', '--json', dest='json', action='store_true', help='Dump configuration in JSON format')
    qconf_dump_parser.add_argument('-a', '--all', dest='all', action='store_true', help='Dump configuration of all active queues')
    qconf_dump_parser.add_argument('queue_list', nargs='*', type=str, action='store', metavar='<queue_name>', help='Name of active queue')
    qconf_dump_parser.add_argument('-i', '--id', dest='id_list', nargs='+', type=int, action='store', metavar='<configID>', help='Dump configuration of queue with configID')
    # qconf purge command
    qconf_purge_parser = qconf_subparsers.add_parser('purge', help='Purge the queue thoroughly from harvester DB (Be careful !!)')
    qconf_purge_parser.set_defaults(which='qconf_purge')
    qconf_purge_parser.add_argument('queue', type=str, action='store', metavar='<queue_name>', help='Name of panda queue to purge')


    # start parsing
    if len(sys.argv) == 1:
        oparser.print_help()
        sys.exit(1)
    arguments = oparser.parse_args(sys.argv[1:])
    # log level
    if arguments.debug:
        ## Debug mode of logger
        mainLogger.setLevel(logging.DEBUG)
    else:
        mainLogger.setLevel(logging.WARNING)
    ## Run command functions
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
            mainLogger.debug('ARGS: {arguments} ; RESULT: {result} '.format(arguments=arguments, result=result))
            mainLogger.debug('Action completed in {0:.3f} seconds'.format(end_time - start_time))
        sys.exit(result)
    except (RuntimeError, NotImplementedError) as e:
        mainLogger.critical('ERROR: {0}'.format(e))
        sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        mainLogger.critical('Command Interrupted !!')
        sys.exit(1)
