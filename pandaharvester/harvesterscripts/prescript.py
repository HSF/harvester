import os
import sys
import argparse

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc.selfcheck import harvesterPackageInfo
from pandaharvester.harvestercore import fifos


def repopulate_fifos():
    agent_fifo_class_name_tuple = ('MonitorFIFO',)
    for agent_fifo_class_name in agent_fifo_class_name_tuple:
        fifo = getattr(fifos, agent_fifo_class_name)()
        if not fifo.enabled:
            continue
        fifo.populate(clear_fifo=True)
        print('Repopulated {0} fifo'.format(fifo.agentName))


def main():
    oparser = argparse.ArgumentParser(prog='prescript', add_help=True)
    oparser.add_argument('-f', '--local_info_file', action='store', dest='local_info_file', help='path of harvester local info file')

    if len(sys.argv) == 1:
        print('No argument or flag specified. Did nothing')
        sys.exit(0)
    args = oparser.parse_args(sys.argv[1:])

    local_info_file = os.path.normpath(args.local_info_file)

    hpi = harvesterPackageInfo(local_info_file=local_info_file)
    if hpi.package_changed:
        print('Harvester package changed')
        repopulate_fifos()
        hpi.renew_local_info()
    else:
        print('Harvester package unchanged. Skipped')


if __name__ == '__main__':
    main()
