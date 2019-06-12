#!/usr/bin/env python
#
# Description:
#   This script is meant to be able to run in any python environment since only builtin libraries are used


import os
import sys
import argparse
import zlib


#=== Command functions =========================================================

def test(arguments):
    print('file_operation: test')

# calculate adler32
def adler32(arguments):
    file_name = arguments.file
    val = 1
    blockSize = 32 * 1024 * 1024
    with open(file_name, 'rb') as fp:
        while True:
            data = fp.read(blockSize)
            if not data:
                break
            val = zlib.adler32(data, val)
    if val < 0:
        val += 2 ** 32
    retVal = hex(val)[2:10].zfill(8).lower()
    print(retVal)

#=== Command map ===============================================================

commandMap = {
            # test commands
            'test': test,
            # adler32 commands
            'adler32': adler32,
            }

#=== Main ======================================================================

def main():
    # main parser
    oparser = argparse.ArgumentParser(prog='harvester-admin', add_help=True)
    subparsers = oparser.add_subparsers()

    # test command
    test_parser = subparsers.add_parser('test', help='for testing only')
    test_parser.set_defaults(which='test')
    # adler32 command
    adler32_parser = subparsers.add_parser('adler32', help='get adler32 checksum of the file')
    adler32_parser.set_defaults(which='adler32')
    adler32_parser.add_argument('file', type=str, action='store', metavar='<file>', help='file path')

    # start parsing
    if len(sys.argv) == 1:
        oparser.print_help()
        sys.exit(1)
    arguments = oparser.parse_args(sys.argv[1:])
    ## Run command functions
    try:
        command = commandMap.get(arguments.which)
    except AttributeError:
        oparser.print_help()
        sys.exit(1)
    try:
        result = command(arguments)
        sys.exit(result)
    except Exception as e:
        sys.stderr.write('{0}: {1}'.format(e.__class__.__name__, e))
        sys.exit(1)


if __name__ == '__main__':
    main()
