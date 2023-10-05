#!/usr/bin/env python
#
# Description:
#   This script is meant to be able to run in any python environment since only builtin libraries are used


import os
import sys
import argparse
import zlib
import tempfile
import re


# === Command functions =========================================================


def test(arguments):
    print("file_operation: test")


# calculate adler32


def adler32(arguments):
    file_name = arguments.file
    val = 1
    blockSize = 32 * 1024 * 1024
    with open(file_name, "rb") as fp:
        while True:
            data = fp.read(blockSize)
            if not data:
                break
            val = zlib.adler32(data, val)
    if val < 0:
        val += 2**32
    retVal = hex(val)[2:10].zfill(8).lower()
    print(retVal)


# write data into a temporary file; return the file name


def write_tmpfile(arguments):
    tmpArgFile = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=arguments.suffix, dir=arguments.dir)
    in_data = re.sub(r"\\\\", r"\\", arguments.data)
    in_data = re.sub(r"\\n", "\n", in_data)
    tmpArgFile.write(in_data)
    tmpArgFile.close()
    print(tmpArgFile.name)


# remove file


def remove_file(arguments):
    os.remove(arguments.path)


# === Command map ===============================================================


commandMap = {
    # test commands
    "test": test,
    # adler32 commands
    "adler32": adler32,
    "write_tmpfile": write_tmpfile,
    "remove_file": remove_file,
}

# === Main ======================================================================


def main():
    # main parser
    oparser = argparse.ArgumentParser(prog="file_operations.py", add_help=True)
    subparsers = oparser.add_subparsers()

    # test command
    test_parser = subparsers.add_parser("test", help="for testing only")
    test_parser.set_defaults(which="test")
    # adler32 command
    adler32_parser = subparsers.add_parser("adler32", help="get adler32 checksum of the file")
    adler32_parser.set_defaults(which="adler32")
    adler32_parser.add_argument("file", type=str, action="store", metavar="<file>", help="file path")
    # write_tmpfile command
    write_tmpfile_parser = subparsers.add_parser("write_tmpfile", help="write data to a temporary file")
    write_tmpfile_parser.set_defaults(which="write_tmpfile")
    write_tmpfile_parser.add_argument("--suffix", type=str, action="store", metavar="<suffix>", default="xxx.tmp", help="name suffix of temporary file")
    write_tmpfile_parser.add_argument("--dir", type=str, action="store", metavar="<dir>", default="/tmp", help="directory of temorary file")
    write_tmpfile_parser.add_argument("data", type=str, action="store", metavar="<data>", help="data to write in temporary file")
    # remove_file command
    remove_file_parser = subparsers.add_parser("remove_file", help="remove a file")
    remove_file_parser.set_defaults(which="remove_file")
    remove_file_parser.add_argument("path", type=str, action="store", metavar="<path>", help="file path")

    # start parsing
    if len(sys.argv) == 1:
        oparser.print_help()
        sys.exit(1)
    arguments = oparser.parse_args(sys.argv[1:])
    # Run command functions
    try:
        command = commandMap.get(arguments.which)
    except AttributeError:
        oparser.print_help()
        sys.exit(1)
    try:
        result = command(arguments)
        sys.exit(result)
    except Exception as e:
        sys.stderr.write("{0}: {1}".format(e.__class__.__name__, e))
        sys.exit(1)


if __name__ == "__main__":
    main()
