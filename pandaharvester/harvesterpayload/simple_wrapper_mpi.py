#!/usr/bin/env python
import sys
import os
import time
import json
from socket import gethostname
from subprocess import call
import logging
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
max_rank = comm.Get_size()

logger = logging.getLogger('Rank {0}'.format(rank))
logger.setLevel(logging.DEBUG)
debug_h = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
debug_h.setFormatter(formatter)
debug_h.setLevel(logging.DEBUG)
error_h = logging.StreamHandler(stream=sys.stderr)
error_h.setFormatter(formatter)
error_h.setLevel(logging.ERROR)
logger.addHandler(error_h)
logger.addHandler(debug_h)

def main():
    start_g = time.time()
    start_g_str = time.asctime(time.localtime(start_g))
    hostname = gethostname()
    logger.info("Script statrted at {0} on {1}".format(start_g_str, hostname))
    # Get a file name with job descriptions
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = 'worker_pandaids.json'
    try:
        in_file = open(input_file)
        panda_ids = json.load(in_file)
        in_file.close()
    except IOError as (errno, strerror):
        logger.critical("I/O error({0}): {1}".format(errno, strerror))
        logger.critical("Exit from rank")
        return errno

    logger.debug("Collected list of jobs {0}".format(panda_ids))
    logger.error("Only for test")
    # PandaID of the job for the command
    job_id = panda_ids[0]
    logger.debug("Job [{0}] will be processed".format(job_id))
    os.chdir(str(job_id))

    try:
        job_file = open("HPCJobs.json")
        jobs = json.load(job_file)
        job_file.close()
    except IOError as (errno, strerror):
        logger.critical("I/O error({0}): {1}".format(errno, strerror))
        logger.critical("Unable to open 'HPCJobs.json'")
        return errno

    job = jobs[str(job_id)]

    my_command = " ".join([job['transformation'],job['jobPars']])
    my_command = my_command.strip()
    logger.debug("Going to launch: {0}".format(my_command))
    payloadstdout = open("stdout.txt", "w")
    payloadstderr = open("stderr.txt", "w")

    start_time = time.asctime(time.localtime(time.time()))
    t0 = os.times()
    exit_code = call(my_command, stdout=payloadstdout, stderr=payloadstderr, shell=True)
    t1 = os.times()
    end_time = time.asctime(time.localtime(time.time()))
    t = map(lambda x, y: x - y, t1, t0)
    t_tot = reduce(lambda x, y: x + y, t[2:3])

    payloadstdout.close()
    payloadstderr.close()
    logger.info("Payload exit code: {0}".format(exit_code))
    logger.info("CPU comsumption time: {0}".format(t_tot))
    logger.info("Start time: {0}".format(start_time))
    logger.info("End time: {0}".format(end_time))

    report = open("rank_report.txt", "w")
    report.write("cpuConsumptionTime: %s\n" % t_tot)
    report.write("exitCode: %s" % exit_code)
    report.close()
    logger.info("All done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
