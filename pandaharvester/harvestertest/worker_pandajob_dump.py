#!/usr/bin/env python
import os
import sys
import optparse
import logging
import sqlite3
import datetime

logger = logging.getLogger(__name__)

# jw_table columns:
# 0|PandaID|integer|0||0
# 1|workerID|integer|0||0
# 2|relationType|text|0||0

# work_table columns:
# 0|workerID|integer|0||0
# 1|batchID|text|0||0
# 2|mapType|text|0||0
# 3|queueName|text|0||0
# 4|status|text|0||0
# 5|hasJob|integer|0||0
# 6|workParams|blob|0||0
# 7|workAttributes|blob|0||0
# 8|eventsRequestParams|blob|0||0
# 9|eventsRequest|integer|0||0
# 10|computingSite|text|0||0
# 11|creationTime|timestamp|0||0
# 12|submitTime|timestamp|0||0
# 13|startTime|timestamp|0||0
# 14|endTime|timestamp|0||0
# 15|nCore|integer|0||0
# 16|walltime|timestamp|0||0
# 17|accessPoint|text|0||0
# 18|modificationTime|timestamp|0||0
# 19|lastUpdate|timestamp|0||0
# 20|eventFeedTime|timestamp|0||0
# 21|lockedBy|text|0||0
# 22|postProcessed|integer|0||0
# 23|nodeID|text|0||0
# 24|minRamCount|integer|0||0
# 25|maxDiskCount|integer|0||0
# 26|maxWalltime|integer|0||0
# 27|killTime|timestamp|0||0
# 28|computingElement|text|0||0


def main():
    """this script grabs the latest workers that have been added to the worker_table, finds their associated panda job ids from the jw_table, then presents how many jobs are in each state for that worker. It also shows the panda jobs which are in the fetched, preparing, and prepared states which have not yet been assigned to a worker."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")

    parser = optparse.OptionParser(description="")
    parser.add_option("-d", "--database-filename", dest="database_filename", help="The Harvester data base file name.")
    parser.add_option(
        "-t",
        "--time-in-hours",
        dest="hours",
        help="this prints the workers last modified in the last N hours. last-n-workers and time-in-hours are mutually exclusive.",
        type="int",
    )
    parser.add_option(
        "-n",
        "--last-n-workers",
        dest="workers",
        help="this prints the last N workers created. last-n-workers and time-in-hours are mutually exclusive.",
        type="int",
    )
    options, args = parser.parse_args()

    manditory_args = [
        "database_filename",
    ]

    for man in manditory_args:
        if options.__dict__[man] is None:
            logger.error("Must specify option: " + man)
            parser.print_help()
            sys.exit(-1)

    if options.hours and options.workers:
        logger.error("can only specify time-in-hours or last-n-workers, not both")
        parser.print_help()
        sys.exit(-1)
    elif not options.hours and not options.workers:
        logger.error("must specify time-in-hours or last-n-workers")
        parser.print_help()
        sys.exit(-1)

    conn = sqlite3.connect(options.database_filename)

    cursor = conn.cursor()

    if options.hours:
        utcnow = datetime.datetime.utcnow() - datetime.timedelta(hours=options.hours)
        utcnow_str = utcnow.strftime("%Y-%d-%m %H:%M:%S")
        work_cmd = 'SELECT workerID,batchID,status FROM work_table WHERE modificationTime > "%s"' % utcnow_str
    elif options.workers:
        work_cmd = "SELECT workerID,batchID,status FROM work_table ORDER BY workerID DESC LIMIT %s" % options.workers
    cursor.execute(work_cmd)

    work_entries = cursor.fetchall()

    for work_entry in work_entries:
        workerID, batchID, workerStatus = work_entry

        jobs_in_state = {}
        jobs_in_substate = {}

        jw_cmd = "SELECT * FROM jw_table WHERE workerID=%s" % workerID

        cursor.execute(jw_cmd)
        jw_entries = cursor.fetchall()

        for jw_entry in jw_entries:
            pandaID, workerID, relationType = jw_entry

            job_cmd = "SELECT status,subStatus FROM job_table WHERE PandaID=%s" % pandaID

            cursor.execute(job_cmd)
            job_info = cursor.fetchall()[0]
            jobStatus, jobSubStatus = job_info
            if jobStatus in jobs_in_state:
                jobs_in_state[jobStatus] += 1
            else:
                jobs_in_state[jobStatus] = 1
            if jobSubStatus in jobs_in_substate:
                jobs_in_substate[jobSubStatus] += 1
            else:
                jobs_in_substate[jobSubStatus] = 1
            # logger.info('pandaID: %s status: %s subStatus: %s',pandaID,status,subStatus)
        string = "job status = ["
        for job_status, count in jobs_in_state.iteritems():
            string += " %s(%s)" % (job_status, count)
        string += "] subStatus = {"
        for job_substatus, count in jobs_in_substate.iteritems():
            string += "%s(%s)" % (job_substatus, count)
        string += "}"
        logger.info("workerID: %s; batchID: %s; worker status: %s; %s", workerID, batchID, workerStatus, string)

    cmd = 'SELECT PandaID,status,subStatus FROM job_table WHERE subStatus="fetched"'
    cursor.execute(cmd)
    jobs = cursor.fetchall()
    logger.info("panda jobs in fetched: %s", len(jobs))

    cmd = 'SELECT PandaID,status,subStatus FROM job_table WHERE subStatus="preparing"'
    cursor.execute(cmd)
    jobs = cursor.fetchall()
    logger.info("panda jobs in preparing: %s", len(jobs))

    cmd = 'SELECT PandaID,status,subStatus FROM job_table WHERE subStatus="prepared"'
    cursor.execute(cmd)
    jobs = cursor.fetchall()
    logger.info("panda jobs in prepared: %s", len(jobs))


if __name__ == "__main__":
    main()
