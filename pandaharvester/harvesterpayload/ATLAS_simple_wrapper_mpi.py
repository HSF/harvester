#!/usr/bin/env python
import sys
import os
import time
import json
import logging
import shutil
from socket import gethostname
from subprocess import call, check_output
from mpi4py import MPI
from pilot.util.filehandling import get_json_dictionary
from pilot.jobdescription import JobDescription #temporary hack

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

# TODO:
# Input file processing
#   - read file attributes from PoolFileCatalog_H.xml
#   - check validity of file by checksum
# Get payload run command
#   - setup string
#   - trf full commandline
# Extract results of execution
#   - collect exit code, message
#   - extract data from jobreport

def get_setup(job):

    # special setup command. should be placed in queue defenition (or job defenition) ?
    setup_commands = ['source /lustre/atlas/proj-shared/csc108/app_dir/pilot/grid_env/external/setup.sh',
                      'source $MODULESHOME/init/bash',
                      'tmp_dirname=/tmp/scratch',
                      'tmp_dirname+="/tmp"',
                      'export TEMP=$tmp_dirname',
                      'export TMPDIR=$TEMP',
                      'export TMP=$TEMP',
                      'export LD_LIBRARY_PATH=/ccs/proj/csc108/AtlasReleases/ldpatch:$LD_LIBRARY_PATH',
                      'export ATHENA_PROC_NUMBER=16',
                      'export G4ATLAS_SKIPFILEPEEK=1',
                      'export PANDA_RESOURCE=\"ORNL_Titan_MCORE\"',
                      'export ROOT_TTREECACHE_SIZE=1',
                      'export RUCIO_APPID=\"simul\"',
                      'export RUCIO_ACCOUNT=\"pilot\"',
                      'export CORAL_DBLOOKUP_PATH=/ccs/proj/csc108/AtlasReleases/21.0.15/nfs_db_files',
                      'export CORAL_AUTH_PATH=$SW_INSTALL_AREA/DBRelease/current/XMLConfig',
                      'export DATAPATH=$SW_INSTALL_AREA/DBRelease/current:$DATAPATH',
                      ' ']

    return setup_commands


def timestamp():
    """ return ISO-8601 compliant date/time format"""
    tmptz = time.timezone
    sign_str = '+'
    if tmptz > 0:
        sign_str = '-'
    tmptz_hours = int(tmptz/3600)

    return str("%s%s%02d:%02d" % (time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()), sign_str, abs(tmptz_hours), int(tmptz/60-tmptz_hours*60)))


def dump_worker_attributes(job, workerAttributesFile):

    logger.info('Dump worker attributes')
    # Harvester only expects the attributes files for certain states.
    if job.state in ['finished', 'failed', 'running']:
        with open(workerAttributesFile, 'w') as outputfile:
            workAttributes = {'jobStatus': job.state}
            workAttributes['workdir'] = os.getcwd()
            workAttributes['messageLevel'] = logging.getLevelName(logger.getEffectiveLevel())
            workAttributes['timestamp'] = timestamp()
            workAttributes['cpuConversionFactor'] = 1.0

            coreCount = None
            nEvents = None
            dbTime = None
            dbData = None

            if 'ATHENA_PROC_NUMBER' in os.environ:
                workAttributes['coreCount'] = os.environ['ATHENA_PROC_NUMBER']
                coreCount = os.environ['ATHENA_PROC_NUMBER']

            # check if job report json file exists
            jobReport = None
            readJsonPath = os.path.join(os.getcwd(), "jobReport.json")
            logger.debug('parsing %s' % readJsonPath)
            if os.path.exists(readJsonPath):
                # load json
                with open(readJsonPath) as jsonFile:
                    jobReport = json.load(jsonFile)
            if jobReport is not None:
                if 'resource' in jobReport:
                    if 'transform' in jobReport['resource']:
                        if 'processedEvents' in jobReport['resource']['transform']:
                            workAttributes['nEvents'] = jobReport['resource']['transform']['processedEvents']
                            nEvents = jobReport['resource']['transform']['processedEvents']
                        if 'cpuTimeTotal' in jobReport['resource']['transform']:
                            workAttributes['cpuConsumptionTime'] = jobReport['resource']['transform'][
                                'cpuTimeTotal']

                    if 'machine' in jobReport['resource']:
                        if 'node' in jobReport['resource']['machine']:
                            workAttributes['node'] = jobReport['resource']['machine']['node']
                        if 'model_name' in jobReport['resource']['machine']:
                            workAttributes['cpuConsumptionUnit'] = jobReport['resource']['machine']['model_name']

                    if 'dbTimeTotal' in jobReport['resource']:
                        dbTime = jobReport['resource']['dbTimeTotal']
                    if 'dbDataTotal' in jobReport['resource']:
                        dbData = jobReport['resource']['dbDataTotal']

                    if 'executor' in jobReport['resource']:
                        if 'memory' in jobReport['resource']['executor']:
                            for transform_name, attributes in jobReport['resource']['executor'].iteritems():
                                if 'Avg' in attributes['memory']:
                                    for name, value in attributes['memory']['Avg'].iteritems():
                                        try:
                                            workAttributes[name] += value
                                        except:
                                            workAttributes[name] = value
                                if 'Max' in attributes['memory']:
                                    for name, value in attributes['memory']['Max'].iteritems():
                                        try:
                                            workAttributes[name] += value
                                        except:
                                            workAttributes[name] = value

                if 'exitCode' in jobReport:
                    workAttributes['transExitCode'] = jobReport['exitCode']
                    workAttributes['exeErrorCode'] = jobReport['exitCode']
                if 'exitMsg' in jobReport:
                    workAttributes['exeErrorDiag'] = jobReport['exitMsg']
                if 'files' in jobReport:
                    if 'input' in jobReport['files']:
                        if 'subfiles' in jobReport['files']['input']:
                            workAttributes['nInputFiles'] = len(jobReport['files']['input']['subfiles'])

                if coreCount and nEvents and dbTime and dbData:
                    res = check_output(['du', '-s'])
                    workAttributes['jobMetrics'] = 'coreCount=%s nEvents=%s dbTime=%s dbData=%s workDirSize=%s' % (
                        coreCount, nEvents, dbTime, dbData, res.split()[0])

            else:
                logger.debug('no jobReport object')
            logger.info('output worker attributes for Harvester: %s' % workAttributes)
            json.dump(workAttributes, outputfile)
    else:
        logger.debug(' %s is not a good state' % job.state)
    logger.debug('exit dump worker attributes')


def main_exit(exit_code, message = "", jobdesc = None):
    sys.exit(exit_code)


def main():

    workerAttributesFile = "worker_attributes.json"
    StageOutnFile = "event_status.dump.json"

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
        main_exit(errno, strerror)

    logger.debug("Collected list of jobs {0}".format(panda_ids))
    # PandaID of the job for the command
    try:
        job_id = panda_ids[rank]
    except ValueError:
        logger.critical("Pilot have no job for rank {0}".format(rank))
        logger.critical("Exit pilot")
        main_exit(1)
    logger.debug("Job [{0}] will be processed".format(job_id))
    os.chdir(str(job_id))

    jobs_dict = get_json_dictionary("HPCJobs.json")
    job_dict = jobs_dict[str(job_id)]

    job = JobDescription()
    job.load(job_dict)

    setup_str = "; ".join(get_setup(job))
    my_command = " ".join([job.script,job.script_parameters])
    my_command = titan_command_fix(my_command)
    #my_command = my_command.strip()
    my_command = setup_str + my_command
    logger.debug("Going to launch: {0}".format(my_command))
    wd_path = os.getcwd()
    logger.debug("Current work directory: {0}".format(wd_path))
    payloadstdout = open("job_stdout.txt", "w")
    payloadstderr = open("job_stderr.txt", "w")
    titan_prepare_wd()
    job.state = 'running'
    start_time = time.asctime(time.localtime(time.time()))
    t0 = os.times()
    exit_code = call(my_command, stdout=payloadstdout, stderr=payloadstderr, shell=True)
    t1 = os.times()
    end_time = time.asctime(time.localtime(time.time()))
    t = map(lambda x, y: x - y, t1, t0)
    t_tot = reduce(lambda x, y: x + y, t[2:3])
    job.state = 'finished'
    dump_worker_attributes(job, workerAttributesFile)
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
    logger.info("Verify output")
    out_file_report = {}
    out_file_report[job.job_id] = []
    outfiles = job.output_files.keys()

    if job.log_file in outfiles:
        outfiles.remove(job.log_file)
    else:
        logger.info("Log files was not declared")
    for outfile in outfiles:
        if os.path.exists(outfile):
            file_desc = {}
            file_desc['type'] = 'output'
            file_desc['path'] = outfile
            file_desc['fsize'] = os.path.getsize(outfile)
            if 'guid' in job.output_files[outfile].keys():
                file_desc['guid'] = job.output_files[outfile]['guid']
            out_file_report[job.job_id].append(file_desc)
        else:
            logger.info("Expected output file {0} missed. Job {1} will be failed".format(outfile, job.job_id))
            job.state = 'failed'

    if out_file_report[job.job_id]:
        with open(StageOutnFile, 'w') as stageoutfile:
            json.dump(out_file_report, stageoutfile)
        logger.debug('Stagout declared in: {0}'.format(StageOutnFile))

    logger.info("All done")
    main_exit(0)


def titan_command_fix(command):

    subs_a = command.split()
    for i in range(len(subs_a)):
        if i > 0:
            if '(' in subs_a[i] and not subs_a[i][0] == '"':
                subs_a[i] = '"'+subs_a[i]+'"'

    command = ' '.join(subs_a)
    command = command.strip()
    command = command.replace('--DBRelease="all:current"', '') # avoid Frontier reading

    return command


def titan_prepare_wd():

    #---------
    # Copy Poolcond files to local working directory

    scratch_path = '/tmp/scratch/'
    dst_db_path = 'sqlite200/'
    dst_db_filename = 'ALLP200.db'
    dst_db_path_2 = 'geomDB/'
    dst_db_filename_2 = 'geomDB_sqlite'
    tmp_path = 'tmp/'
    src_file   = '/ccs/proj/csc108/AtlasReleases/21.0.15/DBRelease/current/sqlite200/ALLP200.db'
    src_file_2 = '/ccs/proj/csc108/AtlasReleases/21.0.15/DBRelease/current/geomDB/geomDB_sqlite'
    copy_start = time.time()
    if os.path.exists(scratch_path):
        try:
            if not os.path.exists(scratch_path + tmp_path):
                os.makedirs(scratch_path + tmp_path)
            if not os.path.exists(scratch_path + dst_db_path):
                os.makedirs(scratch_path + dst_db_path)
            shutil.copyfile(src_file, scratch_path + dst_db_path + dst_db_filename)
            if not os.path.exists(scratch_path + dst_db_path_2):
                os.makedirs(scratch_path + dst_db_path_2)
            shutil.copyfile(src_file_2, scratch_path + dst_db_path_2 + dst_db_filename_2)
        except:
            logger.error("Copy to scratch failed, execution terminated':  \n %s " % (sys.exc_info()[1]))
            main_exit(1, "Copy to scratch failed, execution terminated")
    else:
        logger.error('Scratch directory (%s) dose not exist, execution terminated' % scratch_path)
        return False
    logger.debug("Current directory: {0}".format(os.getcwd()))

    true_dir = '/ccs/proj/csc108/AtlasReleases/21.0.15/nfs_db_files'
    pseudo_dir = "./poolcond"
    os.symlink(true_dir, pseudo_dir)
    copy_time = time.time() - copy_start
    logger.info('Special Titan setup took: {0}'.format(copy_time))

    return True

if __name__ == "__main__":
    main()
