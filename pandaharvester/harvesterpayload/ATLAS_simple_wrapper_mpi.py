#!/usr/bin/env python
import sys
import os
import time
import json
import logging
import shutil
import tarfile
from glob import glob
from socket import gethostname
from subprocess import call, check_output
from datetime import datetime
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

logger.info('HPC Pilot ver. 0.001')

def get_setup(job):

    # special setup preparation.

    setup_commands = [ 'source /lustre/atlas/proj-shared/csc108/app_dir/pilot/grid_env/external/setup.sh',
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
    """ return ISO-8601 compliant date/time format. Should be migrated to Pilot 2"""
    tmptz = time.timezone
    sign_str = '+'
    if tmptz > 0:
        sign_str = '-'
    tmptz_hours = int(tmptz/3600)

    return str("%s%s%02d:%02d" % (time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()), sign_str, abs(tmptz_hours), int(tmptz/60-tmptz_hours*60)))


def dump_worker_attributes(job, workerAttributesFile):

    '''
    Should be refactored

    '''
    logger.info('Dump worker attributes')
    # Harvester only expects the attributes files for certain states.
    if job.state in ['finished', 'failed', 'running']:
        with open(workerAttributesFile, 'w') as outputfile:
            workAttributes = {'jobStatus': job.state}
            workAttributes['workdir'] = os.getcwd()
            workAttributes['messageLevel'] = logging.getLevelName(logger.getEffectiveLevel())
            workAttributes['timestamp'] = timestamp()
            workAttributes['cpuConversionFactor'] = 1.0
            if job.startTime:
                workAttributes['startTime'] = job.startTime
            if job.endTime:
                workAttributes['endTime'] = job.endTime
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
                    if workAttributes['transExitCode'] != 0:
                        workAttributes['jobStatus'] = 'failed'
                        job.state = 'failed'
                if 'exitMsg' in jobReport:
                    workAttributes['exeErrorDiag'] = jobReport['exitMsg']
                if 'files' in jobReport:
                    if 'input' in jobReport['files']:
                        if 'subfiles' in jobReport['files']['input']:
                            workAttributes['nInputFiles'] = len(jobReport['files']['input']['subfiles'])

                if coreCount and nEvents and dbTime and dbData:
                    #res = check_output(['du', '-s'])
                    workAttributes['jobMetrics'] = 'coreCount=%s nEvents=%s dbTime=%s dbData=%s ' % (
                        coreCount, nEvents, dbTime, dbData)

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
    logger.info("Pilot statrted at {0} on {1}".format(start_g_str, hostname))
    starting_point = os.getcwd()

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
    worker_communication_point = os.getcwd()
    jobs_dict = get_json_dictionary("HPCJobs.json")
    job_dict = jobs_dict[str(job_id)]

    job = JobDescription()
    job.load(job_dict)
    job.startTime = ""
    job.endTime = ""
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

    job_working_dir = os.getcwd()
    job.state = 'running'
    start_time = time.asctime(time.localtime(time.time()))
    job.startTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    dump_worker_attributes(job, workerAttributesFile)
    t0 = os.times()
    exit_code = call(my_command, stdout=payloadstdout, stderr=payloadstderr, shell=True)
    t1 = os.times()
    end_time = time.asctime(time.localtime(time.time()))
    t = map(lambda x, y: x - y, t1, t0)
    t_tot = reduce(lambda x, y: x + y, t[2:3])
    job.state = 'finished'
    job.endTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    dump_worker_attributes(job, workerAttributesFile)
    payloadstdout.close()
    payloadstderr.close()
    logger.info("Payload exit code: {0}".format(exit_code))
    logger.info("CPU comsumption time: {0}".format(t_tot))
    logger.info("Start time: {0}".format(start_time))
    logger.info("End time: {0}".format(end_time))
    logger.debug("Job report start time: {0}".format(job.startTime))
    logger.debug("Job report end time: {0}".format(job.endTime))
    report = open("rank_report.txt", "w")
    report.write("cpuConsumptionTime: %s\n" % t_tot)
    report.write("exitCode: %s" % exit_code)
    report.close()
    titan_postprocess_wd(job_working_dir)
    protectedfiles = job.output_files.keys()
    if job.log_file in protectedfiles:
        protectedfiles.remove(job.log_file)
    else:
        logger.info("Log files was not declared")

    logger.info("Cleanup of working directory")
    protectedfiles.extend([workerAttributesFile, StageOutnFile])
    removeRedundantFiles(job_working_dir, protectedfiles)

    packlogs(job_working_dir,protectedfiles,job.log_file)
    logger.info("Declare stage-out")
    out_file_report = {}
    out_file_report[job.job_id] = []

    for outfile in job.output_files.keys():
        if os.path.exists(outfile):
            file_desc = {}
            if outfile == job.log_file:
                file_desc['type'] = 'log'
            else:
                file_desc['type'] = 'output'
            file_desc['path'] = os.path.abspath(outfile)
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
    # Copy Poolcond files to scratch (RAMdisk, ssd, etc) to cope high IO

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


def titan_postprocess_wd(jobdir):

    pseudo_dir = "poolcond"
    if os.path.exists(pseudo_dir):
        remove(os.path.join(jobdir, pseudo_dir))
    return 0


def removeRedundantFiles(workdir, outputfiles = []):
    """ Remove redundant files and directories. Should be migrated to Pilot2 """

    logger.info("Removing redundant files prior to log creation")

    workdir = os.path.abspath(workdir)

    dir_list = ["AtlasProduction*",
                "AtlasPoint1",
                "AtlasTier0",
                "buildJob*",
                "CDRelease*",
                "csc*.log",
                "DBRelease*",
                "EvgenJobOptions",
                "external",
                "fort.*",
                "geant4",
                "geomDB",
                "geomDB_sqlite",
                "home",
                "o..pacman..o",
                "pacman-*",
                "python",
                "runAthena*",
                "share",
                "sources.*",
                "sqlite*",
                "sw",
                "tcf_*",
                "triggerDB",
                "trusted.caches",
                "workdir",
                "*.data*",
                "*.events",
                "*.py",
                "*.pyc",
                "*.root*",
                "JEM",
                "tmp*",
                "*.tmp",
                "*.TMP",
                "MC11JobOptions",
                "scratch",
                "jobState-*-test.pickle",
                "*.writing",
                "pwg*",
                "pwhg*",
                "*PROC*",
                "madevent",
                "HPC",
                "objectstore*.json",
                "saga",
                "radical",
                "ckpt*"]

    # remove core and pool.root files from AthenaMP sub directories
    try:
        cleanupAthenaMP(workdir, outputfiles)
    except Exception, e:
        print("Failed to execute cleanupAthenaMP(): %s" % (e))

    # explicitly remove any soft linked archives (.a files) since they will be dereferenced by the tar command (--dereference option)
    matches = []
    import fnmatch
    for root, dirnames, filenames in os.walk(workdir):
        for filename in fnmatch.filter(filenames, '*.a'):
            matches.append(os.path.join(root, filename))
    for root, dirnames, filenames in os.walk(os.path.dirname(workdir)):
        for filename in fnmatch.filter(filenames, 'EventService_premerge_*.tar'):
            matches.append(os.path.join(root, filename))
    if matches != []:
        for f in matches:
            remove(f)
    #else:
    #    print("Found no archive files")

    # note: these should be partitial file/dir names, not containing any wildcards
    exceptions_list = ["runargs", "runwrapper", "jobReport", "log."]

    to_delete = []
    for _dir in dir_list:
        files = glob(os.path.join(workdir, _dir))
        exclude = []

        if files:
            for exc in exceptions_list:
                for f in files:
                    if exc in f:
                        exclude.append(os.path.abspath(f))

            _files = []
            for f in files:
                if not f in exclude:
                    _files.append(os.path.abspath(f))
            to_delete += _files

    exclude_files = []
    for of in outputfiles:
        exclude_files.append(os.path.join(workdir, of))
    for f in to_delete:
        if not f in exclude_files:
            remove(f)

    # run a second pass to clean up any broken links
    broken = []
    for root, dirs, files in os.walk(workdir):
        for filename in files:
            path = os.path.join(root,filename)
            if os.path.islink(path):
                target_path = os.readlink(path)
                # Resolve relative symlinks
                if not os.path.isabs(target_path):
                    target_path = os.path.join(os.path.dirname(path),target_path)
                if not os.path.exists(target_path):
                    broken.append(path)
            else:
                # If it's not a symlink we're not interested.
                continue

    if broken:
        for p in broken:
            remove(p)

    return 0


def cleanupAthenaMP(workdir, outputfiles = []):
    """ Cleanup AthenaMP sud directories prior to log file creation. ATLAS specific """

    for ampdir in glob('%s/athenaMP-workers-*' % (workdir)):
        for (p, d, f) in os.walk(ampdir):
            for filename in f:
                if 'core' in filename or 'tmp.' in filename:
                    path = os.path.join(p, filename)
                    path = os.path.abspath(path)
                    remove(path)
                for outfile in outputfiles:
                    if outfile in filename:
                        path = os.path.join(p, filename)
                        path = os.path.abspath(path)
                        remove(path)

    return 0


def remove(path):
    "Common function for removing of file. Should migrate to Pilo2"
    try:
        os.unlink(path)
    except OSError as e:
        logger.error("Problem with deletion: %s : %s" % (e.errno, e.strerror))
        return -1
    return 0


def packlogs(wkdir, excludedfiles, logfile_name):
    #logfile_size = 0
    to_pack = []
    for path, subdir, files in os.walk(wkdir):
        for file in files:
            if not file in excludedfiles:
                relDir = os.path.relpath(path, wkdir)
                file_rel_path = os.path.join(relDir, file)
                file_path = os.path.join(path, file)
                to_pack.append((file_path, file_rel_path))
    if to_pack:
        logfile_name = os.path.join(wkdir, logfile_name)
        log_pack = tarfile.open(logfile_name, 'w:gz')
        for f in to_pack:
            #print f[0], f[1]
            log_pack.add(f[0],arcname=f[1])
        log_pack.close()
        #logfile_size = os.path.getsize(logfile_name)

    for f in to_pack:
        remove(f[0])

    del_empty_dirs(wkdir)

    return 0


def del_empty_dirs(src_dir):

    "Common function for removing of empty directories. Should migrate to Pilo2"

    for dirpath, subdirs, files in os.walk(src_dir, topdown=False):
        if dirpath == src_dir:
            break
        try:
            os.rmdir(dirpath)
        except OSError as ex:
            pass
    return 0

if __name__ == "__main__":
    main()
