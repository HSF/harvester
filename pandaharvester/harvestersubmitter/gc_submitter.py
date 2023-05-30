import ast
import argparse
import json
import shlex
import os
import tarfile
import datetime
from math import ceil
import urllib.request as urllib
import traceback

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestersubmitter import submitter_common

from pandaharvester.harvestercore.plugin_factory import PluginFactory

from globus_compute_sdk import Client

# logger
baseLogger = core_utils.setup_logger('gc_submitter')


def run_pilot_wrapper_with_args(run_pilotwrapper_cmd, args):
    import subprocess                    # noqa F811

    run_pilotwrapper_cmd = run_pilotwrapper_cmd.format_map(args)
    p = subprocess.Popen(run_pilotwrapper_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return p.returncode, stdout, stderr


def run_wrapper(base_path, data_path, func_str):
    import traceback
    try:
        import json
        import os
        import socket
        import sys

        current_dir = os.getcwd()
        os.chdir(base_path)

        os.environ['HARVESTER_WORKER_BASE_PATH'] = base_path
        os.environ['HARVESTER_DATA_PATH'] = data_path
        os.environ['PYTHONPATH'] = base_path + ":" + os.environ.get("PYTHONPATH", "")
        print("hostname: %s" % socket.gethostname())
        print("current directory: %s" % os.getcwd())
        print("PYTHONPATH: %s" % os.environ['PYTHONPATH'])
        print("execute programe: %s" % str(func_str))

        func_json = json.loads(func_str)
        func_name = func_json["func_name"]
        kwargs = func_json.get("kwargs", {})
        pre_script = func_json.get("pre_script", None)
        sys.path.append(base_path)

        if pre_script:
            exec(pre_script)

        f = locals()[func_name]
        print("function %s" % f)
        ret_value = f(**kwargs)
        print("return value: %s" % ret_value)

        os.chdir(current_dir)
        return ret_value
    except Exception as ex:
        print("Exception")
        print(ex)
        print(traceback.format_exc())
        raise Exception(traceback.format_exc())
    except:
        print("traceback")
        print(traceback.format_exc())
        raise Exception(traceback.format_exc())

    return None


# submitter for SLURM batch system
class GlobusComputeSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.uploadLog = False
        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)

        self.gc_client = None
        self.submit_func_id = None
        self.parser = None

    def get_messenger(self, workSpec):
        queueconfigmapper = QueueConfigMapper()
        queueConfig = queueconfigmapper.get_queue(workSpec.computingSite)
        pluginFactory = PluginFactory()
        messenger = pluginFactory.get_plugin(queueConfig.messenger)
        return messenger

    def get_job_data(self, workSpec, logger):
        # job_data = None
        # baseDir = workSpec.get_access_point()
        jobSpecs = workSpec.get_jobspec_list()
        func_args = {}
        for jobSpec in jobSpecs:
            # logger.info(jobSpec)
            logger.debug(" ".join([jobSpec.jobParams['transformation'], jobSpec.jobParams['jobPars']]))
            panda_id = jobSpec.PandaID
            func_arg = self.get_job_funcx_args(workSpec, jobSpec, logger)
            func_args[panda_id] = func_arg
        return func_args

    def get_panda_argparser(self):
        if self.parser is None:
            parser = argparse.ArgumentParser(description='PanDA argparser')
            parser.add_argument('-j', type=str, required=False, default='', help='j')
            parser.add_argument('--sourceURL', type=str, required=False, default='', help='source url')
            parser.add_argument('-r', type=str, required=False, default='', help='directory')
            parser.add_argument('-l', '--lib', required=False, action='store_true', default=False, help='library')
            parser.add_argument('-i', '--input', type=str, required=False, default='', help='input')
            parser.add_argument('-o', '--output', type=str, required=False, default='', help='output')
            parser.add_argument('-p', '--program', type=str, required=False, default='', help='program')
            parser.add_argument('-a', '--archive', type=str, required=False, default='', help='source archive file')
            self.parser = parser
        return self.parser

    def get_job_funcx_args(self, workSpec, jobSpec, logger):
        job_pars = jobSpec.jobParams['jobPars']
        job_arguments = shlex.split(job_pars)
        parser = self.get_panda_argparser()
        job_args, _ = parser.parse_known_args(job_arguments)

        job_script = job_args.program
        job_script = urllib.unquote(job_script)
        logger.debug("job_script: %s" % job_script)
        input_files = job_args.input
        if input_files:
            input_files = ast.literal_eval(input_files)
        logger.debug("job_input: %s" % str(input_files))
        input_files = ",".join(input_files)
        logger.debug("job_input: %s" % str(input_files))

        job_script = job_script.replace("%IN", input_files)
        logger.debug("job_script: %s" % job_script)

        messenger = self.get_messenger(workSpec)
        base_path = messenger.get_access_point(workSpec, jobSpec.PandaID)
        # base_path = workSpec.get_access_point()
        data_path = self.dataPath
        # logger.info(data_path)
        source_url = job_args.sourceURL
        # source_url = self.pandaURL
        self.download_source_codes(base_path, source_url, job_args.archive, logger)
        func_args = base_path, data_path, job_script
        return func_args

    def download_source_codes(self, base_dir, source_url, source_file, logger):
        archive_basename = os.path.basename(source_file)
        if not os.path.exists(base_dir):
            os.makedirs(base_dir, exist_ok=True)
        full_output_filename = os.path.join(base_dir, archive_basename)
        if os.path.exists(full_output_filename):
            logger.info("source codes already exist: %s" % full_output_filename)
        else:
            os.environ["PANDACACHE_URL"] = source_url
            logger.info("PANDACACHE_URL: %s" % (os.environ["PANDACACHE_URL"]))
            from pandaclient import Client
            Client.baseURLCSRVSSL = source_url
            status, output = Client.getFile(archive_basename, output_path=full_output_filename)
            logger.info("Download archive file from pandacache status: %s, output: %s" % (status, output))
            if status != 0:
                raise RuntimeError("Failed to download archive file from pandacache")
            with tarfile.open(full_output_filename, 'r:gz') as f:
                f.extractall(base_dir)
            logger.info("Extract %s to %s" % (full_output_filename, base_dir))

    # submit workers
    def submit_workers(self, workspec_list):
        retList = []

        try:
            if self.gc_client is None or self.submit_func_id is None:
                self.gc_client = Client()
                self.submit_func_id = self.gc_client.register_function(run_wrapper)
        except Exception as ex:
            tmpLog = self.make_logger(baseLogger, "init_gc_client",
                                      method_name='submit_workers')
            tmpLog.error("Failed to init gc client: %s" % str(ex))
            tmpLog.error(traceback.format_exc())

        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, 'workerID={0}'.format(workSpec.workerID),
                                      method_name='submit_workers')
            try:
                if self.gc_client is None or self.submit_func_id is None:
                    errStr = "Globus Compute client is not initialized"
                    tmpLog.error(errStr)
                    tmpRetVal = (False, errStr)
                else:
                    func_args = self.get_job_data(workSpec, tmpLog)

                    batch = self.gc_client.create_batch()
                    for panda_id in func_args:
                        func_arg = func_args[panda_id]
                        batch.add(args=func_arg, endpoint_id=self.funcxEndpointId, function_id=self.submit_func_id)
                    batch_res = self.gc_client.batch_run(batch)

                    results = self.gc_client.get_batch_result(batch_res)
                    batch_ids = []
                    for batch_id in results:
                        batch_ids.append(batch_id)

                    workSpec.batchID = json.dumps(batch_ids)
                    tmpLog.debug('PanDAID={0}'.format([panda_id for panda_id in func_args]))
                    tmpLog.debug('batchID={0}'.format(workSpec.batchID))
                    # batch_id = self.gc_client.run(base_path, data_path, job_script, endpoint_id=self.funcxEndpointId, function_id=self.submit_func_id)
                    # workSpec.batchID = batch_id
                    # tmpLog.debug('batchID={0}'.format(workSpec.batchID))

                    # set log files
                    if self.uploadLog:
                        if self.logBaseURL is None:
                            baseDir = workSpec.get_access_point()
                        else:
                            baseDir = self.logBaseURL
                        stdOut, stdErr = self.get_log_file_names(workSpec.batchID)
                        if stdOut is not None:
                            workSpec.set_log_file('stdout', '{0}/{1}'.format(baseDir, stdOut))
                        if stdErr is not None:
                            workSpec.set_log_file('stderr', '{0}/{1}'.format(baseDir, stdErr))
                    tmpRetVal = (True, '')
            except Exception as ex:
                # failed
                errStr = str(ex)
                tmpLog.error(errStr)
                tmpRetVal = (False, errStr)
                tmpLog.error(traceback.format_exc())
            retList.append(tmpRetVal)
        return retList

    def make_submit_args(self, workspec, logger):
        timeNow = datetime.datetime.utcnow()

        panda_queue_name = self.queueName
        this_panda_queue_dict = dict()

        # get default information from queue info
        n_core_per_node_from_queue = this_panda_queue_dict.get('corecount', 1) if this_panda_queue_dict.get('corecount', 1) else 1

        # get override requirements from queue configured
        try:
            n_core_per_node = self.nCorePerNode if self.nCorePerNode else n_core_per_node_from_queue
        except AttributeError:
            n_core_per_node = n_core_per_node_from_queue

        n_core_total = workspec.nCore if workspec.nCore else n_core_per_node
        request_ram = max(workspec.minRamCount, 1 * n_core_total) if workspec.minRamCount else 1 * n_core_total
        request_disk = workspec.maxDiskCount * 1024 if workspec.maxDiskCount else 1
        request_walltime = workspec.maxWalltime if workspec.maxWalltime else 0

        n_node = ceil(n_core_total / n_core_per_node)
        request_ram_bytes = request_ram * 2 ** 20
        request_ram_per_core = ceil(request_ram * n_node / n_core_total)
        request_ram_bytes_per_core = ceil(request_ram_bytes * n_node / n_core_total)
        request_cputime = request_walltime * n_core_total
        request_walltime_minute = ceil(request_walltime / 60)
        request_cputime_minute = ceil(request_cputime / 60)

        job_data = None
        if workspec.mapType in [WorkSpec.MT_OneToOne, WorkSpec.MT_MultiJobs, WorkSpec.MT_MultiWorkers]:
            base_dir = workspec.get_access_point()
            jobSpecFileName = harvester_config.payload_interaction.jobSpecFile
            jobSpecFileName = os.path.join(base_dir, jobSpecFileName)
            logger.debug("jobSpecFileName: %s" % jobSpecFileName)
            if os.path.exists(jobSpecFileName):
                with open(jobSpecFileName, 'r') as jobSpecFile:
                    job_data = jobSpecFile.read()

        _queueConfigMapper = QueueConfigMapper()
        harvester_queue_config = _queueConfigMapper.get_queue(self.queueName)

        pilot_version = 'unknown'
        pilot_opt_dict = submitter_common.get_complicated_pilot_options(pilot_type=workspec.pilotType,
                                                                        pilot_url=None,
                                                                        pilot_version=pilot_version,
                                                                        prod_source_label=harvester_queue_config.get_source_label(workspec.jobType),
                                                                        prod_rc_permille=0)
        prod_source_label = pilot_opt_dict['prod_source_label']
        pilot_type_opt = pilot_opt_dict['pilot_type_opt']
        # pilot_url_str = pilot_opt_dict['pilot_url_str']

        placeholder_map = {
            'nCorePerNode': n_core_per_node,
            'nCoreTotal': n_core_total,
            'nNode': n_node,
            'requestRam': request_ram,
            'requestRamBytes': request_ram_bytes,
            'requestRamPerCore': request_ram_per_core,
            'requestRamBytesPerCore': request_ram_bytes_per_core,
            'requestDisk': request_disk,
            'requestWalltime': request_walltime,
            'requestWalltimeMinute': request_walltime_minute,
            'requestCputime': request_cputime,
            'requestCputimeMinute': request_cputime_minute,
            'accessPoint': workspec.accessPoint,
            'harvesterID': harvester_config.master.harvester_id,
            'workerID': workspec.workerID,
            'computingSite': workspec.computingSite,
            'pandaQueueName': panda_queue_name,
            # 'x509UserProxy': x509_user_proxy,
            'logDir': self.logDir,
            'logSubDir': os.path.join(self.logDir, timeNow.strftime('%y-%m-%d_%H')),
            'jobType': workspec.jobType,
            'prodSourceLabel': prod_source_label,
            'pilotType': pilot_type_opt,
            'pilotUrlOption': None,
            'pilotVersion': 3
        }
        # for k in ['tokenDir', 'tokenName', 'tokenOrigin', 'submitMode', 'tokenFile']:
        for k in ['tokenOrigin', 'tokenFile']:
            try:
                placeholder_map[k] = getattr(self, k)
            except Exception:
                pass
        # read token
        token = None
        try:
            with open(self.tokenFile) as tokenFile:
                token = tokenFile.read()
        except Exception as ex:
            logger.error("Failed to read the token: %s" % ex)
        placeholder_map['token'] = token

        if job_data:
            placeholder_map['jobData'] = job_data

        return placeholder_map

    # get log file names
    def get_log_file_names(self, batch_id):
        stdOut = "stdout.txt"
        stdErr = "tderr.txt"
        return stdOut, stdErr
