import ast
import argparse
import json
import shlex
import os
import tarfile
import urllib.request as urllib
import traceback

from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

from pandaharvester.harvestercore.plugin_factory import PluginFactory

from globus_compute_sdk import Client

# logger
baseLogger = core_utils.setup_logger("globus_compute_submitter")


def run_wrapper(base_path, data_path, func_str):
    import traceback

    try:
        import json
        import os
        import socket
        import sys

        current_dir = os.getcwd()
        os.chdir(base_path)

        os.environ["HARVESTER_WORKER_BASE_PATH"] = base_path
        os.environ["HARVESTER_DATA_PATH"] = data_path
        os.environ["PYTHONPATH"] = base_path + ":" + os.environ.get("PYTHONPATH", "")
        print("hostname: %s" % socket.gethostname())
        print("current directory: %s" % os.getcwd())
        print("PYTHONPATH: %s" % os.environ["PYTHONPATH"])
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
    except BaseException:
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
            logger.debug(" ".join([jobSpec.jobParams["transformation"], jobSpec.jobParams["jobPars"]]))
            panda_id = jobSpec.PandaID
            func_arg = self.get_job_funcx_args(workSpec, jobSpec, logger)
            func_args[panda_id] = func_arg
        return func_args

    def get_panda_argparser(self):
        if self.parser is None:
            parser = argparse.ArgumentParser(description="PanDA argparser")
            parser.add_argument("-j", type=str, required=False, default="", help="j")
            parser.add_argument("--sourceURL", type=str, required=False, default="", help="source url")
            parser.add_argument("-r", type=str, required=False, default="", help="directory")
            parser.add_argument("-l", "--lib", required=False, action="store_true", default=False, help="library")
            parser.add_argument("-i", "--input", type=str, required=False, default="", help="input")
            parser.add_argument("-o", "--output", type=str, required=False, default="", help="output")
            parser.add_argument("-p", "--program", type=str, required=False, default="", help="program")
            parser.add_argument("-a", "--archive", type=str, required=False, default="", help="source archive file")
            self.parser = parser
        return self.parser

    def get_job_funcx_args(self, workSpec, jobSpec, logger):
        job_pars = jobSpec.jobParams["jobPars"]
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
            with tarfile.open(full_output_filename, "r:gz") as f:
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
            tmpLog = self.make_logger(baseLogger, "init_gc_client", method_name="submit_workers")
            tmpLog.error("Failed to init gc client: %s" % str(ex))
            tmpLog.error(traceback.format_exc())

        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workSpec.workerID), method_name="submit_workers")
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
                    tmpLog.debug("PanDAID={0}".format([panda_id for panda_id in func_args]))
                    tmpLog.debug("batchID={0}".format(workSpec.batchID))
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
                            workSpec.set_log_file("stdout", "{0}/{1}".format(baseDir, stdOut))
                        if stdErr is not None:
                            workSpec.set_log_file("stderr", "{0}/{1}".format(baseDir, stdErr))
                    tmpRetVal = (True, "")
            except Exception as ex:
                # failed
                errStr = str(ex)
                tmpLog.error(errStr)
                tmpRetVal = (False, errStr)
                tmpLog.error(traceback.format_exc())
            retList.append(tmpRetVal)
        return retList

    # get log file names
    def get_log_file_names(self, batch_id):
        stdOut = "stdout.txt"
        stdErr = "stderr.txt"
        return stdOut, stdErr
