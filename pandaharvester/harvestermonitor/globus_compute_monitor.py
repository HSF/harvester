import argparse
import ast
import json
import os
import shlex
import traceback

from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.plugin_factory import PluginFactory

from globus_compute_sdk import Client
from globus_compute_sdk import errors as gc_errors


# logger
baseLogger = core_utils.setup_logger("globus_compute_monitor")


# monitor for globus compute batch system
class GlobusComputeMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        self.gc_client = None

        self.parser = None
        self.dbProxy = DBProxy()

    def get_messenger(self, workSpec):
        queueconfigmapper = QueueConfigMapper()
        queueConfig = queueconfigmapper.get_queue(workSpec.computingSite)
        pluginFactory = PluginFactory()
        messenger = pluginFactory.get_plugin(queueConfig.messenger)
        return messenger

    def get_panda_argparser(self):
        if self.parser is None:
            parser = argparse.ArgumentParser(description="PanDA argparser")
            parser.add_argument("-j", type=str, required=False, default="", help="j")
            parser.add_argument("--sourceURL", type=str, required=False, default="", help="source url")
            parser.add_argument("-r", type=str, required=False, default="", help="directory")
            parser.add_argument("-l", "--lib", required=False, action="store_true", default=False, help="library")
            parser.add_argument("-o", "--output", type=str, required=False, default="", help="output")
            parser.add_argument("-p", "--program", type=str, required=False, default="", help="program")
            parser.add_argument("-a", "--archive", type=str, required=False, default="", help="source archive file")
            self.parser = parser
        return self.parser

    def get_out_file_infos(self, workSpec, jobSpec, logFile, ret, logger):
        base_dir = os.path.dirname(logFile)

        job_pars = jobSpec.jobParams["jobPars"]
        job_arguments = shlex.split(job_pars)
        parser = self.get_panda_argparser()
        job_args, _ = parser.parse_known_args(job_arguments)
        output = job_args.output
        logger.debug("output: %s" % output)

        outFileInfos = []
        if output:
            scopes = jobSpec.jobParams["scopeOut"].split(",")
            output = ast.literal_eval(output)

            keys = list(output.keys())
            # the first file is for function output
            lfn = output[keys[0]]
            scope = scopes[0]

            pfn = os.path.join(base_dir, lfn)
            with open(pfn, "w") as fp:
                result = None
                if ret:
                    result = ret.get("result", None)
                fp.write(str(result))

            outFileInfo = {"lfn": lfn, "path": pfn}
            outFileInfos.append(outFileInfo)

            for key, scope in zip(keys[1:], scopes[1:]):
                lfn = output[key]
                src = os.path.join(base_dir, key)
                dest = os.path.join(base_dir, lfn)
                if os.path.exists(src):
                    os.rename(src, dest)
                    outFileInfo = {"lfn": lfn, "path": dest}
                    outFileInfos.append(outFileInfo)
        return outFileInfos

    def get_state_data_structure(self, workSpec, jobSpec, ret, error):
        if ret:
            status = ret.get("status", None)
        else:
            status = None
        state = "failed"
        if status:
            if status in ["success"]:
                state = "finished"
        data = {
            "jobId": jobSpec.PandaID,
            "state": state,
            # 'timestamp': time_stamp(),
            "siteName": workSpec.computingSite,  # args.site,
            "node": None,
            # 'attemptNr': None,
            "startTime": None,
            "jobMetrics": None,
            "metaData": None,
            "xml": None,
            "coreCount": 1,
            "cpuConsumptionTime": None,
            "cpuConversionFactor": None,
            "cpuConsumptionUnit": None,
            "cpu_architecture_level": None,
            # 'maxRSS', 'maxVMEM', 'maxSWAP', 'maxPSS', 'avgRSS', 'avgVMEM', 'avgSWAP', 'avgPSS'
        }
        return data

    def set_work_attributes(self, workSpec, logFile, work_rets, logger):
        rets = work_rets.get("ret", {})
        error = work_rets.get("err", None)

        messenger = self.get_messenger(workSpec)
        jsonAttrsFileName = harvester_config.payload_interaction.workerAttributesFile
        # postProcessAttrs = 'post_process_job_attrs.json'
        jsonJobReport = harvester_config.payload_interaction.jobReportFile
        jsonOutputsFileName = harvester_config.payload_interaction.eventStatusDumpJsonFile

        jobSpecs = self.dbProxy.get_jobs_with_worker_id(workSpec.workerID, None, with_file=True, only_running=False, slim=False)
        jobSpec_map = {}
        for jobSpec in jobSpecs:
            jobSpec_map[jobSpec.PandaID] = jobSpec

        for pandaID in workSpec.pandaid_list:
            jobSpec = jobSpec_map[pandaID]
            ret = rets.get(pandaID, None)
            logger.debug("pandaID %s ret: %s" % (pandaID, str(ret)))
            if ret:
                ret = ret.get("ret", {})
            attrs = self.get_state_data_structure(workSpec, jobSpec, ret, error)

            accessPoint = messenger.get_access_point(workSpec, pandaID)
            if not os.path.exists(accessPoint):
                os.makedirs(accessPoint, exist_ok=True)

            # outputs
            jsonFilePath = os.path.join(accessPoint, jsonOutputsFileName)
            logger.debug("set attributes file {0}".format(jsonFilePath))
            logger.debug("jobSpec: %s" % str(jobSpec))
            # logger.debug('jobSpec jobParams: %s' % str(jobSpec.jobParams))
            outFile_infos = self.get_out_file_infos(workSpec, jobSpec, logFile, ret, logger)
            logger.debug("outFile_infos: %s" % str(outFile_infos))

            out_files = {str(pandaID): []}
            for outFile_info in outFile_infos:
                out_files[str(pandaID)].append({"path": outFile_info["path"], "type": "output"})
            with open(jsonFilePath, "w") as jsonFile:
                json.dump(out_files, jsonFile)

            # work attr
            jsonFilePath = os.path.join(accessPoint, jsonAttrsFileName)
            logger.debug("set attributes file {0}".format(jsonFilePath))
            with open(jsonFilePath, "w") as jsonFile:
                json.dump(attrs, jsonFile)

            # job report
            jsonFilePath = os.path.join(accessPoint, jsonJobReport)
            logger.debug("set attributes file {0}".format(jsonFilePath))
            with open(jsonFilePath, "w") as jsonFile:
                json.dump(attrs, jsonFile)

            # post process
            # jsonFilePath = os.path.join(accessPoint, postProcessAttrs)
            # logger.debug('set attributes file {0}'.format(jsonFilePath))
            # with open(jsonFilePath, 'w') as jsonFile:
            #     json.dump(attrs, jsonFile)

    # check workers
    def check_workers(self, workspec_list):
        retList = []

        try:
            if self.gc_client is None:
                self.gc_client = Client()
        except Exception as ex:
            tmpLog = self.make_logger(baseLogger, "init_gc_client", method_name="check_workers")
            tmpLog.error("Failed to init gc client: %s" % str(ex))

        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workSpec.workerID), method_name="check_workers")

            errStr, errLogStr, outLogStr = None, None, None
            work_rets = {}
            try:
                if self.gc_client is None:
                    errStr = "Funcx client is not initialized"
                    tmpLog.error(errStr)
                    errLogStr = errStr
                    newStatus = WorkSpec.ST_failed
                    tmpRetVal = (newStatus, errStr)
                    work_rets["err"] = errStr
                else:
                    try:
                        # jobSpecs = workSpec.get_jobspec_list()
                        # tmpLog.debug(jobSpecs)
                        # tmpLog.debug(workSpec.get_jobspec_list())
                        # tmpLog.debug(workSpec.pandaid_list)

                        # panda_ids = [jobSpec.PandaID for jobSpec in jobSpecs]
                        tmpLog.debug("batchID: %s" % workSpec.batchID)
                        panda_ids = workSpec.pandaid_list
                        batch_ids = json.loads(workSpec.batchID)
                        tmpLog.debug("batch_ids: %s" % str(batch_ids))
                        if not batch_ids:
                            raise Exception("batchID is empty")
                        rets = self.gc_client.get_batch_result(batch_ids)
                        tmpLog.debug("get_batch_result rets: %s" % rets)
                        if not rets:
                            # rets can be empty sometimes
                            for batch_id in batch_ids:
                                rets[batch_id] = self.gc_client.get_task(batch_id)
                    except gc_errors.error_types.TaskExecutionFailed as ex:
                        newStatus = WorkSpec.ST_failed
                        errStr = str(ex)
                        tmpRetVal = (newStatus, errStr)
                        tmpLog.info("worker terminated: %s" % ex)
                        tmpLog.debug(traceback.format_exc())
                        errLogStr = errStr + "\n" + str(traceback.format_exc())
                        work_rets["err"] = errStr
                    else:
                        newStatus = None
                        all_finished = True
                        # status: received, waiting-for-launch, running, success
                        for batch_id in batch_ids:
                            if batch_id not in rets:
                                all_finished = False
                                newStatus = WorkSpec.ST_running
                                break
                            if rets[batch_id].get("pending", True) or rets[batch_id].get("status", None) in ["waiting-for-launch", "running"]:
                                newStatus = WorkSpec.ST_running
                                all_finished = False
                                break
                            else:
                                batch_status = rets[batch_id].get("status", None)
                                if batch_status and batch_status != "success":
                                    all_finished = False

                        if newStatus is None:
                            if all_finished:
                                newStatus = WorkSpec.ST_finished
                            else:
                                newStatus = WorkSpec.ST_failed
                        tmpLog.info("worker status: %s" % newStatus)

                        try:
                            if newStatus in [WorkSpec.ST_finished, WorkSpec.ST_failed]:
                                new_rets = {}
                                for panda_id, batch_id in zip(panda_ids, list(rets.keys())):
                                    new_rets[panda_id] = {"funcx_id": batch_id, "ret": rets[batch_id]}

                                outLogStr = str(new_rets)
                                work_rets["ret"] = new_rets
                        except Exception as ex:
                            newStatus = WorkSpec.ST_failed
                            errStr = "Failed to parse worker result: %s" % ex
                            tmpLog.error(errStr)
                            tmpLog.debug(traceback.format_exc())
                            errLogStr = errStr + "\n" + str(traceback.format_exc())
                            work_rets["err"] = errStr

                        tmpRetVal = (newStatus, errStr)
            except Exception as ex:
                # failed
                errStr = str(ex)
                tmpLog.error(errStr)
                tmpLog.debug(traceback.format_exc())
                work_rets["err"] = errStr

                newStatus = WorkSpec.ST_failed
                tmpRetVal = (newStatus, errStr)

            if newStatus in [WorkSpec.ST_finished, WorkSpec.ST_failed]:
                baseDir = workSpec.get_access_point()

                stdOut, stdErr = self.get_log_file_names(workSpec.batchID)
                stdOut = os.path.join(baseDir, stdOut)
                stdErr = os.path.join(baseDir, stdErr)
                tmpLog.info("stdout: %s, stderr: %s" % (stdOut, stdErr))
                with open(stdOut, "w") as fp:
                    fp.write(str(outLogStr))
                with open(stdErr, "w") as fp:
                    fp.write(str(errLogStr))

                try:
                    self.set_work_attributes(workSpec, stdOut, work_rets, tmpLog)
                except Exception as ex:
                    tmpLog.error(ex)
                    tmpLog.debug(traceback.format_exc())

            retList.append(tmpRetVal)
        return True, retList

    # get log file names
    def get_log_file_names(self, batch_id):
        stdOut = "stdout.txt"
        stdErr = "stderr.txt"
        return stdOut, stdErr
