from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec as ws

try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

import datetime

# logger
baseLogger = core_utils.setup_logger("titan_utils")


class TitanUtils(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        tmpLog = self.make_logger(baseLogger, method_name="__init__")
        tmpLog.info("Titan utils initiated")

    def get_batchjob_info(self, batchid):
        """
        Collect job info from scheduler
        :param batchid:
        :return res - dictonary with job state and some timing:
        """
        """
        :param batchid:
        :return:
        """
        tmpLog = self.make_logger(baseLogger, method_name="get_batchjob_info")
        res = {}
        tmpLog.info("Collect job info for batchid {}".format(batchid))
        info_dict = self.get_moabjob_info(batchid)
        tmpLog.info("Got: {0}".format(info_dict))
        if info_dict:
            tmpLog.debug("Translate results")
            res["status"] = self.translate_status(info_dict["state"])
            res["nativeStatus"] = info_dict["state"]
            res["nativeExitCode"] = info_dict["exit_code"]
            res["nativeExitMsg"] = self.get_message(info_dict["exit_code"])
            res["start_time"] = self.fixdate(info_dict["start_time"])
            res["finish_time"] = self.fixdate(info_dict["finish_time"])
        tmpLog.info("Collected job info: {0}".format(res))
        return res

    def get_moabjob_info(self, batchid):
        """
        Parsing of checkjob output to get job state, exit code, start time, finish time (if available)
        :return job_info dictonary:
        """
        tmpLog = self.make_logger(baseLogger, method_name="get_moabjob_info")

        job_info = {"state": "", "exit_code": None, "queued_time": None, "start_time": None, "finish_time": None}

        cmd = "checkjob -v {0}".format(batchid)
        p = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # check return code
        stdOut, stdErr = p.communicate()
        retCode = p.returncode
        checkjob_str = ""
        if retCode == 0:
            checkjob_str = stdOut
        else:
            tmpLog.info("checkjob failed with errcode: {0}\nstdout:{1}\nstderr:{2}".format(retCode, stdOut, stdErr))
            return {}
        if checkjob_str:
            checkjob_out = checkjob_str.splitlines()
            for l in checkjob_out:
                if l.startswith("State: "):
                    job_info["state"] = l[7:].split()[0]
                elif l.startswith("Completion Code: "):
                    job_info["exit_code"] = int(l[17:].split()[0])
                    if "Time: " in l:
                        job_info["finish_time"] = l[l.index("Time: ") + 6 :]
                elif l.startswith("StartTime: "):
                    job_info["start_time"] = l[11:]
                elif l.startswith("WallTime: "):
                    tmpLog.info(l)
        tmpLog.debug("checkjob parsing results: {0}".format(job_info))
        return job_info

    def translate_status(self, status):
        """
        MOAB status to worker status
        :param status:
        :return:
        """
        submited = ["deferred", "hold", "idle", "migrated", "staged"]
        running = ["starting", "running", "suspended", "canceling"]
        finished = ["completed"]
        cancelled = ["removed"]
        failed = ["vacated"]
        status = status.lower()
        if status in submited:
            return ws.ST_submitted
        elif status in running:
            return ws.ST_running
        elif status in finished:
            return ws.ST_finished
        elif status in cancelled:
            return ws.ST_cancelled
        elif status in failed:
            return ws.ST_failed
        else:
            return ws.ST_finished

    def get_message(self, exit_code):
        codes_messages = {
            0: "No errors reported",
            1: "The last command the job ran was unsuccessful or job exited with error (SIGHUP)",
            2: "The last command the job ran was unsuccessful or job exited with error (SIGINT)",
            3: "The last command the job ran was unsuccessful or job exited with error (SIGQUIT)",
            11: "Job exited with segmentation fault (SIGSEGV)",
            126: "Command invoked cannot execute",
            127: "Command not found",
            128: "Invalid argument to exit",
            134: "Fortran floating point exception or other runtime error (SIGABRT)",
            137: "Job stopped by user (SIGKILL) or for exceeding a memory limit (stopped by the kernel)",
            139: "Invalid memory access attempt (SIGSEGV)",
            143: "Job stopped by user (SIGTERM)",
            265: "Job stopped by user (SIGKILL)",
            271: "Job stopped by user (SIGTERM)",
            -1: "Job execution failed, before files, no retry",
            -2: "Job execution failed, after files, no retry",
            -3: "Job execution failed, do retry",
            -4: "Job aborted on MOM initialization",
            -5: "Job aborted on MOM init, chkpt, no migrate",
            -6: "Job aborted on MOM init, chkpt, ok migrate",
            -7: "Job restart failed",
            -8: "Exec() of user command failed",
            -9: "Could not create/open stdout stderr files",
            -10: "Job exceeded a memory limit (stopped by the resource manager)",
            -11: "Job exceeded a walltime limit",
            -12: "Job exceeded a CPU time limit",
            -13: "Could not create the jobs control groups (cgroups)",
        }

        if exit_code in codes_messages.keys():
            return codes_messages[exit_code]
        else:
            return "No message for this code"

    def fixdate(self, date_str):
        """
        moab format does not have year field, this should be fixed

        :param date_str:
        :return: date (datetime object)
        """
        tmpLog = self.make_logger(baseLogger, method_name="fixdate")
        if not date_str:
            return None
        tmpLog.debug("Date to fix: {0}".format(date_str))
        format_str = "%a %b %d %H:%M:%S %Y"
        date_str = " ".join([date_str, str(datetime.datetime.now().year)])
        date = datetime.datetime.strptime(date_str, format_str)
        if date > datetime.datetime.now():
            date_str = " ".join([date_str, str(datetime.datetime.now().year - 1)])
        date = datetime.datetime.strptime(date_str, format_str)

        tmpLog.debug("Full date: {0}".format(str(date)))
        utc_offset = datetime.timedelta(0, 18000, 0)  # 5H UTC offset for Oak-Ridge
        tmpLog.debug("UTC offset: {0}".format(str(utc_offset)))
        fixed_date = date + utc_offset
        tmpLog.debug("Fixed date: {0}".format(str(fixed_date)))

        return fixed_date

    def get_resources(self):
        """
        Fucnction to provide number of nodes with walltime limit to worker maker
        :return:
        nodes: integer
        walltime: intger, seconds
        """
        tmpLog = self.make_logger(baseLogger, method_name="get_backfill")
        tmpLog.info("Looking for gap more than '%s' sec" % self.minWalltime)
        nodes = 0
        walltime = self.minWalltime

        backfill = self.get_backfill()
        if backfill:
            for n in sorted(backfill.keys(), reverse=True):
                if self.minWalltime <= backfill[n] and nodes <= n:
                    nodes = n
                    walltime = backfill[n] - 120
                    break

        if nodes < self.minNodes:
            nodes = 0
        tmpLog.info("Nodes: {0} Walltime: {1}".format(nodes, walltime))

        return nodes, walltime

    def get_backfill(self):
        #  Function collect information about current available resources and
        #  return number of nodes with possible maximum value for walltime according Titan policy
        #
        tmpLog = self.make_logger(baseLogger, method_name="get_backfill")
        res = {}
        cmd = "showbf --blocking -p %s" % self.partition
        p = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # check return code
        stdOut, stdErr = p.communicate()
        retCode = p.returncode
        tmpLog.info("retCode={0}".format(retCode))
        showbf_str = ""
        if retCode == 0:
            showbf_str = stdOut
        else:
            tmpLog.error("showbf failed with errcode: {0} stdout: {1} stderr: {2}".format(retCode, stdOut, stdErr))
            return res

        tmpLog.debug("Available resources in {0} partition\n{1}".format(self.partition, showbf_str))
        if showbf_str:
            shobf_out = showbf_str.splitlines()
            tmpLog.info("Fitted resources")
            for l in shobf_out[2:]:
                d = l.split()
                nodes = int(d[2])
                if not d[3] == "INFINITY":
                    walltime_arr = d[3].split(":")
                    if len(walltime_arr) < 4:
                        walltime_sec = int(walltime_arr[0]) * (60 * 60) + int(walltime_arr[1]) * 60 + int(walltime_arr[2])
                        if walltime_sec > 24 * 3600:  # in case we will have more than 24H
                            walltime_sec = 24 * 3600
                    else:
                        walltime_sec = 24 * 3600
                else:
                    walltime_sec = 24 * 3600  # max walltime for Titan

                # Fitting Titan policy
                # https://www.olcf.ornl.gov/kb_articles/titan-scheduling-policy/
                if self.maxNodes:
                    nodes = self.maxNodes if nodes > self.maxNodes else nodes

                if nodes < 125 and walltime_sec > 2 * 3600:  # less than 125 nodes, max 2H
                    walltime_sec = 2 * 3600
                elif nodes < 312 and walltime_sec > 6 * 3600:  # between 125 and 312 nodes, max 6H
                    walltime_sec = 6 * 3600
                elif nodes < 3749 and walltime_sec > 12 * 3600:  # between 312 and 3749 nodes, max 12H
                    walltime_sec = 12 * 3600

                tmpLog.info("Nodes: %s, Walltime (str): %s, Walltime (sec) %s" % (nodes, d[3], walltime_sec))

                res.update({nodes: walltime_sec})

        return res
