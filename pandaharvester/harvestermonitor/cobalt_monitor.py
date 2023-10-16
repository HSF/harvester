import re

try:
    import subprocess32 as subprocess
except BaseException:
    import subprocess
import json
import os.path
from pprint import pprint

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger("cobalt_monitor")


# qstat output
# JobID  User     WallTime  Nodes  State   Location
# ===================================================
# 77734  fcurtis  06:00:00  64     queued  None


# monitor for HTCONDOR batch system
class CobaltMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check workers
    def check_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            # print "pprint(dir(workSpec))"
            # pprint(dir(workSpec))
            # print "pprint(vars(workSpec))"
            # pprint(vars(workSpec))
            # make logger
            tmpLog = self.make_logger(baseLogger, "workerID={0}".format(workSpec.workerID), method_name="check_workers")
            # first command
            comStr = "qstat {0}".format(workSpec.batchID)
            # first check
            tmpLog.debug("check with {0}".format(comStr))
            p = subprocess.Popen(comStr.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            oldStatus = workSpec.status
            newStatus = None
            # first check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug("retCode= {0}".format(retCode))
            tmpLog.debug("stdOut = {0}".format(stdOut))
            tmpLog.debug("stdErr = {0}".format(stdErr))
            errStr = ""
            if retCode == 0:
                # batch job is still running and has a state, output looks like this:
                # JobID   User    WallTime  Nodes  State   Location
                # ===================================================
                # 124559  hdshin  06:00:00  64     queued  None

                lines = stdOut.split("\n")
                parts = lines[2].split()
                batchid = parts[0]
                user = parts[1]
                walltime = parts[2]
                nodes = parts[3]
                state = parts[4]

                if int(batchid) != int(workSpec.batchID):
                    errStr += "qstat returned status for wrong batch id %s != %s" % (batchid, workSpec.batchID)
                    newStatus = WorkSpec.ST_failed
                else:
                    if "running" in state:
                        newStatus = WorkSpec.ST_running
                    elif "queued" in state:
                        newStatus = WorkSpec.ST_submitted
                    elif "user_hold" in state:
                        newStatus = WorkSpec.ST_submitted
                    elif "starting" in state:
                        newStatus = WorkSpec.ST_running
                    elif "killing" in state:
                        newStatus = WorkSpec.ST_failed
                    elif "exiting" in state:
                        newStatus = WorkSpec.ST_running
                    elif "maxrun_hold" in state:
                        newStatus = WorkSpec.ST_submitted
                    else:
                        raise Exception('failed to parse job state "%s" qstat stdout: %s\n stderr: %s' % (state, stdOut, stdErr))

                retList.append((newStatus, errStr))
            elif retCode == 1 and len(stdOut.strip()) == 0 and len(stdErr.strip()) == 0:
                tmpLog.debug("job has already exited, checking cobalt log for exit status")
                # exit code 1 and stdOut/stdErr has no content means job exited
                # need to look at cobalt log to determine exit status

                cobalt_logfile = os.path.join(workSpec.get_access_point(), "cobalt.log")
                if os.path.exists(cobalt_logfile):
                    return_code = None
                    job_cancelled = False
                    for line in open(cobalt_logfile):
                        # looking for line like this:
                        # Thu Aug 24 19:01:20 2017 +0000 (UTC) Info: task completed normally with an exit code of 0; initiating job cleanup and removal
                        if "task completed normally" in line:
                            start_index = line.find("exit code of ") + len("exit code of ")
                            end_index = line.find(";", start_index)
                            str_return_code = line[start_index:end_index]
                            if "None" in str_return_code:
                                return_code = -1
                            else:
                                return_code = int(str_return_code)
                            break
                        elif "maximum execution time exceeded" in line:
                            errStr += " batch job exceeded wall clock time "
                        elif "user delete requested" in line:
                            errStr += " job was cancelled "
                            job_cancelled = True

                    if return_code == 0:
                        tmpLog.debug("job finished normally")
                        newStatus = WorkSpec.ST_finished
                        retList.append((newStatus, errStr))
                    elif return_code is None:
                        if job_cancelled:
                            tmpLog.debug("job was cancelled")
                            errStr += " job cancelled "
                            newStatus = WorkSpec.ST_cancelled
                            retList.append((newStatus, errStr))
                        else:
                            tmpLog.debug("job has no exit code, failing job")
                            errStr += " exit code not found in cobalt log file %s " % cobalt_logfile
                            newStatus = WorkSpec.ST_failed
                            retList.append((newStatus, errStr))
                    else:
                        tmpLog.debug(" non zero exit code %s from batch job id %s" % (return_code, workSpec.batchID))
                        errStr += " non-zero exit code %s from batch job id %s " % (return_code, workSpec.batchID)
                        newStatus = WorkSpec.ST_failed
                        retList.append((newStatus, errStr))
                else:
                    tmpLog.debug(" cobalt log file does not exist")
                    errStr += " cobalt log file %s does not exist " % cobalt_logfile
                    newStatus = WorkSpec.ST_failed
                    retList.append((newStatus, errStr))

            tmpLog.debug("batchStatus {0} -> workerStatus {1}".format(oldStatus, newStatus))
            tmpLog.debug("errStr: %s" % errStr)

        return True, retList
