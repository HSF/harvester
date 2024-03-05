import datetime
import os
import signal
import smtplib
import socket
import subprocess
import time
from email.mime.text import MIMEText

from pandalogger import logger_config

from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils

logDir = logger_config.daemon["logdir"]
if "PANDA_LOCK_DIR" in os.environ:
    lockFileName = os.path.join("PANDA_LOCK_DIR", "watcher.lock")
else:
    lockFileName = os.path.join(logDir, "watcher.lock")

# logger
_logger = core_utils.setup_logger("watcher")


# watching the system
class Watcher(AgentBase):
    # constructor
    def __init__(self, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.startTime = datetime.datetime.utcnow()

    # main loop
    def run(self):
        while True:
            # execute
            self.execute()
            # check if being terminated
            if self.terminated(harvester_config.watcher.sleepTime, randomize=False):
                return

    # main
    def execute(self):
        # avoid too early check
        if not self.singleMode and datetime.datetime.utcnow() - self.startTime < datetime.timedelta(seconds=harvester_config.watcher.checkInterval):
            return
        mainLog = core_utils.make_logger(_logger, f"id={self.get_pid()}", method_name="execute")
        mainLog.debug("start")
        # get file lock
        try:
            with core_utils.get_file_lock(lockFileName, harvester_config.watcher.checkInterval):
                try:
                    logFileNameList = harvester_config.watcher.logFileNameList.split(",")
                except Exception:
                    logFileNameList = ["panda-db_proxy.log"]
                lastTime = None
                logDuration = None
                lastTimeName = None
                logDurationName = None
                actionsList = harvester_config.watcher.actions.split(",")
                for logFileName in logFileNameList:
                    logFilePath = os.path.join(logDir, logFileName)
                    timeNow = datetime.datetime.utcnow()
                    if os.path.exists(logFilePath):
                        # get latest timestamp
                        tmpLogDuration = None
                        try:
                            p = subprocess.Popen(["tail", "-1", logFilePath], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                            line = p.communicate()[0]
                            tmpLastTime = datetime.datetime.strptime(str(line[:23], "utf-8"), "%Y-%m-%d %H:%M:%S,%f")
                        except Exception:
                            tmpLastTime = None
                        # get processing time for last 1000 queries
                        try:
                            p = subprocess.Popen(
                                f"tail -{harvester_config.watcher.nMessages} {logFilePath} | head -1",
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                shell=True,
                            )
                            line = p.communicate()[0]
                            firstTime = datetime.datetime.strptime(str(line[:23], "utf-8"), "%Y-%m-%d %H:%M:%S,%f")
                            if tmpLastTime is not None:
                                tmpLogDuration = tmpLastTime - firstTime
                        except Exception as e:
                            mainLog.warning(f"Skip with error {e.__class__.__name__}: {e}")
                        tmpMsg = "log={0} : last message at {0}. ".format(logFileName, tmpLastTime)
                        if tmpLogDuration is not None:
                            tmpMsg += f"{harvester_config.watcher.nMessages} messages took {tmpLogDuration.total_seconds()} sec"
                        mainLog.debug(tmpMsg)
                        if tmpLastTime is not None and (lastTime is None or lastTime > tmpLastTime):
                            lastTime = tmpLastTime
                            lastTimeName = logFileName
                        if tmpLogDuration is not None and (logDuration is None or logDuration < tmpLogDuration):
                            logDuration = tmpLogDuration
                            logDurationName = logFileName
                    # check timestamp
                    doAction = False
                    if (
                        harvester_config.watcher.maxStalled > 0
                        and lastTime is not None
                        and timeNow - lastTime > datetime.timedelta(seconds=harvester_config.watcher.maxStalled)
                    ):
                        mainLog.warning(f"last log message is too old in {lastTimeName}. seems to be stalled")
                        doAction = True
                    elif (
                        harvester_config.watcher.maxDuration > 0
                        and logDuration is not None
                        and logDuration.total_seconds() > harvester_config.watcher.maxDuration
                    ):
                        mainLog.warning(f"slow message generation in {logDurationName}. seems to be a performance issue")
                        doAction = True
                    # take action
                    if doAction:
                        # email
                        if "email" in actionsList:
                            # get pass phrase
                            toSkip = False
                            mailUser = None
                            mailPass = None
                            if harvester_config.watcher.mailUser != "" and harvester_config.watcher.mailPassword != "":
                                envName = harvester_config.watcher.passphraseEnv
                                if envName not in os.environ:
                                    tmpMsg = f"{envName} is undefined in etc/sysconfig/panda_harvester"
                                    mainLog.error(tmpMsg)
                                    toSkip = True
                                else:
                                    key = os.environ[envName]
                                    mailUser = core_utils.decrypt_string(key, harvester_config.watcher.mailUser)
                                    mailPass = core_utils.decrypt_string(key, harvester_config.watcher.mailPassword)
                            if not toSkip:
                                # message
                                msgBody = f"harvester {harvester_config.master.harvester_id} "
                                msgBody += f"is having a problem on {socket.getfqdn()} "
                                msgBody += f"at {datetime.datetime.utcnow()} (UTC)"
                                message = MIMEText(msgBody)
                                message["Subject"] = "Harvester Alarm"
                                message["From"] = harvester_config.watcher.mailFrom
                                message["To"] = harvester_config.watcher.mailTo
                                # send email
                                mainLog.debug(f"sending email to {harvester_config.watcher.mailTo}")
                                server = smtplib.SMTP(harvester_config.watcher.mailServer, harvester_config.watcher.mailPort)
                                if hasattr(harvester_config.watcher, "mailUseSSL") and harvester_config.watcher.mailUseSSL is True:
                                    server.starttls()
                                if mailUser is not None and mailPass is not None:
                                    server.login(mailUser, mailPass)
                                server.ehlo()
                                server.sendmail(harvester_config.watcher.mailFrom, harvester_config.watcher.mailTo.split(","), message.as_string())
                                server.quit()
                        # kill
                        if "kill" in actionsList:
                            # send USR2 fist
                            mainLog.debug("sending SIGUSR2")
                            os.killpg(os.getpgrp(), signal.SIGUSR2)
                            time.sleep(60)
                            mainLog.debug("sending SIGKILL")
                            os.killpg(os.getpgrp(), signal.SIGKILL)
                        elif "terminate" in actionsList:
                            mainLog.debug("sending SIGTERM")
                            os.killpg(os.getpgrp(), signal.SIGTERM)
                    else:
                        mainLog.debug(f"No action needed for {logFileName}")
        except IOError:
            mainLog.debug("skip as locked by another thread or too early to check")
        except Exception:
            core_utils.dump_error_message(mainLog)
        mainLog.debug("done")
