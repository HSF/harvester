import os
import time
import signal
import socket
import smtplib
import datetime
import subprocess
from email.mime.text import MIMEText

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterbody.agent_base import AgentBase

from pandalogger import logger_config

logDir = logger_config.daemon['logdir']
lockFileName = os.path.join(logDir, 'watcher.lock')

# logger
_logger = core_utils.setup_logger('watcher')


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
        if not self.singleMode and datetime.datetime.utcnow() - self.startTime \
                < datetime.timedelta(seconds=harvester_config.watcher.checkInterval):
            return
        mainLog = core_utils.make_logger(_logger, 'id={0}'.format(self.ident), method_name='execute')
        mainLog.debug('start')
        # get file lock
        try:
            with core_utils.get_file_lock(lockFileName, harvester_config.watcher.checkInterval):
                logFileName = os.path.join(logDir, 'panda-db_proxy.log')
                timeNow = datetime.datetime.utcnow()
                if os.path.exists(logFileName):
                    # get latest timestamp
                    try:
                        p = subprocess.Popen(['tail', '-1', logFileName],
                                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        line = p.stdout.readline()
                        lastTime = datetime.datetime.strptime(line[:23], "%Y-%m-%d %H:%M:%S,%f")
                    except:
                        lastTime = None
                    # get processing time for last 1000 queries
                    logDuration = None
                    try:
                        p = subprocess.Popen('tail -{0} {1} | head -1'.format(harvester_config.watcher.nMessages,
                                                                              logFileName),
                                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                        line = p.stdout.readline()
                        firstTime = datetime.datetime.strptime(line[:23], "%Y-%m-%d %H:%M:%S,%f")
                        if lastTime is not None:
                            logDuration = lastTime - firstTime
                    except:
                        pass
                    tmpMsg = 'last log message at {0}. '.format(lastTime)
                    if logDuration is not None:
                        tmpMsg += '{0} messages took {1} sec'.format(harvester_config.watcher.nMessages,
                                                                     logDuration.total_seconds())
                    mainLog.debug(tmpMsg)
                    # check timestamp
                    doAction = False
                    if harvester_config.watcher.maxStalled > 0 and lastTime is not None and \
                            timeNow - lastTime > datetime.timedelta(seconds=harvester_config.watcher.maxStalled):
                        mainLog.warning('last log message is too old. seems to be stalled')
                        doAction = True
                    elif harvester_config.watcher.maxDuration > 0 and logDuration is not None and \
                            logDuration.total_seconds() > harvester_config.watcher.maxDuration:
                        mainLog.warning('slow message generation. seems to be a performance issue')
                        doAction = True
                    # take action
                    if doAction:
                        # email
                        if 'email' in harvester_config.watcher.actions.split(','):
                            # get pass phrase
                            toSkip = False
                            mailUser = None
                            mailPass = None
                            if harvester_config.watcher.mailUser != '' and \
                                    harvester_config.watcher.mailPassword != '':
                                envName = harvester_config.watcher.passphraseEnv
                                if envName not in os.environ:
                                    tmpMsg = '{0} is undefined in etc/sysconfig/panda_harvester'.format(envName)
                                    mainLog.error(tmpMsg)
                                    toSkip = True
                                else:
                                    key = os.environ[envName]
                                    mailUser = core_utils.decrypt_string(key, harvester_config.watcher.mailUser)
                                    mailPass = core_utils.decrypt_string(key, harvester_config.watcher.mailPassword)
                            if not toSkip:
                                # message
                                msgBody = 'harvester {0} '.format(harvester_config.master.harvester_id)
                                msgBody += 'is having a problem on {0} '.format(socket.getfqdn())
                                msgBody += 'at {0} (UTC)'.format(datetime.datetime.utcnow())
                                message = MIMEText(msgBody)
                                message['Subject'] = "Harvester Alarm"
                                message['From'] = harvester_config.watcher.mailFrom
                                message['To'] = harvester_config.watcher.mailTo
                                # send email
                                mainLog.debug('sending email to {0}'.format(harvester_config.watcher.mailTo))
                                server = smtplib.SMTP(harvester_config.watcher.mailServer,
                                                      harvester_config.watcher.mailPort)
                                if hasattr(harvester_config.watcher, 'mailUseSSL') and \
                                        harvester_config.watcher.mailUseSSL is True:
                                    server.starttls()
                                if mailUser is not None and mailPass is not None:
                                    server.login(mailUser, mailPass)
                                server.ehlo()
                                server.sendmail(harvester_config.watcher.mailFrom,
                                                harvester_config.watcher.mailTo.split(','),
                                                message.as_string())
                                server.quit()
                        # kill
                        if 'kill' in harvester_config.watcher.actions.split(','):
                            # send USR2 fist
                            mainLog.debug('sending SIGUSR2')
                            os.killpg(os.getpgrp(), signal.SIGUSR2)
                            time.sleep(60)
                            mainLog.debug('sending SIGKILL')
                            os.killpg(os.getpgrp(), signal.SIGKILL)
                else:
                    mainLog.debug('skip as {0} is missing'.format(logFileName))
        except IOError:
            mainLog.debug('skip as locked by another thread or too early to check')
        except:
            core_utils.dump_error_message(mainLog)
        mainLog.debug('done')
