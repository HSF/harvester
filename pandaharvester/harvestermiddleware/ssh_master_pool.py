import random
import threading
import uuid
import os
import time

import six
import pexpect
import tempfile

try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

from pandaharvester.harvestercore import core_utils

if six.PY2:
    pexpect_spawn = pexpect.spawn
else:
    pexpect_spawn = pexpect.spawnu

# logger
baseLogger = core_utils.setup_logger("ssh_master_pool")


# Pool of SSH control masters
class SshMasterPool(object):
    # constructor
    def __init__(self):
        self.lock = threading.Lock()
        self.pool = dict()
        self.params = dict()

    # make a dict key
    def make_dict_key(self, host, port):
        return "{0}:{1}".format(host, port)

    # make a control master
    def make_control_master(
        self,
        remote_host,
        remote_port,
        num_masters=1,
        ssh_username=None,
        ssh_password=None,
        private_key=None,
        pass_phrase=None,
        jump_host=None,
        jump_port=None,
        login_timeout=60,
        reconnect=False,
        with_lock=True,
        sock_dir=None,
        connection_lifetime=None,
    ):
        dict_key = self.make_dict_key(remote_host, remote_port)
        if with_lock:
            self.lock.acquire()
        # make dicts
        if dict_key not in self.pool:
            self.pool[dict_key] = []
        # preserve parameters
        if not reconnect:
            self.params[dict_key] = {
                "num_masters": num_masters,
                "ssh_username": ssh_username,
                "ssh_password": ssh_password,
                "private_key": private_key,
                "pass_phrase": pass_phrase,
                "jump_host": jump_host,
                "jump_port": jump_port,
                "login_timeout": login_timeout,
                "sock_dir": sock_dir,
                "connection_lifetime": connection_lifetime,
            }
        else:
            num_masters = self.params[dict_key]["num_masters"]
            ssh_username = self.params[dict_key]["ssh_username"]
            ssh_password = self.params[dict_key]["ssh_password"]
            private_key = self.params[dict_key]["private_key"]
            pass_phrase = self.params[dict_key]["pass_phrase"]
            jump_host = self.params[dict_key]["jump_host"]
            jump_port = self.params[dict_key]["jump_port"]
            login_timeout = self.params[dict_key]["login_timeout"]
            sock_dir = self.params[dict_key]["sock_dir"]
            connection_lifetime = self.params[dict_key]["connection_lifetime"]
        # make a master
        for i in range(num_masters - len(self.pool[dict_key])):
            # make a socket file
            sock_file = os.path.join(sock_dir, "sock_{0}_{1}".format(remote_host, uuid.uuid4().hex))
            com = "ssh -M -S {sock_file} "
            com += "-p {remote_port} {ssh_username}@{remote_host} "
            com += "-o ServerAliveInterval=120 -o ServerAliveCountMax=2 "
            if private_key is not None:
                com += "-i {private_key} "
            if jump_host is not None and jump_port is not None:
                com += '-o ProxyCommand="ssh -p {jump_port} {ssh_username}@{jump_host} -W %h:%p" '
            com = com.format(
                remote_host=remote_host,
                remote_port=remote_port,
                ssh_username=ssh_username,
                private_key=private_key,
                jump_host=jump_host,
                jump_port=jump_port,
                sock_file=sock_file,
            )
            loginString = "login_to_be_confirmed_with " + uuid.uuid4().hex
            com += "'echo {0}; bash".format(loginString)
            # list of expected strings
            expected_list = [
                pexpect.EOF,
                pexpect.TIMEOUT,
                "(?i)are you sure you want to continue connecting",
                "(?i)password:",
                "(?i)enter passphrase for key.*",
                loginString,
            ]
            c = pexpect_spawn(com, echo=False)
            baseLogger.debug("pexpect_spawn")
            c.logfile_read = baseLogger.handlers[0].stream
            isOK = False
            for iTry in range(3):
                idx = c.expect(expected_list, timeout=login_timeout)
                if idx == expected_list.index(loginString):
                    # succeeded
                    isOK = True
                    break
                if idx == 1:
                    # timeout
                    baseLogger.error("timeout when making a master with com={0} out={1}".format(com, c.buffer))
                    c.close()
                    break
                if idx == 2:
                    # new certificate
                    c.sendline("yes")
                    idx = c.expect(expected_list, timeout=login_timeout)
                if idx == 1:
                    # timeout
                    baseLogger.error("timeout after accepting new cert with com={0} out={1}".format(com, c.buffer))
                    c.close()
                    break
                if idx == 3:
                    # password prompt
                    c.sendline(ssh_password)
                elif idx == 4:
                    # passphrase prompt
                    c.sendline(pass_phrase)
                elif idx == 0:
                    baseLogger.error("something weired with com={0} out={1}".format(com, c.buffer))
                    c.close()
                    break
                # exec to confirm login
                c.sendline("echo {0}".format(loginString))
            if isOK:
                conn_exp_time = (time.time() + connection_lifetime) if connection_lifetime is not None else None
                self.pool[dict_key].append((sock_file, c, conn_exp_time))
        if with_lock:
            self.lock.release()

    # get a connection
    def get_connection(self, remote_host, remote_port, exec_string):
        baseLogger.debug("get_connection start")
        dict_key = self.make_dict_key(remote_host, remote_port)
        self.lock.acquire()
        active_masters = []
        someClosed = False
        for sock_file, child, conn_exp_time in list(self.pool[dict_key]):
            if child.isalive() and time.time() <= conn_exp_time:
                active_masters.append((sock_file, child, conn_exp_time))
            else:
                child.close()
                self.pool[dict_key].remove((sock_file, child, conn_exp_time))
                someClosed = True
                if child.isalive():
                    baseLogger.debug("a connection process is dead")
                else:
                    baseLogger.debug("a connection is expired")
        if someClosed:
            self.make_control_master(remote_host, remote_port, reconnect=True, with_lock=False)
            active_masters = [item for item in self.pool[dict_key] if os.path.exists(item[0])]
            baseLogger.debug("reconnected; now {0} active connections".format(len(active_masters)))
        if len(active_masters) > 0:
            sock_file, child, conn_exp_time = random.choice(active_masters)
            con = subprocess.Popen(
                ["ssh", "dummy", "-S", sock_file, exec_string], shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
        else:
            con = None
        self.lock.release()
        return con


# singleton
sshMasterPool = SshMasterPool()
del SshMasterPool
