import random
import threading
import uuid
import socket

import six
import pexpect

from pandaharvester.harvestercore import core_utils

if six.PY2:
    pexpect_spawn = pexpect.spawn
else:
    pexpect_spawn = pexpect.spawnu

# logger
baseLogger = core_utils.setup_logger("ssh_tunnel_pool")


# Pool of SSH tunnels
class SshTunnelPool(object):
    # constructor
    def __init__(self):
        self.lock = threading.Lock()
        self.pool = dict()
        self.params = dict()

    # make a dict key
    def make_dict_key(self, host, port):
        return "{0}:{1}".format(host, port)

    # make a tunnel server
    def make_tunnel_server(
        self,
        remote_host,
        remote_port,
        remote_bind_port=None,
        num_tunnels=1,
        ssh_username=None,
        ssh_password=None,
        private_key=None,
        pass_phrase=None,
        jump_host=None,
        jump_port=None,
        login_timeout=60,
        reconnect=False,
        with_lock=True,
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
                "remote_bind_port": remote_bind_port,
                "num_tunnels": num_tunnels,
                "ssh_username": ssh_username,
                "ssh_password": ssh_password,
                "private_key": private_key,
                "pass_phrase": pass_phrase,
                "jump_host": jump_host,
                "jump_port": jump_port,
                "login_timeout": login_timeout,
            }
        else:
            remote_bind_port = self.params[dict_key]["remote_bind_port"]
            num_tunnels = self.params[dict_key]["num_tunnels"]
            ssh_username = self.params[dict_key]["ssh_username"]
            ssh_password = self.params[dict_key]["ssh_password"]
            private_key = self.params[dict_key]["private_key"]
            pass_phrase = self.params[dict_key]["pass_phrase"]
            jump_host = self.params[dict_key]["jump_host"]
            jump_port = self.params[dict_key]["jump_port"]
            login_timeout = self.params[dict_key]["login_timeout"]
        # make a tunnel server
        for i in range(num_tunnels - len(self.pool[dict_key])):
            # get a free port
            s = socket.socket()
            s.bind(("", 0))
            com = "ssh -L {local_bind_port}:127.0.0.1:{remote_bind_port} "
            com += "-p {remote_port} {ssh_username}@{remote_host} "
            com += "-o ServerAliveInterval=120 -o ServerAliveCountMax=2 "
            if private_key is not None:
                com += "-i {private_key} "
            if jump_port is not None:
                com += '-o ProxyCommand="ssh -p {jump_port} {ssh_username}@{jump_host} -W %h:%p" '
            local_bind_port = s.getsockname()[1]
            com = com.format(
                remote_host=remote_host,
                remote_port=remote_port,
                remote_bind_port=remote_bind_port,
                ssh_username=ssh_username,
                private_key=private_key,
                jump_host=jump_host,
                jump_port=jump_port,
                local_bind_port=local_bind_port,
            )
            s.close()
            # list of expected strings
            loginString = "login_to_be_confirmed_with " + uuid.uuid4().hex
            expected_list = [
                pexpect.EOF,
                pexpect.TIMEOUT,
                "(?i)are you sure you want to continue connecting",
                "(?i)password:",
                "(?i)enter passphrase for key.*",
                loginString,
            ]
            c = pexpect_spawn(com, echo=False)
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
                    baseLogger.error("timeout when making a tunnel with com={0} out={1}".format(com, c.buffer))
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
                self.pool[dict_key].append((local_bind_port, c))
        if with_lock:
            self.lock.release()

    # get a tunnel
    def get_tunnel(self, remote_host, remote_port):
        dict_key = self.make_dict_key(remote_host, remote_port)
        self.lock.acquire()
        active_tunnels = []
        someClosed = False
        for port, child in self.pool[dict_key]:
            if child.isalive():
                active_tunnels.append([port, child])
            else:
                child.close()
                someClosed = True
        if someClosed:
            self.make_tunnel_server(remote_host, remote_port, reconnect=True, with_lock=False)
            active_tunnels = [item for item in self.pool[dict_key] if item[1].isalive()]
        if len(active_tunnels) > 0:
            port, child = random.choice(active_tunnels)
        else:
            port, child = None, None
        self.lock.release()
        return ("127.0.0.1", port, child)


# singleton
sshTunnelPool = SshTunnelPool()
del SshTunnelPool
