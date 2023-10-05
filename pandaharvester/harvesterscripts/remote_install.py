import os
import re
import sys
import shutil
import argparse
import tempfile
import subprocess
import paramiko
import logging

from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper


class TemporaryDirectory(object):
    def __enter__(self):
        self.name = tempfile.mkdtemp()
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.name)


def make_ssh_connection(ssh_host, ssh_port, ssh_username, ssh_password, pass_phrase, private_key, jump_host, jump_port):
    # ssh
    sshClient = paramiko.SSHClient()
    sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if jump_host is None:
        # direct SSH
        sshClient.connect(ssh_host, ssh_port, username=ssh_username, password=ssh_password, passphrase=pass_phrase, key_filename=private_key)
    else:
        # via jump host
        sshClient.connect(jump_host, jump_port, username=ssh_username, password=ssh_password, passphrase=pass_phrase, key_filename=private_key)
        transport = sshClient.get_transport()
        dst_address = (ssh_host, ssh_port)
        src_address = (jump_host, jump_port)
        channel = transport.open_channel("direct-tcpip", dst_address, src_address)
        jumpClient = paramiko.SSHClient()
        jumpClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        jumpClient.connect(ssh_host, ssh_port, username=ssh_username, password=ssh_password, passphrase=pass_phrase, key_filename=private_key, sock=channel)
    return sshClient


def main():
    logging.basicConfig()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--remoteDir", action="store", dest="remoteDir", default="harvester", help="directory on the remote target machine where harvester is installed"
    )
    parser.add_argument(
        "--remoteBuildDir",
        action="store",
        dest="remoteBuildDir",
        default="harvester_build",
        help="directory on the remote target machine where harvester is build",
    )
    parser.add_argument("--remotePythonSetup", action="store", dest="remotePythonSetup", default="", help="python setup on remote target machine")
    parser.add_argument("--queueName", action="store", dest="queueName", default=None, required=True, help="the name of queue where harvester is installed")
    parser.add_argument("--middleware", action="store", dest="middleware", default="rpc", help="middleware to access the remote target machine")
    options = parser.parse_args()

    # remove ~/ which doesn't work with sftp
    options.remoteDir = re.sub("^~/", "", options.remoteDir)
    options.remoteBuildDir = re.sub("^~/", "", options.remoteBuildDir)

    # get queue
    qcm = QueueConfigMapper()
    qcm.load_data()
    queueConfig = qcm.get_queue(options.queueName)
    if queueConfig is None:
        print("ERROR: queue={0} not found in panda_queueconfig.json".format(options.queueName))
        sys.exit(1)

    # get middleware
    if not hasattr(queueConfig, options.middleware):
        print("ERROR: middleware={0} is not defined for {1} in panda_queueconfig.json".format(options.middleware, options.queueName))
        sys.exit(1)
    middleware = getattr(queueConfig, options.middleware)

    # get ssh parameters
    sshHost = middleware["remoteHost"]
    try:
        sshPort = middleware["remotePort"]
    except Exception:
        sshPort = 22
    sshUserName = middleware["sshUserName"]
    try:
        sshPassword = middleware["sshPassword"]
    except Exception:
        sshPassword = None

    privateKey = None
    passPhrase = None
    if sshPassword is None:
        try:
            privateKey = middleware["privateKey"]
        except Exception:
            print("ERROR: set sshPassword or privateKey in middleware={0}".format(options.middleware))
            sys.exit(1)
        try:
            passPhrase = middleware["passPhrase"]
        except Exception:
            passPhrase = None

    try:
        jumpHost = middleware["jumpHost"]
    except Exception:
        jumpHost = None
    try:
        jumpPort = middleware["jumpPort"]
    except Exception:
        jumpPort = 22

    # ssh
    sshClient = make_ssh_connection(sshHost, sshPort, sshUserName, sshPassword, passPhrase, privateKey, jumpHost, jumpPort)

    # get remote python version
    exec_out = sshClient.exec_command(";".join([options.remotePythonSetup, """python -c 'import sys;print("{0}{1}".format(*(sys.version_info[:2])))' """]))
    remotePythonVer = exec_out[1].read().rstrip()
    sshClient.close()
    print("remote python version : {0}".format(remotePythonVer))

    # make tmp dir
    with TemporaryDirectory() as tmpDir:
        harvesterGit = "git+git://github.com/PanDAWMS/panda-harvester.git"

        # get all dependencies
        print("getting dependencies")
        p = subprocess.Popen("pip download -d {0} {1}; rm -rf {0}/*".format(tmpDir, harvesterGit), stdout=subprocess.PIPE, shell=True)
        stdout, stderr = p.communicate()
        packages = []
        for line in stdout.split("\n"):
            if line.startswith("Successfully downloaded"):
                packages = line.split()[2:]
        packages.append(harvesterGit)
        packages.append("pip")
        packages.remove("pandaharvester")

        # download packages
        print("pip download to {0}".format(tmpDir))
        for package in packages:
            print("getting {0}".format(package))
            ret = subprocess.call("pip download --no-deps --python-version {0} -d {1} {2}".format(remotePythonVer, tmpDir, package), shell=True)
            if ret != 0:
                print("ERROR: failed to download {0}".format(package))
                sys.exit(1)

        # sftp
        sshClient = make_ssh_connection(sshHost, sshPort, sshUserName, sshPassword, passPhrase, privateKey, jumpHost, jumpPort)
        try:
            sshClient.exec_command("rm -rf {0}; mkdir -p {0}".format(options.remoteBuildDir))
        except Exception:
            pass
        sftp = sshClient.open_sftp()
        for name in os.listdir(tmpDir):
            path = os.path.join(tmpDir, name)
            if os.path.isdir(path):
                continue
            remotePath = os.path.join(options.remoteBuildDir, name)
            print("copy {0} to {1}".format(name, remotePath))
            sftp.put(path, remotePath)

        # install
        print("install harvester")
        buildDir = options.remoteBuildDir
        if not buildDir.startswith("/"):
            buildDir = "~/" + buildDir
        exec_out = sshClient.exec_command(
            ";".join(
                [options.remotePythonSetup, "cd {0}".format(options.remoteDir), "pip install pip pandaharvester --no-index --find-links {0}".format(buildDir)]
            )
        )
        print(exec_out[1].read())
        print(exec_out[2].read())
        sshClient.close()


if __name__ == "__main__":
    main()
