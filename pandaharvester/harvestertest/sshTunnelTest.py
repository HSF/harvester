import sys
import argparse


from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestermiddleware.ssh_tunnel_pool import sshTunnelPool


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queueName", action="store", dest="queueName", default=None, required=True, help="the name of queue where harvester is installed")
    parser.add_argument("--middleware", action="store", dest="middleware", default="rpc", help="middleware to access the remote target machine")
    options = parser.parse_args()

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
    sshTunnelPool.make_tunnel_server(
        sshHost,
        sshPort,
        remote_bind_port=middleware["remoteBindPort"],
        num_tunnels=1,
        ssh_username=sshUserName,
        ssh_password=sshPassword,
        private_key=privateKey,
        pass_phrase=passPhrase,
        jump_host=jumpHost,
        jump_port=jumpPort,
    )
    ssh = sshTunnelPool.get_tunnel(sshHost, sshPort)[-1]
    return ssh


if __name__ == "__main__":
    ssh = main()
    if ssh is None:
        print("ERROR: failed to make an SSH tunnel. See ssh_tunnel_pool.log for more details")
    else:
        print("OK")
