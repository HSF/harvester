import rpyc
import daemon
import daemon.pidfile
import argparse
import logging

from pandaharvester.harvestercore.plugin_factory import PluginFactory


# RPC bot running on remote node
class RpcBot(rpyc.Service):

    # initialization action
    def on_connect(self, conn):
        self.pluginFactory = PluginFactory(no_db=True)


    ######################
    # submitter section

    # submit workers
    def exposed_submit_workers(self, plugin_config, workspec_list):
        core = self.pluginFactory.get_plugin(plugin_config)
        return core.submit_workers(workspec_list)


    ######################
    # monitor section

    # check workers
    def exposed_check_workers(self, plugin_config, workspec_list):
        core = self.pluginFactory.get_plugin(plugin_config)
        return core.check_workers(workspec_list)


    ######################
    # messenger section

    # setup access points
    def exposed_setup_access_points(self, plugin_config, workspec_list):
        core = self.pluginFactory.get_plugin(plugin_config)
        return core.setup_access_points(workspec_list)

    # feed jobs
    def exposed_feed_jobs(self, plugin_config, workspec, jobspec_list):
        core = self.pluginFactory.get_plugin(plugin_config)
        return core.feed_jobs(workspec, jobspec_list)

    # request job
    def exposed_job_requested(self, plugin_config, workspec):
        core = self.pluginFactory.get_plugin(plugin_config)
        return core.job_requested(workspec)

    # request kill
    def exposed_kill_requested(self, plugin_config, workspec):
        core = self.pluginFactory.get_plugin(plugin_config)
        return core.kill_requested(workspec)

    # is alive
    def exposed_is_alive(self, plugin_config, workspec, worker_heartbeat_limit):
        core = self.pluginFactory.get_plugin(plugin_config)
        return core.is_alive(workspec, worker_heartbeat_limit)

    # get work attributes
    def exposed_get_work_attributes(self, plugin_config, workspec):
        core = self.pluginFactory.get_plugin(plugin_config)
        return core.get_work_attributes(workspec)

    # get output files
    def exposed_get_files_to_stage_out(self, plugin_config, workspec):
        core = self.pluginFactory.get_plugin(plugin_config)
        return core.get_files_to_stage_out(workspec)

    # get events
    def exposed_events_to_update(self, plugin_config, workspec):
        core = self.pluginFactory.get_plugin(plugin_config)
        return core.events_to_update(workspec)

    # request events
    def exposed_events_requested(self, plugin_config, workspec):
        core = self.pluginFactory.get_plugin(plugin_config)
        return core.events_requested(workspec)

    # get PandaIDs
    def exposed_get_panda_ids(self, plugin_config, workspec):
        core = self.pluginFactory.get_plugin(plugin_config)
        return core.get_panda_ids(workspec)

    # post processing
    def exposed_post_processing(self, plugin_config, workspec, jobspec_list, map_type):
        core = self.pluginFactory.get_plugin(plugin_config)
        return core.post_processing(workspec, jobspec_list, map_type)

    # send ACK
    def exposed_acknowledge_events_files(self, plugin_config, workspec):
        core = self.pluginFactory.get_plugin(plugin_config)
        return core.acknowledge_events_files(workspec)


# main body
def main():
    logging.basicConfig()

    parser = argparse.ArgumentParser()
    parser.add_argument('--pid', action='store', dest='pid', default='/var/tmp/harvester_rpc.pid',
                        help='pid filename')
    parser.add_argument('--port', dest='port', type=int, default=18861,
                        help='the TCP port to bind to')
    parser.add_argument('--backlog', dest='backlog', type=int, default=10,
                        help='backlog for the port')
    options = parser.parse_args()
    # make daemon context
    dc = daemon.DaemonContext(pidfile=daemon.pidfile.PIDLockFile(options.pid))
    with dc:
        from rpyc.utils.server import ThreadedServer
        t = ThreadedServer(RpcBot, port=options.port, backlog=options.backlog,
                            protocol_config={"allow_all_attrs": True})
        t.start()


if __name__ == "__main__":
    main()
