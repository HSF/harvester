import psutil
import subprocess
import re

from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.service_metrics_spec import ServiceMetricSpec

# logger
_logger = core_utils.setup_logger('service_monitor')


# class to monitor the service, e.g. memory usage
class ServiceMonitor(AgentBase):
    # constructor
    def __init__(self, pid_file, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.db_proxy = DBProxy()
        self.pid_file = pid_file
        self.pid = self.get_master_pid()

    def get_master_pid(self):
        """
        Gets the master pid from the lock file
        :return:
        """
        fh = open(self.pid_file, 'r')

        try:
            pid = int(file.readline())
        except:
            pid = None

        fh.close()
        return pid

    def get_memory_n_cpu(self):
        """
        sum memory of whole process tree starting from master pid
        :return: rss in MiB
        """
        try:
            # obtain master process
            master_process = psutil.Process(self.pid)
            rss = master_process.memory_info()[0]
            cpu_pc = master_process.cpu_percent()
            for child in master_process.children(recursive=True):
                rss += child.process.memory_info()[0]
                cpu_pc += child.process.cpu_percent()

            # convert bytes to MiB
            rss_mib = rss / float(2 ** 20)
        except:
            rss_mib = None
            cpu_pc = None

        return rss_mib, cpu_pc

    def volume_use(self, volume_name):
        command = "df -Pkh /" + volume_name
        used_amount = 0
        tmp_array = command.split()
        output = subprocess.Popen(tmp_array, stdout=subprocess.PIPE).communicate()[0]

        for line in output.split('\n'):
            if re.search(volume_name, line):
                used_amount = re.search(r"(\d+)\%", line).group(1)

        return used_amount

    # main loop
    def run(self):
        while True:
            _logger.debug('Running service monitor')

            service_metrics = {}

            # get memory usage
            rss_mib, cpu_pc = self.get_memory_n_cpu()
            service_metrics['rss_mib'] = rss_mib
            service_metrics['cpu_pc'] = cpu_pc
            _logger.debug('Memory usage: {0} MiB, CPU usage: {1}'.format(rss_mib, cpu_pc))

            # get volume usage
            try:
                volumes = harvester_config.service_monitor.disk_volumes.split(',')
            except:
                volumes = []
            for volume in volumes:
                volume_use = self.volume_use(volume)
                service_metrics['volume_{0}'.format(volume)] = volume_use

            service_metrics_spec = ServiceMetricSpec(service_metrics)
            self.db_proxy.insert_service_metrics(service_metrics_spec)

            # check if being terminated
            try:
                sleep_time = harvester_config.service_monitor.sleepTime
            except:
                sleep_time = 300

            if self.terminated(sleep_time, randomize=False):
                return

