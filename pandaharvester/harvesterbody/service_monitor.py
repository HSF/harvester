import traceback
import psutil
import re
import multiprocessing
try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

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

        if pid_file is not None:
            self.pid_file = pid_file
        else:
            try:
                self.pid_file = harvester_config.service_monitor.pidfile
            except Exception:
                self.pid_file = None

        self.pid = self.get_master_pid()
        self.master_process = psutil.Process(self.pid)
        self.children = self.master_process.children(recursive=True)

        self.cpu_count = multiprocessing.cpu_count()

    def get_master_pid(self):
        """
        Gets the master pid from the lock file
        :return:
        """
        try:
            fh = open(self.pid_file, 'r')
            pid = int(fh.readline())
            fh.close()
        except Exception:
            _logger.error('Could not read pidfile "{0}"'.format(self.pid_file))
            pid = None

        return pid

    def refresh_children_list(self, children):

        children_refreshed = []

        for child_current in children:
            pid_current = child_current.pid
            found = False
            for child_stored in self.children:
                pid_stored = child_stored.pid
                if pid_stored == pid_current:
                    found = True
                    break

            if found:
                children_refreshed.append(child_stored)
            else:
                children_refreshed.append(child_current)

        self.children = children_refreshed

        return children_refreshed

    def get_memory_n_cpu(self):
        """
        sum memory of whole process tree starting from master pid
        :return: rss in MiB
        """
        try:
            master_process = self.master_process
            rss = master_process.memory_info()[0]
            memory_pc = master_process.memory_percent()
            cpu_pc = master_process.cpu_percent()

            children = self.refresh_children_list(master_process.children(recursive=True))
            for child in children:
                rss += child.memory_info()[0]
                memory_pc += child.memory_percent()
                cpu_pc += child.cpu_percent()

            # convert bytes to MiB
            rss_mib = rss / float(2 ** 20)
            # normalize cpu percentage by cpu count
            cpu_pc = cpu_pc * 1.0 / self.cpu_count
        except Exception:
            _logger.error('Excepted with: {0}'.format(traceback.format_exc()))
            rss_mib = None
            memory_pc = None
            cpu_pc = None

        return rss_mib, memory_pc, cpu_pc

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
            rss_mib, memory_pc, cpu_pc = self.get_memory_n_cpu()
            service_metrics['rss_mib'] = rss_mib
            service_metrics['memory_pc'] = memory_pc
            service_metrics['cpu_pc'] = cpu_pc
            _logger.debug('Memory usage: {0} MiB/{1}%, CPU usage: {2}'.format(rss_mib, memory_pc, cpu_pc))

            # get volume usage
            try:
                volumes = harvester_config.service_monitor.disk_volumes.split(',')
            except Exception:
                volumes = []
            for volume in volumes:
                volume_use = self.volume_use(volume)
                _logger.debug('Disk usage of {0}: {1} %'.format(volume, volume_use))
                service_metrics['volume_{0}_pc'.format(volume)] = volume_use

            service_metrics_spec = ServiceMetricSpec(service_metrics)
            self.db_proxy.insert_service_metrics(service_metrics_spec)

            # check if being terminated
            try:
                sleep_time = harvester_config.service_monitor.sleepTime
            except Exception:
                sleep_time = 300

            if self.terminated(sleep_time, randomize=False):
                return
