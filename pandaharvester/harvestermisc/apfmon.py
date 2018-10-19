"""
API described here: http://apfmon.lancs.ac.uk/help
"""

import requests
import json
import time
import traceback

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester import panda_pkg_info
from pandaharvester.harvestermisc import generic_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict

_base_logger = core_utils.setup_logger('apfmon')


def apfmon_active(method, *args, **kwargs):
    if cls.__active:
        method(*args, **kwargs)
    else:
        return

class Apfmon:

    def __init__(self, queue_config_mapper):

        try:
            self.__active = harvester_config.apfmon.active
        except:
            self.__active = False

        try:
            self.__worker_timeout = harvester_config.apfmon.worker_timeout
        except:
            self.__worker_timeout = 0.2

        try:
            self.__label_timeout = harvester_config.apfmon.worker_timeout
        except:
            self.__label_timeout = 0.5

        # TODO: make proper exception handling and defaults
        try:
            self.harvester_id = harvester_config.master.harvester_id
        except:
            self.harvester_id = 'DUMMY'

        try:
            self.base_url = harvester_config.apfmon.base_url
        except:
            self.base_url = 'http://apfmon.lancs.ac.uk/api'

        self.queue_config_mapper = queue_config_mapper

    def create_factory(self):
        """
        Creates or updates a harvester instance to APF Mon. Should be done at startup of the instance.
        """

        start_time = time.time()
        tmp_log = core_utils.make_logger(_base_logger, 'harvester_id={0}'.format(self.harvester_id),
                                         method_name='create_factory')

        if not self.__active:
            tmp_log.debug('APFMon reporting not enabled')
            return

        try:
            tmp_log.debug('start')

            url = '{0}/factories/{1}'.format(self.base_url, self.harvester_id)

            f = {'url': 'url_to_logs', # TODO: get the URL properly. This can be problematic when harvester runs on two nodes and one DB
                 'email': 'atlas-adc-harvester-central-support@cern.ch',
                 'version': panda_pkg_info.release_version}
            payload = json.dumps(f)

            r = requests.put(url, data=payload, timeout=self.__label_timeout)
            tmp_log.debug('registration ended with {0}'.format(r.status_code))
            end_time = time.time()
            tmp_log.debug('done (took {0})'.format(end_time - start_time))
        except:
            tmp_log.error('Excepted with: {0}'.format(traceback.format_exc()))

    def create_labels(self):
        """
        Creates or updates a collection of labels (=panda queues)
        """
        start_time = time.time()
        tmp_log = core_utils.make_logger(_base_logger, 'harvester_id={0}'.format(self.harvester_id),
                                         method_name='create_labels')

        if not self.__active:
            tmp_log.debug('APFMon reporting not enabled')
            return

        try:
            tmp_log.debug('start')

            url = '{0}/labels'.format(self.base_url)

            # get the active queues from the config mapper
            all_sites = self.queue_config_mapper.get_active_queues().keys()
            panda_queues_dict = PandaQueuesDict()

            # publish the active queues to APF mon in shards
            for sites in generic_utils.create_shards(all_sites, 20):
                labels = []
                for site in sites:
                    try:
                        site_info = panda_queues_dict.get(site, dict())

                        for queue in site_info['queues']:
                            ce = queue['ce_endpoint'].split('.')[0]
                            labels.append({'name': '{0}-{1}'.format(site, ce),
                                           'wmsqueue': site,
                                           'factory': self.harvester_id})
                    except:
                        tmp_log.error('Excepted for site {0} with: {1}'.format(site, traceback.format_exc()))


                payload = json.dumps(labels)

                r = requests.put(url, data=payload, timeout=self.__label_timeout)
                tmp_log.debug('label creation for {0} ended with {1}'.format(sites, r.status_code))

            end_time = time.time()
            tmp_log.debug('done (took {0})'.format(end_time - start_time))
        except:
            tmp_log.error('Excepted with: {0}'.format(traceback.format_exc()))

    def create_workers(self, worker_spec_list):
        """
        Creates a worker
        """
        start_time = time.time()
        tmp_log = core_utils.make_logger(_base_logger, 'harvester_id={0}'.format(self.harvester_id),
                                         method_name='create_workers')

        if not self.__active:
            tmp_log.debug('APFMon reporting not enabled')
            return

        try:
            tmp_log.debug('start')

            url = '{0}/jobs'.format(self.base_url)

            for worker_spec_shard in generic_utils.create_shards(worker_spec_list, 20):
                apfmon_workers = []
                for worker_spec in worker_spec_shard:
                    batch_id = worker_spec.batchID
                    factory = self.harvester_id
                    computingsite = worker_spec.computingSite
                    ce = worker_spec.computingElement.split('.')[0]

                    # extract the log URLs
                    work_attribs = worker_spec.workAttributes
                    stdout_url = work_attribs['stdOut']
                    stderr_url = work_attribs['stdErr']
                    log_url = work_attribs['batchLog']

                    jdl_url = '' # TODO: publish the jdl files

                    apfmon_worker = {'cid': batch_id,
                                     'factory': factory,
                                     'label': '{0}-{1}'.format(computingsite, ce),
                                     'jdlurl': jdl_url,
                                     'stdouturl': stdout_url,
                                     'stderrurl': stderr_url,
                                     'logurl': log_url
                                     }
                    tmp_log.debug('packed worker: {0}'.format(apfmon_worker))
                    apfmon_workers.append(apfmon_worker)

                payload = json.dumps(apfmon_workers)
                r = requests.put(url, data=payload, timeout=self.__worker_timeout)
                tmp_log.debug('worker creation for {0} ended with {1}'.format(apfmon_workers, r.status_code))

            end_time = time.time()
            tmp_log.debug('done (took {0})'.format(end_time - start_time))
        except:
            tmp_log.error('Excepted with: {0}'.format(traceback.format_exc()))

    def convert_status(self, harvester_status):
        """
        convert harvester status to APFMon status
        :param harvester_status
        :return: list with apfmon_status. Usually it's just one status, except for exiting&done
        """
        if harvester_status == 'submitted':
            return ['created']
        if harvester_status in ['running', 'idle']:
            return ['running']
        if harvester_status in ['missed', 'failed', 'cancelled']:
            return ['fault']
        if harvester_status == 'finished':
            return ['exiting', 'done']

    def update_workers(self, worker_spec_list):
        """
        Updates the state of a worker. This can also be done directly from the wrapper, assuming there is outbound
        connectivity on the worker node
        """
        start_time = time.time()
        tmp_log = core_utils.make_logger(_base_logger, 'harvester_id={0}'.format(self.harvester_id),
                                         method_name='update_workers')

        if not self.__active:
            tmp_log.debug('APFMon reporting not enabled')
            return

        try:
            tmp_log.debug('start')

            for worker_spec in worker_spec_list:

                batch_id = worker_spec.batchID
                factory = self.harvester_id

                url = '{0}/jobs/{1}:{2}'.format(self.base_url, factory, batch_id)

                apfmon_status = self.convert_status(worker_spec.status)
                apfmon_worker = {}

                for status in apfmon_status:
                    apfmon_worker['state'] = status

                    if status == 'exiting':
                        # return code
                        apfmon_worker['rc'] = 0 # TODO: I'm not sure how to fill this field
                        if worker_spec.has_attribute('pandaid_list'):
                            apfmon_worker['ids'] = ','.join(str(x) for x in worker_spec.pandaid_list)

                    tmp_log.debug('updating worker {0}: {1}'.format(batch_id, apfmon_worker))
                    payload = json.dumps(apfmon_worker)

                    r = requests.post(url, data=payload, timeout=self.__worker_timeout)
                    tmp_log.debug('worker update for {0} ended with {1}'.format(batch_id, r.status_code))

            end_time = time.time()
            tmp_log.debug('done (took {0})'.format(end_time - start_time))
        except:
            tmp_log.error('Excepted with: {0}'.format(traceback.format_exc()))


if __name__== "__main__":

    """
    Quick tests
    """
    from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
    queue_config_mapper = QueueConfigMapper()

    apfmon = Apfmon(queue_config_mapper)
    apfmon.create_factory()
    apfmon.create_labels()

    worker_a = WorkSpec()
    worker_a.batchID = 1
    worker_a.computingSite = 'CERN-PROD-DEV_UCORE'
    worker_a.workAttributes = {"batchLog": "https://aipanda024.cern.ch/condor_logs/18-07-19_09/grid.9659.0.log", "stdErr": "https://aipanda024.cern.ch/condor_logs/18-07-19_09/grid.9659.0.err", "stdOut": "https://aipanda024.cern.ch/condor_logs/18-07-19_09/grid.9659.0.out"}
    worker_a.pandaid_list = [1234, 5678]

    worker_b = WorkSpec()
    worker_b.batchID = 2
    worker_b.computingSite = 'CERN-PROD-DEV_UCORE'
    worker_b.workAttributes = {"batchLog": "https://aipanda024.cern.ch/condor_logs/18-07-19_09/grid.9659.0.log", "stdErr": "https://aipanda024.cern.ch/condor_logs/18-07-19_09/grid.9659.0.err", "stdOut": "https://aipanda024.cern.ch/condor_logs/18-07-19_09/grid.9659.0.out"}


    workers = [worker_a, worker_b]

    apfmon.create_workers(workers)
    worker_a.status = 'running'
    worker_b.status = 'running'
    apfmon.update_workers(workers)
    worker_a.status = 'finished'
    worker_b.status = 'failed'
    apfmon.update_workers(workers)

