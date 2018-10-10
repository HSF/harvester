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

class Apfmon:

    def __init__(self, queue_config_mapper):

        self._base_logger = core_utils.setup_logger('apfmon')

        try:
            self.__active = harvester_config.apfmon.active
        except:
            self.__active = True # TODO: decide if default is active or not

        # TODO: make proper exception handling and defaults
        try:
            self.harvester_id = harvester_config.master.harvester_id
        except:
            self.harvester_id = 'DUMMY'

        try:
            self.base_url = harvester_config.apfmon.base_url
        except:
            self.base_url = 'http://apfmon.lancs.ac.uk/api'

        self.queue_config_mapper = queue_config_mapper()

    def __apfmon_active(cls, method, *args, **kwargs):
        if cls.__active:
            method(*args, **kwargs)
        else:
            return

    def create_factory(self):
        """
        Creates or updates a harvester instance to APF Mon. Should be done at startup of the instance.
        """
        start_time = time.time()
        tmp_log = core_utils.make_logger(self._base_logger, 'harvester_id={0}'.format(self.harvester_id),
                                         method_name='create_factory')
        try:
            tmp_log.debug('start')

            url = '{0}/factories/{1}'.format(self.base_url, self.harvester_id)

            f = {'url': 'url_to_logs', # TODO: get the URL properly. This can be problematic when harvester runs on two nodes and one DB
                 'email': 'atlas-adc-harvester-central-support@cern.ch',
                 'version': panda_pkg_info.release_version}
            payload = json.dumps(f)

            r = requests.put(url, data=payload)
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
        tmp_log = core_utils.make_logger(self._base_logger, 'harvester_id={0}'.format(self.harvester_id),
                                         method_name='create_labels')
        try:
            tmp_log.debug('start')

            url = '{0}/labels'.format(self.base_url)

            # get the active queues from the config mapper
            all_sites = self.queue_config_mapper.get_active_queues().keys()

            # publish the active queues to APF mon in shards
            for sites in generic_utils.create_shards(all_sites, 20):
                labels = []
                for site in sites:
                    labels.append({'name': site, 'factory': self.harvester_id})

                payload = json.dumps(labels)

                r = requests.put(url, data=payload)
                tmp_log.debug('label creation for {0} ended with {1}'.format(sites, r.status_code))

            end_time = time.time()
            tmp_log.debug('done (took {0})'.format(end_time - start_time))
        except:
            tmp_log.error('Excepted with: {0}'.format(traceback.format_exc()))

    def create_workers(self, worker_spec_list):
        """
        Creates a worker. The updates (state transitions) are expected to come from the pilot wrappers
        """
        start_time = time.time()
        tmp_log = core_utils.make_logger(self._base_logger, 'harvester_id={0}'.format(self.harvester_id),
                                         method_name='create_workers')
        try:
            tmp_log.debug('start')

            url = '{0}/jobs'.format(self.base_url)

            for worker_spec_shard in generic_utils.create_shards(worker_spec_list, 20):
                apfmon_workers = []
                for worker_spec in worker_spec_shard:
                    # TODO: be sure it's expecting the condor ID and it's consistent with the wrapper info
                    batch_id = worker_spec.batchID
                    factory = self.harvester_id
                    computingsite = worker_spec.computingSite

                    # extract the log URLs
                    work_attribs = json.loads(worker_spec.workAttributes)
                    stdout_url = work_attribs['stdOut']
                    stderr_url = work_attribs['stdErr']
                    log_url = work_attribs['batchLog']

                    jdl_url = '' # TODO: publish the jdl files

                    apfmon_worker = {'cid': batch_id,
                                     'factory': factory,
                                     'label': computingsite,
                                     'jdlurl': jdl_url,
                                     'stdouturl': stdout_url,
                                     'stderrurl': stderr_url,
                                     'logurl': log_url
                                     }
                    tmp_log.debug('packed worker: {0}'.format(apfmon_worker))
                    apfmon_workers.append(apfmon_worker)

                payload = json.dumps(apfmon_workers)
                r = requests.put(url, data=payload)
                tmp_log.debug('worker creation for {0} ended with {1}'.format(apfmon_workers, r.status_code))

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
    worker_a.workAttributes = '{"batchLog": "https://aipanda024.cern.ch/condor_logs/18-07-19_09/grid.9659.0.log", "stdErr": "https://aipanda024.cern.ch/condor_logs/18-07-19_09/grid.9659.0.err", "stdOut": "https://aipanda024.cern.ch/condor_logs/18-07-19_09/grid.9659.0.out"}'

    worker_b = WorkSpec()
    worker_b.batchID = 2
    worker_b.computingSite = 'CERN-PROD-DEV_UCORE'
    worker_b.workAttributes = '{"batchLog": "https://aipanda024.cern.ch/condor_logs/18-07-19_09/grid.9659.0.log", "stdErr": "https://aipanda024.cern.ch/condor_logs/18-07-19_09/grid.9659.0.err", "stdOut": "https://aipanda024.cern.ch/condor_logs/18-07-19_09/grid.9659.0.out"}'


    workers = [worker_a, worker_b]

    apfmon.create_workers(workers)


