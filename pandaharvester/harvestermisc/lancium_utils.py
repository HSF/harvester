"""
Lancium python API wrapper functions

"""
import traceback

from pandaharvester.harvesterconfig import harvester_config
import os

try:
    api_key = harvester_config.lancium.api_key
except AttributeError:
    raise RuntimeError('The configuration is missing the [lancium] section and/or the api_key entry')

# The key needs to be set before importing the lancium API
os.environ['LANCIUM_API_KEY'] = api_key

from lancium.api.Job import Job
from lancium.api.Data import Data

base_logger = core_utils.setup_logger('lancium_utils')

SECRETS_PATH = '/secrets/'
SCRIPTS_PATH = '/scripts/'

class LanciumClient(object):

    def __init__(self, queue_name=None):
        self.queue_name = queue_name

    def upload_file(self, local_path, lancium_path, force=True):
        try:
            tmp_log = core_utils.make_logger(base_logger, 'queue_name={0}'.format(self.queue_name),
                                             method_name='upload_file')

            data = Data().create(lancium_path, 'file', source=os.path.abspath(local_path), force=force)
            data.upload(os.path.abspath(local_path), fake_callback)
            ex = data.show(lancium_path)[0]
            tmp_log.debug("Uploaded file {0}".format(ex.__dict__))

            return True, ''
        except Exception as _e:
            error_message = 'Failed to upload file with {0}'.format(_e)
            tmp_log.error('Failed to upload the file with {0}'.format(traceback.format_exc()))
            return False, error_message

    def submit_job(self, **jobparams):
        # create and submit a job to lancium
        tmp_log = core_utils.make_logger(base_logger, 'queue_name={0}'.format(self.queue_name),
                                         method_name='submit_job')

        try:
            tmp_log.debug('Creating and submitting a job')

            job = Job().create(**jobparams)
            tmp_log.debug('Job created. name: {0}, id: {1}, status: {2}'.format(job.name, job.id, job.status))
    
            job.submit()
            tmp_log.debug('Job submitted. name: {0}, id: {1}, status: {2}'.format(job.name, job.id, job.status))
            return True, ''
        except Exception as _e:
            error_message = 'Failed to create or submit a job with {0}'.format(_e)
            tmp_log.error('Failed to create or submit a job with {0}'.format(traceback.format_exc()))
            return False, error_message


if __name__ == "__main__":
    lancium_client = LanciumClient()
    lancium_client.test()


