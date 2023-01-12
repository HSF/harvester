from lancium.api.Job import Job
import uuid
from constants import voms_lancium_path, voms_job_path

"""
https://lancium.github.io/compute-api-docs/library/lancium/api/Job.html#Job.create
- name (string): job name
- notes (string): job description
- account (string): string for internal account billing
- qos (string): quality of service
- command_line (string): command line argument
- image (string): base image for container to run job on
- resources (dict): dictionary containing the fields
    - core_count (int)
    - gpu (string)
    - vram (int)
    - gpu_count (int)
    - memory (int)
    - scratch (int)
- max_run_time (int): max run time for job (in seconds)
    - Limit: 30 days
- expected_run_time (int): expected run time of job (in seconds)
- input_files (list of JobInput): input files for Job wrapped in a Job_Input object.
- output_files (tuple): expected output file(s) from job
    - Format: (‘output_file1.txt{:name_to_save_as_in_storage.txt}, …)
    - The destination in persistent storage is optional.
- callback_url (string): Webhook URL to receive updates when job status changes
- environment (tuple of strings): tuple of environment variables to set for job
    - Format: (‘var1=def1’, ‘var2=def2’,...)
- kwargs(dictionary): can contain auth key if you would like to perform this method using a different account. {'auth': ANOTHER_API_KEY}
"""
worker_id = uuid.uuid1()
core_count = 1
memory = 2
scratch = 20

params = {'name': 'grid-job-{0}'.format(worker_id),
          'command_line': 'ls',
          'image': 'lancium/ubuntu',
          'resources': {'core_count': core_count,
                        'memory': memory,
                        'scratch': scratch
                        },
          'input_files': [
              {"source_type": "data",
               "data": voms_lancium_path,
               "name": voms_job_path
               }
          ],
          # 'output_files': RETRIEVE THE PILOT LOG AND STORE IT IN HARVESTER?
          'environment': ('key1:value1', 'key2:value2', 'voms_path:{0}'.format(voms_job_path))
          }

# create the job
job = Job().create(**params)
print('Created! name: {0}, id: {1}, status: {2}'.format(job.name, job.id, job.status))

# submit the job
job.submit()
print('Submitted! name: {0}, id: {1}, status: {2}'.format(job.name, job.id, job.status))