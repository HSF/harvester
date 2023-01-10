from lancium.api.Job import Job
from lancium.errors.common import *

kwargs = {'name': 'This is a test job', 'image': 'lancium/ubuntu', 'command': 'ls', 'notes': 'this is a note'}
job = Job.create(**kwargs)

job_id = job.id
print(job_id)

#  What is the difference between terminate, delete and destroy a job
#  Does delete also terminate a job?
Job.delete(job_id)
