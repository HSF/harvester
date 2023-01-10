from lancium.api.Job import Job
from lancium.errors.common import *

job_id = 1234

#  What is the difference between terminate, delete and destroy a job
#  Does delete also terminate a job?
Job.delete(job_id)
