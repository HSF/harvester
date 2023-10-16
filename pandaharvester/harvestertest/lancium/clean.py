import sys
from lancium.api.Job import Job

if len(sys.argv) != 2:
    print("Pass the job id as argument")
    return

job_id = sys.argv[1]

#  What is the difference between terminate, delete and destroy a job
#  Does delete also terminate a job?
Job.delete(job_id)
