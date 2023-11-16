import sys

from lancium.api.Job import Job

if len(sys.argv) == 2:
    job_id = sys.argv[1]
else:
    # print all job statuses
    job_id = 0

if job_id == 0:
    all_jobs = Job().all()
    for job in all_jobs:
        print(f"id: {job.id}, status: {job.status}")
else:
    job = Job().get(job_id)
    print(f"id: {job.id}, status: {job.status}")
