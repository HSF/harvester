from lancium.api.Job import Job

id = 1234
job = Job().get(id)
print("id: {0}, status: {1}".format(job.id, job.status))
