from lancium.api.Job import Job
import uuid
from constants import voms_lancium_path, voms_job_path, script_lancium_path, script_job_path

# https://lancium.github.io/compute-api-docs/library/lancium/api/Job.html#Job.create

worker_id = uuid.uuid1()
core_count = 0.5
memory = 1
scratch = 20

params = {
    "name": "grid-job-{0}".format(worker_id),
    "command_line": "python voms/scripts/pilots_starter.py",
    "image": "harvester/centos7-singularity",
    "resources": {"core_count": core_count, "memory": memory, "scratch": scratch},
    "input_files": [
        {"source_type": "data", "data": voms_lancium_path, "name": voms_job_path},
        {"source_type": "data", "data": script_lancium_path, "name": script_job_path},
    ],
    # 'output_files': RETRIEVE THE PILOT LOG AND STORE IT IN HARVESTER?
    "environment": (
        # pilotUrlOpt, stdout_name
        {"variable": "PILOT_NOKILL", "value": "True"},
        {"variable": "computingSite", "value": "GOOGLE_EUW1"},
        {"variable": "pandaQueueName", "value": "GOOGLE_EUW1"},
        {"variable": "resourceType", "value": "SCORE"},
        {"variable": "prodSourceLabel", "value": "managed"},
        {"variable": "pilotType", "value": "PR"},
        # {'variable': 'pythonOption', 'value': '--pythonversion\ 3'},
        {"variable": "pilotVersion", "value": "3.5.0.31"},
        {"variable": "jobType", "value": "managed"},
        {"variable": "proxySecretPath", "value": "/jobDir/voms/secrets/test1"},
        {"variable": "workerID", "value": "1"},
        {"variable": "pilotProxyCheck", "value": "False"},
        {"variable": "logs_frontend_w", "value": "https://aipanda047.cern.ch:25443/server/panda"},
        {"variable": "logs_frontend_r", "value": "https://aipanda047.cern.ch:25443/cache"},
        {"variable": "PANDA_JSID", "value": "harvester-CERN_central_k8s"},
        {"variable": "HARVESTER_WORKER_ID", "value": "21421931"},
        {"variable": "HARVESTER_ID", "value": "CERN_central_k8s"},
        {"variable": "submit_mode", "value": "PULL"},
        {"variable": "TMPDIR", "value": "/jobDir"},
        {"variable": "HOME", "value": "/jobDir"},
        {"variable": "K8S_JOB_ID", "value": "grid-job-1"},
    ),
}

# create the job
job = Job().create(**params)
print("Created! name: {0}, id: {1}, status: {2}".format(job.name, job.id, job.status))

# submit the job
job.submit()
print("Submitted! name: {0}, id: {1}, status: {2}".format(job.name, job.id, job.status))
