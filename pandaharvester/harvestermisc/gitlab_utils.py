import os
import json

job_params_file = "__job_params__"


# get job params from file
def get_job_params(work_spec):
    with open(make_file_path(work_spec)) as f:
        return json.load(f)


# store job params in file
def store_job_params(work_spec, params):
    with open(make_file_path(work_spec), "w") as f:
        json.dump(params, f)


# make file path
def make_file_path(work_spec):
    return os.path.join(work_spec.get_access_point(), job_params_file)
