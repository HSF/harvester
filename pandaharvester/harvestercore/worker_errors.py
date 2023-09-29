# Error codes of workers which are mapped to job.supErrorCode.
# Integer values for errors must be not less than 1000


class WorkerErrors(object):
    error_codes = {
        "SUCCEEDED": 0,
        "UNKNOWN": 1000,
        "PREEMPTED": 1001,
        "GENERAL_ERROR": 9000,
    }
