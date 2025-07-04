# Error codes of workers which are mapped to job.supErrorCode.
# Integer values for errors must be not less than 1000

import re


class ErrorMessagePatternHandler(object):
    """
    Class to handle error message patterns and their corresponding error codes.
    This class is used to map error messages to specific error codes for supplemental worker errors.
    """

    def __init__(self, code_pattern_map: dict):
        """
        Initialize the ErrorMessagePatternHandler with a mapping of error codes to regex patterns.

        Args:
            code_pattern_map (dict): A dictionary mapping error codes to regex patterns.
        """
        self._code_pattern_map = code_pattern_map.copy()
        self._pattern_code_map = {v: k for k, v in self._code_pattern_map.items()}

    def get_error_code(self, message: str) -> int | None:
        """
        Get the error code for a given message based on the defined patterns.

        Args:
            message (str): The error message to check against the patterns.

        Returns:
            int: The error code if a pattern matches, otherwise None.
        """
        for pattern, code in self._pattern_code_map.items():
            if re.search(pattern, message):
                return code
        return None


class WorkerErrors(object):
    error_codes = {
        "SUCCEEDED": 0,
        "UNKNOWN": 1000,
        "PREEMPTED": 1001,
        "GENERAL_ERROR": 9000,
    }

    error_message_pattern_map = {
        # 2. Pilot wrapper or pilot errors
        2001: r"wrapper fault",
        2002: r"wrapper killed stuck pilot",
        2064: r"wrapper got cvmfs repos issue",
        2080: r"pilot proxy invalid or with insufficient timeleft",
        # 3. Condor schedd errors
        # 3.1. Gridmanager errors
        3131: r"Failed to start GAHP",
        3132: r"Failed to provide GAHP with token",
        3190: r"Unspecified gridmanager error",
        # 3.2. To schedd errors
        3200: r"Error connecting to schedd .*",
        # 3.6. Condor transfer errors
        3601: r"Transfer output files failure at execution point .* while sending files to access point .*",
        # 3.7 misc Condor errors
        3701: r"curl_easy_perform.* failed .*",
        # 3.8 Harvester reserved condor errors
        3811: r"condor job .* not found",
        3812: r"cannot get JobStatus of job .*",
        3831: r"Payload execution error: returned non-zero .*",
        3841: r"cannot get ExitCode of job .*",
        3842: r"got invalid ExitCode .* of job .*",
        # 3.9. Condor RemoveReason on Schedds (when HoldReason is insignificant)
        3901: r"removed by SYSTEM_PERIODIC_REMOVE due to job restarted undesirably",
        3902: r"removed by SYSTEM_PERIODIC_REMOVE due to job held time exceeded .*",
        3903: r"removed by SYSTEM_PERIODIC_REMOVE due to job status unchanged time exceeded .*",
        3904: r"removed by SYSTEM_PERIODIC_REMOVE due to job staying in queue time exceeded .*",
        3905: r"removed by SYSTEM_PERIODIC_REMOVE due to job remote status outdated time exceeded .*",
        3990: r"removed by SYSTEM_PERIODIC_REMOVE due to .*",
        3991: r"Remove Reason unknown",
        # 4. CE errors in HoldReason
        # 4.1. HTCondor CE general errors
        4101: r"Job disappeared from remote schedd",
        4102: r"Error locating schedd .*",
        # 4.3. ARC CE general errors
        # 4.3.1 ARC command timeout errors
        4311: r"ARC_JOB_NEW timed out",
        4312: r"ARC_JOB_KILL timed out",
        4313: r"ARC_JOB_CLEAN timed out",
        4314: r"ARC_DELEGATION_NEW timed out",
        4315: r"ARC_JOB_STAGE_IN timed out",
        4316: r"ARC_JOB_STAGE_OUT timed out",
        4319: r"ARC_.* timed out",
        # 4.3.3 stage-in errors
        4331: r"File stage-in failed: Transfer of .* failed: Forbidden",
        4332: r"File stage-in failed: Transfer of .* failed: Not Found",
        4336: r"File stage-in failed: Transfer of .* failed: HTTP response .*",
        4339: r"File stage-in failed: .*",
        # 4.3.4 stage-out errors
        4341: r"File stage-out failed: Transfer of .* failed: Forbidden",
        4342: r"File stage-out failed: Transfer of .* failed: Not Found",
        4346: r"File stage-out failed: Transfer of .* failed: HTTP response .*",
        4349: r"File stage-out failed: .*",
        # 4.3.6+ ARC misc errors
        4301: r"ARC job failed: Job submission to .* failed",
        4302: r"ARC job failed: Job is canceled by external request",
        4350: r"ARC job has no credentials",
        4360: r"New job submission is not allowed",
        4361: r"Forbidden",
        4362: r"Job could not be cleaned",
        4380: r"ARC job failed for unknown reason",
        # 4.4. ARC CE LRMS errors
        4401: r"ARC job failed: LRMS error: .* Node fail",
        4411: r"ARC job failed: LRMS error: .* job killed: vmem",
        4414: r"ARC job failed: LRMS error: .* job killed: cput",
        4415: r"ARC job failed: LRMS error: .* job killed: wall",
        4419: r"ARC job failed: LRMS error: .* job killed: .*",
        4420: r"ARC job failed: LRMS error: .* Job missing from .*",
        4440: r"ARC job failed: LRMS error: .* Job failed",
        4441: r"ARC job failed: LRMS error: .* Job failed with unknown exit code",
        4442: r"ARC job failed: LRMS error: .* Job failed but .* reported .*",
        4445: r"ARC job failed: LRMS error: .* Job was lost with unknown exit code",
        4447: r"ARC job failed: LRMS error: .* Job was killed by .*",
        4448: r"ARC job failed: LRMS error: .* Job was .*",
        4449: r"ARC job failed: LRMS error: .* Job .*",
        4460: r"ARC job failed: LRMS error: .* PeriodicRemove evaluated to TRUE",
        4461: r"ARC job failed: LRMS error: .* RemoveReason: .*",
        4470: r"ARC job failed: LRMS error: .* Node fail",
        4490: r"ARC job failed: LRMS error: .*",
        # 5. Batch system and Kubernetes errors
        # 5.1. Batch system errors
        # 5.1.1 Memory errors
        5110: r"Job has gone over cgroup memory limit of .*",
        5116: r"peak memory usage exceeded .*",
        # 5.5 HTCondor batch errors
        5511: r"Number of submitted jobs would exceed MAX_JOBS_SUBMITTED",
        5512: r"Number of submitted jobs would exceed MAX_JOBS_PER_OWNER",
        5519: r"Number of submitted jobs would exceed .*",
        5520: r"The job exceeded allowed .* duration of .*",
        # 5.5.7 Site customized HoldReason
        5571: r".* Second start not allowed",  # INFN-CNAF
        # 5.8 Kubernetes errors
        5801: r"Job has reached the specified backoff limit",
        # 9. Catch all errors
        9000: r".*",
    }

    # instance of handler
    message_pattern_handler = ErrorMessagePatternHandler(error_message_pattern_map)
