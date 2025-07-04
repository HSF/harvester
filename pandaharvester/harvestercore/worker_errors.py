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
        3121: r"Failed to start gridmanager",
        3131: r"Failed to start GAHP",
        3132: r"Failed to provide GAHP with token",
        3190: r"Unspecified gridmanager error",
        # 3.2. To schedd errors
        3200: r"Error connecting to schedd .*",
        # 3.6. Condor transfer errors
        3601: r"Transfer output files failure at execution point .* while sending files to access point .*",
        # 3.6.3 stage-in errors
        3631: r"File stage-in failed: Transfer of .* failed: Forbidden",
        3632: r"File stage-in failed: Transfer of .* failed: Not Found",
        3636: r"File stage-in failed: Transfer of .* failed: HTTP response .*",
        3637: r"File stage-in failed: Transfer of .* failed: curl_easy_perform\(\) failed .*",
        3639: r"File stage-in failed: .*",
        # 3.6.4 stage-out errors
        3641: r"File stage-out failed: Transfer of .* failed: Forbidden",
        3642: r"File stage-out failed: Transfer of .* failed: Not Found",
        3646: r"File stage-out failed: Transfer of .* failed: HTTP response .*",
        3647: r"File stage-out failed: Transfer of .* failed: curl_easy_perform\(\) failed .*",
        3649: r"File stage-out failed: .*",
        # 3.6.8 other transfer or http errors
        3681: r"curl_easy_perform.* failed .*",
        # 3.8 Harvester reserved condor errors
        3811: r"condor job .* not found",
        3812: r"cannot get JobStatus of job .*",
        3831: r"Payload execution error: returned non-zero .*",
        3841: r"cannot get ExitCode of job .*",
        3842: r"got invalid ExitCode .* of job .*",
        # 4. CE errors in HoldReason
        # 4.1. HTCondor CE general errors
        4101: r"Job disappeared from remote schedd",
        4102: r"Error locating schedd .*",
        4103: r"Schedd .* didn't send expected files",
        4104: r"Error receiving files from schedd .*",
        # 4.2. ARC CE LRMS errors
        4201: r"LRMS error: .* Node fail",
        4211: r"LRMS error: .* job killed: vmem",
        4214: r"LRMS error: .* job killed: cput",
        4215: r"LRMS error: .* job killed: wall",
        4219: r"LRMS error: .* job killed: .*",
        4220: r"LRMS error: .* Job missing from .*",
        4241: r"LRMS error: .* Job failed with unknown exit code",
        4242: r"LRMS error: .* Job failed but .* reported .*",
        4244: r"LRMS error: .* Job failed .*",
        4245: r"LRMS error: .* Job was lost with unknown exit code",
        4247: r"LRMS error: .* Job was killed by .*",
        4248: r"LRMS error: .* Job was .*",
        4249: r"LRMS error: .* Job .*",
        4260: r"LRMS error: .* PeriodicRemove evaluated to TRUE",
        4261: r"LRMS error: .* RemoveReason: .*",
        4290: r"LRMS error: .*",
        # 4.3. ARC CE general errors
        # 4.3.1 ARC command timeout errors
        4311: r"ARC_JOB_NEW timed out",
        4312: r"ARC_JOB_KILL timed out",
        4313: r"ARC_JOB_CLEAN timed out",
        4314: r"ARC_DELEGATION_NEW timed out",
        4315: r"ARC_JOB_STAGE_IN timed out",
        4316: r"ARC_JOB_STAGE_OUT timed out",
        4319: r"ARC_.* timed out",
        # 4.3.5+ ARC misc errors
        4351: r"ARC job failed: Job submission to .* failed",
        4352: r"ARC job failed: Job is canceled by external request",
        4353: r"ARC job failed: Failed extracting LRMS ID due to some internal error",
        4360: r"New job submission is not allowed",
        4361: r"Forbidden",
        4362: r"Job could not be cleaned",
        4363: r"ARC job has no credentials",
        4364: r"Failed to find valid session directory",
        4390: r"ARC job failed: .*",
        4391: r"ARC job failed for unknown reason",
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
        5572: r"job aborted due to .*",  # FZK-LCG2
        # 5.6 Slurm errors
        5601: r"submission command failed \(exit code = .*\).*",
        5602: r"no jobId in submission script's output .*",
        5699: r"Slurm .*",
        # 5.8 Kubernetes errors
        5801: r"Job has reached the specified backoff limit",
        # 5.8.8 Harvester K8s plugin reserved errors
        5881: r"container not terminated yet (.*) while pod Succeeded",
        5882: r"container terminated by k8s for reason .*",
        # 8. Kill or cancel messages
        # 8.9 Condor RemoveReason
        # 8.9.7 Miscellaneous Condor RemoveReason
        8970: r"Python-initiated action\. \(by user .*\)",
        8971: r"via condor_rm .*",
        # 8.9.8+ Condor RemoveReason reserved on Schedds (when HoldReason is insignificant)
        8981: r"removed by SYSTEM_PERIODIC_REMOVE due to job restarted undesirably",
        8982: r"removed by SYSTEM_PERIODIC_REMOVE due to job held time exceeded .*",
        8983: r"removed by SYSTEM_PERIODIC_REMOVE due to job status unchanged time exceeded .*",
        8984: r"removed by SYSTEM_PERIODIC_REMOVE due to job staying in queue time exceeded .*",
        8985: r"removed by SYSTEM_PERIODIC_REMOVE due to job remote status outdated time exceeded .*",
        8990: r"removed by SYSTEM_PERIODIC_REMOVE due to .*",
        8991: r"Remove Reason unknown",
        # 9. Catch all errors
        9000: r".*",
    }

    # instance of handler
    message_pattern_handler = ErrorMessagePatternHandler(error_message_pattern_map)
