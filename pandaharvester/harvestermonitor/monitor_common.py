#########################
# Maps of payload exit code to error message
#########################

ec_em_map = {
    "atlas_pilot_wrapper": {
        1: "wrapper fault",
        2: "wrapper killed stuck pilot",
        64: "wrapper got cvmfs repos issue",
        80: "pilot proxy invalid or with insufficient timeleft",
    },
}


def get_payload_errstr_from_ec(payload_type, exit_code):
    tmp_em_map = ec_em_map.get(payload_type, {})
    errstr = tmp_em_map.get(exit_code, "")
    return errstr


#########################
# Errors of monitor plugins
#########################


class PartialReportError(Exception):
    """
    Raised by report_updated_workers when it failed to check some of the resources;
    workers holds the list of (workerID, updateTimestamp) got from the others
    """

    def __init__(self, message, workers):
        super().__init__(message)
        self.workers = workers
