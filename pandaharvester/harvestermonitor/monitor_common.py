#########################
# Maps of payload exit code to error message
#########################

ec_em_map = {
    "atlas_pilot_wrapper": {
        1: "wrapper fault",
        2: "wrapper killed stuck pilot",
    },
}


def get_payload_errstr_from_ec(payload_type, exit_code):
    tmp_em_map = ec_em_map.get(payload_type, {})
    errstr = tmp_em_map.get(exit_code, "")
    return errstr
