import random

from math import log1p

#########################
# Pilot related functions
#########################

# Map "pilotType" (defined in harvester) to prodSourceLabel and pilotType option (defined in pilot, -i option)
# and piloturl (pilot option --piloturl) for pilot 2


def get_complicated_pilot_options(pilot_type, pilot_url=None, pilot_version="", prod_source_label=None, prod_rc_permille=0):
    # for pilot 3
    is_pilot3 = True if pilot_version.startswith("3") else False
    # basic map
    pt_psl_map = {
        "RC": {
            "prod_source_label": "rc_test2",
            "pilot_type_opt": "RC",
            "pilot_url_str": "--piloturl http://cern.ch/atlas-panda-pilot/pilot3-dev.tar.gz"
            if is_pilot3
            else "--piloturl http://cern.ch/atlas-panda-pilot/pilot2-dev.tar.gz",
            "pilot_debug_str": "-d",
        },
        "ALRB": {
            "prod_source_label": "rc_alrb",
            "pilot_type_opt": "ALRB",
            "pilot_url_str": "",
            "pilot_debug_str": "",
        },
        "PT": {
            "prod_source_label": "ptest",
            "pilot_type_opt": "PR",
            "pilot_url_str": "--piloturl http://cern.ch/atlas-panda-pilot/pilot3-dev2.tar.gz"
            if is_pilot3
            else "--piloturl http://cern.ch/atlas-panda-pilot/pilot2-dev2.tar.gz",
            "pilot_debug_str": "-d",
        },
        "PR": {
            "prod_source_label": prod_source_label,
            "pilot_type_opt": "PR",
            "pilot_url_str": "",
            "pilot_debug_str": "",
        },
    }
    # get pilot option dict
    pilot_opt_dict = pt_psl_map.get(pilot_type, pt_psl_map["PR"])
    if pilot_url:
        # overwrite with specified pilot_url
        pilot_opt_dict["pilot_url_str"] = "--piloturl {0}".format(pilot_url)
    elif pilot_type == "PR":
        # randomization of pilot url for PR (managed, user) pilot run some portion of RC version (not RC dev) pilot
        prod_rc_pilot_url_str = "--piloturl http://pandaserver.cern.ch:25085/cache/pilot/pilot3-rc.tar.gz"
        prod_rc_prob = min(max(prod_rc_permille / 1000.0, 0), 1)
        lucky_number = random.random()
        if lucky_number < prod_rc_prob:
            pilot_opt_dict["pilot_url_str"] = prod_rc_pilot_url_str
    # return pilot option dict
    return pilot_opt_dict


# get special flag of pilot wrapper about python version of pilot, and whether to run with python 3 if python version is "3"


def get_python_version_option(python_version, prod_source_label):
    option = ""
    if python_version.startswith("3"):
        option = "--pythonversion 3"
    return option


# get pilot joblabel (-j) option, support unified dispatch


def get_joblabel(prod_source_label, is_unified_dispatch=False):
    joblabel = prod_source_label
    if is_unified_dispatch and prod_source_label in ["managed", "user"]:
        joblabel = "unified"
    return joblabel


# get pilot job type (--job-type) option, support unified dispatch


def get_pilot_job_type(job_type, is_unified_dispatch=False):
    pilot_job_type = job_type
    if is_unified_dispatch:
        pilot_job_type = "unified"
    return pilot_job_type


# Parse resource type from string for Unified PanDA Queue


def get_resource_type(string, is_unified_queue, is_pilot_option=False):
    string = str(string)
    if not is_unified_queue:
        ret = ""
    elif string in set(["SCORE", "MCORE", "SCORE_HIMEM", "MCORE_HIMEM"]):
        if is_pilot_option:
            ret = "--resource-type {0}".format(string)
        else:
            ret = string
    else:
        ret = ""
    return ret


#############################
# CE stats related functions
#############################


# Compute weight of each CE according to worker stat, return tuple(dict, total weight score)
def get_ce_weighting(ce_endpoint_list=[], worker_ce_all_tuple=None, is_slave_queue=False):
    multiplier = 1000.0
    n_ce = len(ce_endpoint_list)
    worker_limits_dict, worker_ce_stats_dict, worker_ce_backend_throughput_dict, time_window, n_new_workers = worker_ce_all_tuple
    N = float(n_ce)
    Q = float(worker_limits_dict["nQueueLimitWorker"])
    W = float(worker_limits_dict["maxWorkers"])
    Q_good_init = float(
        sum(worker_ce_backend_throughput_dict[_ce][_st] for _st in ("submitted", "running", "finished") for _ce in worker_ce_backend_throughput_dict)
    )
    Q_good_fin = float(sum(worker_ce_backend_throughput_dict[_ce][_st] for _st in ("submitted",) for _ce in worker_ce_backend_throughput_dict))
    thruput_avg = log1p(Q_good_init) - log1p(Q_good_fin)
    n_new_workers = float(n_new_workers)
    # target number of queuing
    target_Q = Q + n_new_workers
    if is_slave_queue:
        # take total number of current queuing if slave queue
        total_Q = sum((float(worker_ce_stats_dict[_k]["submitted"]) for _k in worker_ce_stats_dict))
        target_Q = min(total_Q, Q) + n_new_workers

    def _get_thruput(_ce_endpoint):  # inner function
        if _ce_endpoint not in worker_ce_backend_throughput_dict:
            q_good_init = 0.0
            q_good_fin = 0.0
        else:
            q_good_init = float(sum(worker_ce_backend_throughput_dict[_ce_endpoint][_st] for _st in ("submitted", "running", "finished")))
            q_good_fin = float(sum(worker_ce_backend_throughput_dict[_ce_endpoint][_st] for _st in ("submitted",)))
        thruput = log1p(q_good_init) - log1p(q_good_fin)
        return thruput

    def _get_thruput_adj_ratio(thruput):  # inner function
        try:
            thruput_adj_ratio = thruput / thruput_avg + 1 / N
        except ZeroDivisionError:
            if thruput == 0.0:
                thruput_adj_ratio = 1 / N
            else:
                raise
        return thruput_adj_ratio

    ce_base_weight_sum = sum((_get_thruput_adj_ratio(_get_thruput(_ce)) for _ce in ce_endpoint_list))

    def _get_init_weight(_ce_endpoint):  # inner function
        if _ce_endpoint not in worker_ce_stats_dict:
            q = 0.0
            r = 0.0
        else:
            q = float(worker_ce_stats_dict[_ce_endpoint]["submitted"])
            r = float(worker_ce_stats_dict[_ce_endpoint]["running"])
            # q_avg = sum(( float(worker_ce_stats_dict[_k]['submitted']) for _k in worker_ce_stats_dict )) / N
            # r_avg = sum(( float(worker_ce_stats_dict[_k]['running']) for _k in worker_ce_stats_dict )) / N
        if _ce_endpoint in worker_ce_stats_dict and q > Q:
            return float(0)
        ce_base_weight_normalized = _get_thruput_adj_ratio(_get_thruput(_ce_endpoint)) / ce_base_weight_sum
        # target number of queuing of the CE
        q_expected = target_Q * ce_base_weight_normalized
        # weight by difference
        ret = max((q_expected - q), 2**-10)
        # # Weight by running ratio
        # _weight_r = 1 + N*r/R
        if r == 0:
            # Penalty for dead CE (no running worker)
            ret = ret / (1 + log1p(q) ** 2)
        return ret

    init_weight_iterator = map(_get_init_weight, ce_endpoint_list)
    sum_of_weights = sum(init_weight_iterator)
    total_score = multiplier * N
    try:
        regulator = total_score / sum_of_weights
    except ZeroDivisionError:
        regulator = 1.0
    ce_weight_dict = {_ce: _get_init_weight(_ce) * regulator for _ce in ce_endpoint_list}
    ce_thruput_dict = {_ce: _get_thruput(_ce) * 86400.0 / time_window for _ce in ce_endpoint_list}
    return total_score, ce_weight_dict, ce_thruput_dict, target_Q


# Choose a CE accroding to weighting
def choose_ce(weighting):
    total_score, ce_weight_dict, ce_thruput_dict, target_Q = weighting
    lucky_number = random.random() * total_score
    cur = 0.0
    ce_now = None
    for _ce, _w in ce_weight_dict.items():
        if _w == 0.0:
            continue
        ce_now = _ce
        cur += _w
        if cur >= lucky_number:
            return _ce
    if ce_weight_dict.get(ce_now, -1) > 0.0:
        return ce_now
    else:
        return None


# Get better string to display the statistics and weightng of CEs


def get_ce_stats_weighting_display(ce_list, worker_ce_all_tuple, ce_weighting):
    worker_limits_dict, worker_ce_stats_dict, worker_ce_backend_throughput_dict, time_window, n_new_workers = worker_ce_all_tuple
    total_score, ce_weight_dict, ce_thruput_dict, target_Q = ce_weighting
    worker_ce_stats_dict_sub_default = {"submitted": 0, "running": 0}
    worker_ce_backend_throughput_dict_sub_default = {"submitted": 0, "running": 0, "finished": 0}
    general_dict = {
        "maxWorkers": int(worker_limits_dict.get("maxWorkers")),
        "nQueueLimitWorker": int(worker_limits_dict.get("nQueueLimitWorker")),
        "nNewWorkers": int(n_new_workers),
        "target_Q": int(target_Q),
        "history_time_window": int(time_window),
    }
    general_str = (
        "maxWorkers={maxWorkers} "
        "nQueueLimitWorker={nQueueLimitWorker} "
        "nNewWorkers={nNewWorkers} "
        "target_Q={target_Q} "
        "hist_timeWindow={history_time_window} "
    ).format(**general_dict)
    ce_str_list = []
    for _ce in ce_list:
        schema_sub_dict = {
            "submitted_now": int(worker_ce_stats_dict.get(_ce, worker_ce_stats_dict_sub_default).get("submitted")),
            "running_now": int(worker_ce_stats_dict.get(_ce, worker_ce_stats_dict_sub_default).get("running")),
            "submitted_history": int(worker_ce_backend_throughput_dict.get(_ce, worker_ce_backend_throughput_dict_sub_default).get("submitted")),
            "running_history": int(worker_ce_backend_throughput_dict.get(_ce, worker_ce_backend_throughput_dict_sub_default).get("running")),
            "finished_history": int(worker_ce_backend_throughput_dict.get(_ce, worker_ce_backend_throughput_dict_sub_default).get("finished")),
            "thruput_score": ce_thruput_dict.get(_ce),
            "weight_score": ce_weight_dict.get(_ce),
        }
        ce_str = (
            '"{_ce}": '
            "now_S={submitted_now} "
            "now_R={running_now} "
            "hist_S={submitted_history} "
            "hist_R={running_history} "
            "hist_F={finished_history} "
            "T={thruput_score:.02f} "
            "W={weight_score:.03f} "
        ).format(_ce=_ce, **schema_sub_dict)
        ce_str_list.append(ce_str)
    stats_weighting_display_str = general_str + " ; " + " , ".join(ce_str_list)
    return stats_weighting_display_str
