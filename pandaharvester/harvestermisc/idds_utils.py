import os
import requests

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess


# get HP point
def get_hp_point(idds_url, task_id, point_id, tmp_log, verbose):
    url = os.path.join(idds_url, "idds", "hpo", str(task_id), "null", str(point_id), "null", "null")
    try:
        if verbose:
            tmp_log.debug("getting HP point from {0}".format(url))
        r = requests.get(url, verify=False)
        if verbose:
            tmp_log.debug("status: {0}, body: {1}".format(r.status_code, r.text))
        if r.status_code != requests.codes.ok:
            False, "bad http status {0} when getting point (ID={1}) : {2}".format(r.status_code, point_id, r.text)
        tmp_dict = r.json()
        for i in tmp_dict:
            if i["id"] == point_id:
                return True, i
    except Exception as e:
        errStr = "failed to get point (ID={0}) : {1}".format(point_id, str(e))
        return False, errStr
    return False, "cannot get point (ID={0}) since it is unavailable".format(point_id)


# update HP point
def update_hp_point(idds_url, task_id, point_id, loss, tmp_log, verbose):
    url = os.path.join(idds_url, "idds", "hpo", str(task_id), "null", str(point_id), str(loss))
    try:
        if verbose:
            tmp_log.debug("updating HP point at {0}".format(url))
        r = requests.put(url, verify=False)
        if verbose:
            tmp_log.debug("status: {0}, body: {1}".format(r.status_code, r.text))
        if r.status_code != requests.codes.ok:
            False, "bad http status {0} when updating point (ID={1}) : {2}".format(r.status_code, point_id, r.text)
        tmp_dict = r.json()
        if tmp_dict["status"] == 0:
            return True, None
    except Exception as e:
        errStr = "failed to update point (ID={0}) : {1}".format(point_id, str(e))
        return False, errStr
    return False, "cannot update point (ID={0}) since status is missing".format(point_id)
