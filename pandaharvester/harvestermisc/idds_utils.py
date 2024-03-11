import os
import subprocess

import requests


# get HP point
def get_hp_point(idds_url, task_id, point_id, tmp_log, verbose):
    url = os.path.join(idds_url, "idds", "hpo", str(task_id), "null", str(point_id), "null", "null")
    try:
        if verbose:
            tmp_log.debug(f"getting HP point from {url}")
        r = requests.get(url, verify=False)
        if verbose:
            tmp_log.debug(f"status: {r.status_code}, body: {r.text}")
        if r.status_code != requests.codes.ok:
            False, f"bad http status {r.status_code} when getting point (ID={point_id}) : {r.text}"
        tmp_dict = r.json()
        for i in tmp_dict:
            if i["id"] == point_id:
                return True, i
    except Exception as e:
        errStr = f"failed to get point (ID={point_id}) : {str(e)}"
        return False, errStr
    return False, f"cannot get point (ID={point_id}) since it is unavailable"


# update HP point
def update_hp_point(idds_url, task_id, point_id, loss, tmp_log, verbose):
    url = os.path.join(idds_url, "idds", "hpo", str(task_id), "null", str(point_id), str(loss))
    try:
        if verbose:
            tmp_log.debug(f"updating HP point at {url}")
        r = requests.put(url, verify=False)
        if verbose:
            tmp_log.debug(f"status: {r.status_code}, body: {r.text}")
        if r.status_code != requests.codes.ok:
            False, f"bad http status {r.status_code} when updating point (ID={point_id}) : {r.text}"
        tmp_dict = r.json()
        if tmp_dict["status"] == 0:
            return True, None
    except Exception as e:
        errStr = f"failed to update point (ID={point_id}) : {str(e)}"
        return False, errStr
    return False, f"cannot update point (ID={point_id}) since status is missing"
