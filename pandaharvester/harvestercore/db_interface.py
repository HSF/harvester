"""
interface to give limited database access to plugins
"""

import os
import logging
import threading

from .db_proxy_pool import DBProxyPool
from pandaharvester.harvesterconfig import harvester_config


class DBInterface(object):
    # constructor
    def __init__(self):
        self.dbProxy = DBProxyPool()

    # get cache data
    def get_cache(self, data_name):
        return self.dbProxy.get_cache(data_name)

    # get files with a group ID
    def get_files_with_group_id(self, group_id):
        return self.dbProxy.get_files_with_group_id(group_id)

    # update group status
    def update_file_group_status(self, group_id, status_string):
        return self.dbProxy.update_file_group_status(group_id, status_string)

    # get group status
    def get_file_group_status(self, group_id):
        return self.dbProxy.get_file_group_status(group_id)

    # get locker identifier
    def get_locked_by(self):
        currentThr = threading.current_thread()
        if currentThr is not None:
            thrName = currentThr.ident
        else:
            thrName = None
        return "plugin-{0}-{1}".format(os.getpid(), thrName)

    # get a lock for an object
    def get_object_lock(self, object_name, lock_interval):
        lockedBy = self.get_locked_by()
        return self.dbProxy.get_process_lock(object_name, lockedBy, lock_interval)

    # release a process lock
    def release_object_lock(self, object_name):
        lockedBy = self.get_locked_by()
        return self.dbProxy.release_process_lock(object_name, lockedBy)

    # refresh file group info
    def refresh_file_group_info(self, job_spec):
        return self.dbProxy.refresh_file_group_info(job_spec)

    # set file group
    def set_file_group(self, file_specs, group_id, status_string):
        return self.dbProxy.set_file_group(file_specs, group_id, status_string)

    # get queue status
    def get_worker_limits(self, site_name):
        return self.dbProxy.get_worker_limits(site_name)

    # get worker CE stats
    def get_worker_ce_stats(self, site_name):
        return self.dbProxy.get_worker_ce_stats(site_name)

    # get worker CE backend throughput
    def get_worker_ce_backend_throughput(self, site_name, time_window):
        return self.dbProxy.get_worker_ce_backend_throughput(site_name, time_window)

    # add dialog message
    def add_dialog_message(self, message, level, module_name, identifier=None):
        # set level
        validLevels = ["DEBUG", "INFO", "ERROR", "WARNING"]
        if level not in validLevels:
            level = "INFO"
        levelNum = getattr(logging, level)
        # get minimum level
        try:
            minLevel = harvester_config.propagator.minMessageLevel
        except Exception:
            minLevel = None
        if minLevel not in validLevels:
            minLevel = "WARNING"
        minLevelNum = getattr(logging, minLevel)
        # check level to avoid redundant db lock
        if levelNum < minLevelNum:
            return True
        return self.dbProxy.add_dialog_message(message, level, module_name, identifier)
