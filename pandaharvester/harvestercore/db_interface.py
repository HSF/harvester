"""
interface to give limited database access to plugins
"""

import os
import threading

from .db_proxy_pool import DBProxyPool


class DBInterface:
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

    # get locker identifier
    def get_locked_by(self):
        currentThr = threading.current_thread()
        if currentThr is not None:
            thrName = currentThr.ident
        else:
            thrName = None
        return 'plugin-{0}-{1}'.format(os.getpid(), thrName)

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
