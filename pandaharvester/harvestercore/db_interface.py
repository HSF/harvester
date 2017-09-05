"""
interface to give limited database access to plugins
"""

from db_proxy_pool import DBProxyPool


class DBInterface:
    # constructor
    def __init__(self):
        self.dbProxy = DBProxyPool()

    # get cache data
    def get_cache(self, data_name):
        return self.dbProxy.get_cache(data_name)
