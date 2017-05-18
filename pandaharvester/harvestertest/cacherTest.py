from pandaharvester.harvesterbody.cacher import Cacher
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy

proxy = DBProxy()

cacher = Cacher(single_mode=True)
cacher.run()
