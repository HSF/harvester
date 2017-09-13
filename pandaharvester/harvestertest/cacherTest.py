from pandaharvester.harvesterbody.cacher import Cacher
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool

proxy = DBProxy()
communicator = CommunicatorPool()

cacher = Cacher(communicator, single_mode=True)
cacher.run()

dataKey = 'ddmendpoints_objectstores.json'
print ("getting {0}".format(dataKey))
c_data = proxy.get_cache(dataKey)
print (c_data.data)
