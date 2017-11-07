import json
from pandaharvester.harvesterbody.cacher import Cacher
from pandaharvester.harvestercore.db_proxy import DBProxy
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
from pandaharvester.harvestercore import core_utils

# make communication channel to PanDA
com = CommunicatorPool()

proxy = DBProxy()

cacher = Cacher(com,single_mode=True)
cacher.run()

# now get this results from DBProxy and print out data (if possible)
queuStat = proxy.get_cache('panda_queues.json',None)
if queuStat is None:
    queueStat = dict()
else:
    queueStat = queuStat.data

#print "panda_queues.json data :",queueStat
#print "panda_queues.json type :",type(queueStat)
print 
print '{{"{}":'.format('ALCF_Theta'),json.dumps(queueStat['ALCF_Theta']),"}"
print 
#print "panda_queues.json data [ALCF_Theta] :",json.dumps(queueStat['ALCF_Theta'], indent=4)
print
print

"""
globalDict = core_utils.get_global_dict()
print "printing globalDict "
for k,v in globalDict.iteritems():
     print "key: {}".format(k)
     print "value: {}".format(v)
     #print "key: {}, value: {}".format(k, v)
"""

