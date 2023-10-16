import sys
import logging
from future.utils import iteritems

from pandaharvester.harvesterbody.cacher import Cacher
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool

for loggerName, loggerObj in iteritems(logging.Logger.manager.loggerDict):
    if loggerName.startswith("panda.log"):
        if len(loggerObj.handlers) == 0:
            continue
        if loggerName.split(".")[-1] not in ["cacher"]:
            continue
        stdoutHandler = logging.StreamHandler(sys.stdout)
        stdoutHandler.setFormatter(loggerObj.handlers[0].formatter)
        loggerObj.addHandler(stdoutHandler)

proxy = DBProxy()
communicator = CommunicatorPool()

cacher = Cacher(communicator, single_mode=True)
cacher.execute(force_update=True, skip_lock=True)
