import sys
import logging
from future.utils import iteritems

from pandaharvester.harvestercore.communicator_pool import CommunicatorPool

for loggerName, loggerObj in iteritems(logging.Logger.manager.loggerDict):
    if loggerName.startswith("panda.log"):
        if len(loggerObj.handlers) == 0:
            continue
        if loggerName.split(".")[-1] in ["db_proxy"]:
            continue
        stdoutHandler = logging.StreamHandler(sys.stdout)
        stdoutHandler.setFormatter(loggerObj.handlers[0].formatter)
        loggerObj.addHandler(stdoutHandler)

a = CommunicatorPool()
return_object = a.check_panda()
print(return_object)
