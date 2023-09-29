import datetime

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger("simple_throttler")


# simple throttler
class SimpleThrottler(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        # logic type : AND: throttled if all rules are satisfied, OR: throttled if one rule is satisfied
        self.logicType = "OR"
        PluginBase.__init__(self, **kwarg)
        self.dbProxy = DBProxy()

    # check if to be throttled
    def to_be_throttled(self, queue_config):
        tmpLog = self.make_logger(baseLogger, "computingSite={0}".format(queue_config.queueName), method_name="to_be_throttled")
        tmpLog.debug("start")
        # set default return vale
        if self.logicType == "OR":
            retVal = False, "no rule was satisfied"
        else:
            retVal = True, "all rules were satisfied"
        # loop over all rules
        criteriaList = []
        maxMissedList = []
        timeNow = datetime.datetime.utcnow()
        for rule in self.rulesForMissed:
            # convert rule to criteria
            if rule["level"] == "site":
                criteria = dict()
                criteria["siteName"] = queue_config.siteName
                criteria["timeLimit"] = timeNow - datetime.timedelta(minutes=rule["timeWindow"])
                criteriaList.append(criteria)
                maxMissedList.append(rule["maxMissed"])
            elif rule["level"] == "pq":
                criteria = dict()
                criteria["computingSite"] = queue_config.queueName
                criteria["timeLimit"] = timeNow - datetime.timedelta(minutes=rule["timeWindow"])
                criteriaList.append(criteria)
                maxMissedList.append(rule["maxMissed"])
            elif rule["level"] == "ce":
                elmName = "computingElements"
                if elmName not in queue_config.submitter:
                    tmpLog.debug("skipped since {0} is undefined in submitter config".format(elmName))
                    continue
                for ce in queue_config.submitter[elmName]:
                    criteria = dict()
                    criteria["computingElement"] = ce
                    criteria["timeLimit"] = timeNow - datetime.timedelta(minutes=rule["timeWindow"])
                    criteriaList.append(criteria)
                    maxMissedList.append(rule["maxMissed"])
        # loop over all criteria
        for criteria, maxMissed in zip(criteriaList, maxMissedList):
            nMissed = self.dbProxy.get_num_missed_workers(queue_config.queueName, criteria)
            if nMissed > maxMissed:
                if self.logicType == "OR":
                    tmpMsg = "logic={0} and ".format(self.logicType)
                    tmpMsg += "nMissed={0} > maxMissed={1} for {2}".format(nMissed, maxMissed, str(criteria))
                    retVal = True, tmpMsg
                    break
            else:
                if self.logicType == "AND":
                    tmpMsg = "logic={0} and ".format(self.logicType)
                    tmpMsg += "nMissed={0} <= maxMissed={1} for {2}".format(nMissed, maxMissed, str(criteria))
                    retVal = False, tmpMsg
                    break
        tmpLog.debug("ret={0} : {1}".format(*retVal))
        return retVal
