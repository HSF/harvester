import datetime
import threading

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.DBProxy import DBProxy
from pandaharvester.harvestercore.WorkSpec import WorkSpec
from pandaharvester.harvestercore.PluginFactory import PluginFactory

# logger
_logger = CoreUtils.setupLogger()



# class to feed events to workers
class EventFeeder (threading.Thread):

    # constructor
    def __init__(self,communicator,queueConfigMapper,singleMode=False):
        threading.Thread.__init__(self)
        self.dbProxy = DBProxy()
        self.queueConfigMapper = queueConfigMapper
        self.communicator = communicator
        self.singleMode = singleMode
        self.pluginFactory = PluginFactory()



    # main loop
    def run (self):
        lockedBy = 'eventfeeder-{0}'.format(self.ident)
        while True:
            mainLog = CoreUtils.makeLogger(_logger,'id={0}'.format(lockedBy))
            mainLog.debug('getting workers to feed events')
            workSpecsPerQueue = self.dbProxy.getWorkersToFeedEvents(harvester_config.eventfeeder.maxWorkers,
                                                                    harvester_config.eventfeeder.lockInterval)
            mainLog.debug('got {0} queues'.format(len(workSpecsPerQueue)))
            # loop over all workers
            for queueName,workSpecList in workSpecsPerQueue.iteritems():
                tmpQueLog = CoreUtils.makeLogger(_logger,'queue={0}'.format(queueName))
                # check queue
                if not self.queueConfigMapper.hasQueue(queueName):
                    tmpQueLog.error('config not found')
                    continue
                # get queue
                queueConfig = self.queueConfigMapper.getQueue(queueName)
                # get plugin
                messenger = self.pluginFactory.getPlugin(queueConfig.messenger)
                # loop over all workers
                for workSpec in workSpecList:
                    tmpLog = CoreUtils.makeLogger(_logger,'workerID={0}'.format(workSpec.workerID))
                    # get events
                    tmpStat,events = self.communicator.getEventRanges(workSpec.eventsRequestParams)
                    # failed
                    if tmpStat == False:
                        tmpLog.error('failed to get events with {0}'.format(events))
                        continue
                    tmpStat = messenger.feedEvents(workSpec,events)
                    # failed
                    if tmpStat == False:
                        tmpLog.error('failed to feed events')
                        continue
                    # update worker
                    workSpec.eventsRequest = WorkSpec.EV_useEvents
                    workSpec.eventsRequestParams = None
                    workSpec.eventFeedTime = None
                    # update local database
                    tmpStat = self.dbProxy.updateWorker(workSpec)
                    tmpLog.debug('done with {0}'.format(tmpStat))
                tmpQueLog.debug('done')    
            mainLog.debug('done')
            if self.singleMode:
                return
            # sleep
            CoreUtils.sleep(harvester_config.eventfeeder.sleepTime)
