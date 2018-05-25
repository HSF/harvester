import time
import datetime

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.command_spec import CommandSpec
from pandaharvester.harvesterbody.agent_base import AgentBase
from pandaharvester.harvestercore.pilot_errors import PilotErrors

# logger
_logger = core_utils.setup_logger('propagator')

STATS_PERIOD = 300


# propagate important checkpoints to panda
class Propagator(AgentBase):
    # constructor
    def __init__(self, communicator, queue_config_mapper, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.communicator = communicator
        self.queueConfigMapper = queue_config_mapper
        self._last_stats_update = None

    # main loop
    def run(self):
        while True:
            sw = core_utils.get_stopwatch()
            mainLog = self.make_logger(_logger, 'id={0}'.format(self.ident), method_name='run')
            mainLog.debug('getting jobs to propagate')
            jobSpecs = self.dbProxy.get_jobs_to_propagate(harvester_config.propagator.maxJobs,
                                                          harvester_config.propagator.lockInterval,
                                                          harvester_config.propagator.updateInterval,
                                                          self.ident)
            mainLog.debug('got {0} jobs'.format(len(jobSpecs)))
            # update jobs in central database
            iJobs = 0
            nJobs = harvester_config.propagator.nJobsInBulk
            hbSuppressMap = dict()
            while iJobs < len(jobSpecs):
                jobList = jobSpecs[iJobs:iJobs + nJobs]
                iJobs += nJobs
                # collect jobs to update or check
                jobListToSkip = []
                jobListToUpdate = []
                jobListToCheck = []
                retList = []
                for tmpJobSpec in jobList:
                    if tmpJobSpec.computingSite not in hbSuppressMap:
                        queueConfig = self.queueConfigMapper.get_queue(tmpJobSpec.computingSite)
                        hbSuppressMap[tmpJobSpec.computingSite] = queueConfig.get_no_heartbeat_status()
                    # heartbeat is suppressed
                    if tmpJobSpec.status in hbSuppressMap[tmpJobSpec.computingSite] and \
                            not tmpJobSpec.not_suppress_heartbeat():
                        # check running job to detect lost heartbeat
                        if tmpJobSpec.status == 'running':
                            jobListToCheck.append(tmpJobSpec)
                        else:
                            jobListToSkip.append(tmpJobSpec)
                            retList.append({'StatusCode': 0, 'command': None})
                    else:
                        jobListToUpdate.append(tmpJobSpec)
                retList += self.communicator.check_jobs(jobListToCheck)
                retList += self.communicator.update_jobs(jobListToUpdate)
                # logging
                for tmpJobSpec, tmpRet in zip(jobListToSkip+jobListToCheck+jobListToUpdate, retList):
                    if tmpRet['StatusCode'] == 0:
                        if tmpJobSpec in jobListToUpdate:
                            mainLog.debug('updated PandaID={0} status={1}'.format(tmpJobSpec.PandaID,
                                                                                  tmpJobSpec.status))
                        else:
                            mainLog.debug('skip updating PandaID={0} status={1}'.format(tmpJobSpec.PandaID,
                                                                                        tmpJobSpec.status))
                        # release job
                        tmpJobSpec.propagatorLock = None
                        if tmpJobSpec.is_final_status() and tmpJobSpec.status == tmpJobSpec.get_status():
                            # unset to disable further updating
                            tmpJobSpec.propagatorTime = None
                            tmpJobSpec.subStatus = 'done'
                        else:
                            # check event availability
                            if tmpJobSpec.status == 'starting' and 'eventService' in tmpJobSpec.jobParams and \
                                    tmpJobSpec.subStatus != 'submitted':
                                tmpEvStat, tmpEvRet = self.communicator.check_event_availability(tmpJobSpec)
                                if tmpEvStat and tmpEvRet == 0:
                                    mainLog.debug('kill PandaID={0} due to no event'.format(tmpJobSpec.PandaID))
                                    tmpRet['command'] = 'tobekilled'
                            # got kill command
                            if 'command' in tmpRet and tmpRet['command'] in ['tobekilled']:
                                nWorkers = self.dbProxy.kill_workers_with_job(tmpJobSpec.PandaID)
                                if nWorkers == 0:
                                    # no workers
                                    tmpJobSpec.status = 'cancelled'
                                    tmpJobSpec.subStatus = 'killed'
                                    tmpJobSpec.set_pilot_error(PilotErrors.ERR_PANDAKILL,
                                                               PilotErrors.pilotError[PilotErrors.ERR_PANDAKILL])
                                    tmpJobSpec.stateChangeTime = datetime.datetime.utcnow()
                                    tmpJobSpec.trigger_propagation()
                        self.dbProxy.update_job(tmpJobSpec, {'propagatorLock': self.ident})
                    else:
                        mainLog.error('failed to update PandaID={0} status={1}'.format(tmpJobSpec.PandaID,
                                                                                       tmpJobSpec.status))
            mainLog.debug('getting workers to propagate')
            workSpecs = self.dbProxy.get_workers_to_propagate(harvester_config.propagator.maxWorkers,
                                                              harvester_config.propagator.updateInterval)
            mainLog.debug('got {0} workers'.format(len(workSpecs)))
            # update workers in central database
            iWorkers = 0
            nWorkers = harvester_config.propagator.nWorkersInBulk
            while iWorkers < len(workSpecs):
                workList = workSpecs[iWorkers:iWorkers + nJobs]
                iWorkers += nWorkers
                retList, tmpErrStr = self.communicator.update_workers(workList)
                # logging
                if retList is None:
                    mainLog.error('failed to update workers with {0}'.format(tmpErrStr))
                else:
                    for tmpWorkSpec, tmpRet in zip(workList, retList):
                        if tmpRet:
                            mainLog.debug('updated workerID={0} status={1}'.format(tmpWorkSpec.workerID,
                                                                                   tmpWorkSpec.status))
                            # update logs
                            for logFilePath, logOffset, logSize, logRemoteName in \
                                    tmpWorkSpec.get_log_files_to_upload():
                                with open(logFilePath, 'rb') as logFileObj:
                                    tmpStat, tmpErr = self.communicator.upload_file(logRemoteName, logFileObj,
                                                                                    logOffset, logSize)
                                    if tmpStat:
                                        tmpWorkSpec.update_log_files_to_upload(logFilePath, logOffset+logSize)
                            # disable further update
                            if tmpWorkSpec.is_final_status():
                                tmpWorkSpec.disable_propagation()
                            self.dbProxy.update_worker(tmpWorkSpec, {'workerID': tmpWorkSpec.workerID})
                        else:
                            mainLog.error('failed to update workerID={0} status={1}'.format(tmpWorkSpec.workerID,
                                                                                            tmpWorkSpec.status))
            mainLog.debug('getting commands')
            commandSpecs = self.dbProxy.get_commands_for_receiver('propagator')
            mainLog.debug('got {0} commands'.format(len(commandSpecs)))
            for commandSpec in commandSpecs:
                if commandSpec.command.startswith(CommandSpec.COM_reportWorkerStats):
                    # get worker stats
                    siteName = commandSpec.command.split(':')[-1]
                    workerStats = self.dbProxy.get_worker_stats(siteName)
                    if len(workerStats) == 0:
                        mainLog.error('failed to get worker stats for {0}'.format(siteName))
                    else:
                        # report worker stats
                        tmpRet, tmpStr = self.communicator.update_worker_stats(siteName, workerStats)
                        if tmpRet:
                            mainLog.debug('updated worker stats (command) for {0}'.format(siteName))
                        else:
                            mainLog.error('failed to update worker stats (command) for {0} err={1}'.format(siteName, tmpStr))

            if not self._last_stats_update or time.time() - self._last_stats_update > STATS_PERIOD:

                # get active UPS queues. PanDA server needs to know about them and which harvester instance is taking
                # care of them
                active_ups_queues = self.queueConfigMapper.get_active_ups_queues()

                # update worker stats for all sites
                worker_stats_bulk = self.dbProxy.get_worker_stats_bulk(active_ups_queues)
                if not worker_stats_bulk:
                    mainLog.error('failed to get worker stats in bulk')
                else:
                    for site_name in worker_stats_bulk:
                        tmp_ret, tmp_str = self.communicator.update_worker_stats(site_name, worker_stats_bulk[site_name])
                        if tmp_ret:
                            mainLog.debug('update of worker stats (bulk) for {0}'.format(site_name))
                            self._last_stats_update = time.time()
                        else:
                            mainLog.error('failed to update worker stats (bulk) for {0} err={1}'.format(site_name, tmp_str))
            # send dialog messages
            mainLog.debug('getting dialog messages to propagate')
            try:
                maxDialogs = harvester_config.propagator.maxDialogs
            except:
                maxDialogs = 50
            diagSpecs = self.dbProxy.get_dialog_messages_to_send(maxDialogs,
                                                                 harvester_config.propagator.lockInterval)
            mainLog.debug('got {0} dialogs'.format(len(diagSpecs)))
            if len(diagSpecs) > 0:
                tmpStat, tmpStr = self.communicator.send_dialog_messages(diagSpecs)
                if tmpStat:
                    diagIDs = [diagSpec.diagID for diagSpec in diagSpecs]
                    self.dbProxy.delete_dialog_messages(diagIDs)
                    mainLog.debug('sent {0} dialogs'.format(len(diagSpecs)))

                else:
                    mainLog.error('failed to send dialogs err={0}'.format(tmpStr))
            if sw.get_elapsed_time_in_sec() > harvester_config.propagator.lockInterval:
                mainLog.warning('a single cycle was longer than lockInterval ' + sw.get_elapsed_time())
            else:
                mainLog.debug('done' + sw.get_elapsed_time())
            # check if being terminated
            if self.terminated(harvester_config.propagator.sleepTime):
                mainLog.debug('terminated')
                return
