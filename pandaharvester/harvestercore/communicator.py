"""
Connection to the PanDA server

"""
import ssl
try:
    # disable SNI for TLSV1_UNRECOGNIZED_NAME before importing requests
    ssl.HAS_SNI = False
except:
    pass
import sys
import json
import inspect
import datetime
import requests
import traceback
from future.utils import iteritems
# TO BE REMOVED for python2.7
import requests.packages.urllib3
try:
    requests.packages.urllib3.disable_warnings()
except:
    pass
from . import core_utils
from pandaharvester.harvesterconfig import harvester_config

# logger
_logger = core_utils.setup_logger('communicator')


# connection class
class Communicator:
    # constructor
    def __init__(self):
        if hasattr(harvester_config.pandacon, 'verbose') and harvester_config.pandacon.verbose:
            self.verbose = True
        else:
            self.verbose = False

    # POST with http
    def post(self, path, data):
        try:
            tmpLog = None
            if self.verbose:
                tmpLog = core_utils.make_logger(_logger, method_name='post')
                tmpExec = inspect.stack()[1][3]
            url = '{0}/{1}'.format(harvester_config.pandacon.pandaURL, path)
            if self.verbose:
                tmpLog.debug('exec={0} URL={1} data={2}'.format(tmpExec, url, str(data)))
            res = requests.post(url,
                                data=data,
                                headers={"Accept": "application/json",
                                         "Connection": "close"},
                                timeout=harvester_config.pandacon.timeout)
            if self.verbose:
                tmpLog.debug('exec={0} code={1} return={2}'.format(tmpExec, res.status_code, res.text))
            if res.status_code == 200:
                return True, res
            else:
                errMsg = 'StatusCode={0} {1}'.format(res.status_code,
                                                     res.text)
        except:
            errType, errValue = sys.exc_info()[:2]
            errMsg = "failed to post with {0}:{1} ".format(errType, errValue)
            errMsg += traceback.format_exc()
        return False, errMsg

    # POST with https
    def post_ssl(self, path, data, cert=None):
        try:
            tmpLog = None
            if self.verbose:
                tmpLog = core_utils.make_logger(_logger, method_name='post_ssl')
                tmpExec = inspect.stack()[1][3]
            url = '{0}/{1}'.format(harvester_config.pandacon.pandaURLSSL, path)
            if self.verbose:
                tmpLog.debug('exec={0} URL={1} data={2}'.format(tmpExec, url, str(data)))
            if cert is None:
                cert = (harvester_config.pandacon.cert_file,
                        harvester_config.pandacon.key_file)
            res = requests.post(url,
                                data=data,
                                headers={"Accept": "application/json",
                                         "Connection": "close"},
                                timeout=harvester_config.pandacon.timeout,
                                verify=harvester_config.pandacon.ca_cert,
                                cert=cert)
            if self.verbose:
                tmpLog.debug('exec={0} code={1} return={2}'.format(tmpExec, res.status_code, res.text))
            if res.status_code == 200:
                return True, res
            else:
                errMsg = 'StatusCode={0} {1}'.format(res.status_code,
                                                     res.text)
        except:
            errType, errValue = sys.exc_info()[:2]
            errMsg = "failed to post with {0}:{1} ".format(errType, errValue)
            errMsg += traceback.format_exc()
        return False, errMsg

    # check server
    def check_panda(self):
        tmpStat, tmpRes = self.post_ssl('isAlive', {})
        if tmpStat :
            return tmpStat, tmpRes.status_code, tmpRes.text
        else:
            return tmpStat, tmpRes

    # get jobs
    def get_jobs(self, site_name, node_name, prod_source_label, computing_element, n_jobs,
                 additional_criteria):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'siteName={0}'.format(site_name), method_name='get_jobs')
        tmpLog.debug('try to get {0} jobs'.format(n_jobs))
        data = {}
        data['siteName'] = site_name
        data['node'] = node_name
        data['prodSourceLabel'] = prod_source_label
        data['computingElement'] = computing_element
        data['nJobs'] = n_jobs
        if additional_criteria is not None:
            for tmpKey, tmpVal in additional_criteria:
                data[tmpKey] = tmpVal
        tmpStat, tmpRes = self.post_ssl('getJob', data)
        errStr = 'OK'
        if tmpStat is False:
            errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                tmpDict = tmpRes.json()
                tmpLog.debug('StatusCode={0}'.format(tmpDict['StatusCode']))
                if tmpDict['StatusCode'] == 0:
                    tmpLog.debug('got {0} jobs'.format(len(tmpDict['jobs'])))
                    return tmpDict['jobs'], errStr
                else:
                    if 'errorDialog' in tmpDict:
                        errStr = tmpDict['errorDialog']
                    else:
                        errStr = "StatusCode={0}".format(tmpDict['StatusCode'])
                return [], errStr
            except:
                errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        return [], errStr

    # update jobs TOBEFIXED to use bulk method
    def update_jobs(self, jobspec_list):
        retList = []
        for jobSpec in jobspec_list:
            tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(jobSpec.PandaID),
                                            method_name='update_jobs')
            tmpLog.debug('start')
            # update events
            eventRanges, eventSpecs = jobSpec.to_event_data()
            if eventRanges != []:
                tmpRet = self.update_event_ranges(eventRanges, tmpLog)
                if tmpRet['StatusCode'] == 0:
                    for eventSpec, retVal in zip(eventSpecs, tmpRet['Returns']):
                        if retVal in [True, False] and eventSpec.is_final_status():
                            eventSpec.subStatus = 'done'
            # update job
            data = jobSpec.get_job_attributes_for_panda()
            data['jobId'] = jobSpec.PandaID
            data['state'] = jobSpec.get_status()
            data['attemptNr'] = jobSpec.attemptNr
            data['jobSubStatus'] = jobSpec.subStatus
            # change cancelled to failed to be accepted by panda server
            if data['state'] == 'cancelled':
                data['jobSubStatus'] = data['state']
                data['state'] = 'failed'
            if jobSpec.startTime is not None:
                data['startTime'] = jobSpec.startTime.strftime('%Y-%m-%d %H:%M:%S')
            if jobSpec.endTime is not None:
                data['endTime'] = jobSpec.endTime.strftime('%Y-%m-%d %H:%M:%S')
            if jobSpec.nCore is not None:
                data['coreCount'] = jobSpec.nCore
            if jobSpec.is_final_status() and jobSpec.status == jobSpec.get_status():
                if jobSpec.metaData is not None:
                    data['metaData'] = json.dumps(jobSpec.metaData)
                if jobSpec.outputFilesToReport is not None:
                    data['xml'] = jobSpec.outputFilesToReport
            tmpLog.debug('data={0}'.format(str(data)))
            tmpStat, tmpRes = self.post_ssl('updateJob', data)
            retMap = None
            errStr = ''
            if tmpStat is False:
                errStr = core_utils.dump_error_message(tmpLog, tmpRes)
            else:
                try:
                    retMap = tmpRes.json()
                except:
                    errStr = core_utils.dump_error_message(tmpLog)
            if retMap is None:
                retMap = {}
                retMap['StatusCode'] = 999
                retMap['ErrorDiag'] = errStr
            retList.append(retMap)
            tmpLog.debug('done with {0}'.format(str(retMap)))
        return retList

    # get events
    def get_event_ranges(self, data_map):
        retStat = False
        retVal = dict()
        for pandaID, data in iteritems(data_map):
            # get logger
            tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(data['pandaID']),
                                            method_name='get_event_ranges')
            tmpLog.debug('start')
            tmpStat, tmpRes = self.post_ssl('getEventRanges', data)
            if tmpStat is False:
                core_utils.dump_error_message(tmpLog, tmpRes)
            else:
                try:
                    tmpDict = tmpRes.json()
                    if tmpDict['StatusCode'] == 0:
                        retStat = True
                        retVal[data['pandaID']] = tmpDict['eventRanges']
                except:
                    core_utils.dump_error_message(tmpLog, tmpRes)
            tmpLog.debug('done with {0}'.format(str(retVal)))
        return retStat, retVal

    # update events
    def update_event_ranges(self, event_ranges, tmp_log):
        tmp_log.debug('start update_event_ranges')
        data = {}
        data['eventRanges'] = json.dumps(event_ranges)
        data['version'] = 1
        tmp_log.debug('data={0}'.format(str(data)))
        tmpStat, tmpRes = self.post_ssl('updateEventRanges', data)
        retMap = None
        if tmpStat is False:
            core_utils.dump_error_message(tmp_log, tmpRes)
        else:
            try:
                retMap = tmpRes.json()
            except:
                core_utils.dump_error_message(tmp_log)
        if retMap is None:
            retMap = {}
            retMap['StatusCode'] = 999
        tmp_log.debug('done updateEventRanges with {0}'.format(str(retMap)))
        return retMap

    # get commands
    def get_commands(self, n_commands):
        harvester_id = harvester_config.master.harvester_id
        tmpLog = core_utils.make_logger(_logger, 'harvesterID={0}'.format(harvester_id),
                                        method_name='get_commands')
        tmpLog.debug('Start retrieving {0} commands'.format(n_commands))
        data = {}
        data['harvester_id'] = harvester_id
        data['n_commands'] = n_commands
        tmp_stat, tmp_res = self.post_ssl('getCommands', data)
        if tmp_stat is False:
            core_utils.dump_error_message(_logger, tmp_res)
        else:
            try:
                tmp_dict = tmp_res.json()
                if tmp_dict['StatusCode'] == 0:
                    tmpLog.debug('Commands {0}'.format(tmp_dict['Commands']))
                    return tmp_dict['Commands']
                return []
            except KeyError:
                core_utils.dump_error_message(tmpLog, tmp_res)
        return []

    # send ACKs
    def ack_commands(self, command_ids):
        harvester_id = harvester_config.master.harvester_id
        tmpLog = core_utils.make_logger(_logger, 'harvesterID={0}'.format(harvester_id),
                                        method_name='ack_commands')
        tmpLog.debug('Start acknowledging {0} commands (command_ids={1})'.format(len(command_ids), command_ids))
        data = {}
        data['command_ids'] = json.dumps(command_ids)
        tmp_stat, tmp_res = self.post_ssl('ackCommands', data)
        if tmp_stat is False:
            core_utils.dump_error_message(tmpLog, tmp_res)
        else:
            try:
                tmp_dict = tmp_res.json()
                if tmp_dict['StatusCode'] == 0:
                    tmpLog.debug('Finished acknowledging commands')
                    return True
                return False
            except KeyError:
                core_utils.dump_error_message(_logger, tmp_res)
        return False
    
    # get proxy
    def get_proxy(self, voms_role, cert=None):
        retVal = None
        retMsg = ''
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name='get_proxy')
        tmpLog.debug('start')
        data = {'role': voms_role}
        tmpStat, tmpRes = self.post_ssl('getProxy', data, cert)
        if tmpStat is False:
            core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                tmpDict = tmpRes.json()
                if tmpDict['StatusCode'] == 0:
                    retVal = tmpDict['userProxy']
                else:
                    retMsg = tmpDict['errorDialog']
                    core_utils.dump_error_message(tmpLog, retMsg)
                    tmpStat = False
            except:
                retMsg = core_utils.dump_error_message(tmpLog, tmpRes)
                tmpStat = False
        if tmpStat:
            tmpLog.debug('done with {0}'.format(str(retVal)))
        return retVal, retMsg

    # update workers
    def update_workers(self, workspec_list):
        tmpLog = core_utils.make_logger(_logger, method_name='update_workers')
        tmpLog.debug('start')
        dataList = []
        for workSpec in workspec_list:
            dataList.append(workSpec.convert_to_propagate())
        data = dict()
        data['harvesterID'] = harvester_config.master.harvester_id
        data['workers'] = json.dumps(dataList)
        tmpLog.debug('update {0} workers'.format(len(dataList)))
        tmpStat, tmpRes = self.post_ssl('updateWorkers', data)
        retList = None
        errStr = 'OK'
        if tmpStat is False:
            errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                retCode, retList = tmpRes.json()
                if not retCode:
                    errStr = core_utils.dump_error_message(tmpLog, retList)
                    retList = None
                    tmpStat = False
            except:
                errStr = core_utils.dump_error_message(tmpLog)
                tmpLog.error('conversion failure from {0}'.format(tmpRes.text))
                tmpStat = False
        if tmpStat:
            tmpLog.debug('done with {0}'.format(errStr))
        return retList, errStr

    # send instance heartbeat
    def is_alive(self, key_values):
        tmpLog = core_utils.make_logger(_logger, method_name='is_alive')
        tmpLog.debug('start')
        # convert datetime
        for tmpKey, tmpVal in iteritems(key_values):
            if isinstance(tmpVal, datetime.datetime):
                tmpVal = 'datetime/' + tmpVal.strftime('%Y-%m-%d %H:%M:%S.%f')
                key_values[tmpKey] = tmpVal
        # send data
        data = dict()
        data['harvesterID'] = harvester_config.master.harvester_id
        data['data'] = json.dumps(key_values)
        tmpStat, tmpRes = self.post_ssl('harvesterIsAlive', data)
        retCode = False
        if tmpStat is False:
            tmpStr = core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                retCode, tmpStr = tmpRes.json()
            except:
                tmpStr = core_utils.dump_error_message(tmpLog)
                tmpLog.error('conversion failure from {0}'.format(tmpRes.text))
                tmpStat = False
        if tmpStat:
            tmpLog.debug('done with {0} : {1}'.format(retCode, tmpStr))
        return retCode, tmpStr

    # update worker stats
    def update_worker_stats(self, site_name, stats):
        tmpLog = core_utils.make_logger(_logger, method_name='update_worker_stats')
        tmpLog.debug('start')
        data = dict()
        data['harvesterID'] = harvester_config.master.harvester_id
        data['siteName'] = site_name
        data['paramsList'] = json.dumps(stats)
        tmpLog.debug('update stats for {0}'.format(site_name))
        tmpStat, tmpRes = self.post_ssl('reportWorkerStats', data)
        errStr = 'OK'
        if tmpStat is False:
            errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                retCode, retMsg = tmpRes.json()
                if not retCode:
                    tmpStat = False
                    errStr = core_utils.dump_error_message(tmpLog, retMsg)
            except:
                tmpStat = False
                errStr = core_utils.dump_error_message(tmpLog)
                tmpLog.error('conversion failure from {0}'.format(tmpRes.text))
        if tmpStat:
            tmpLog.debug('done with {0}:{1}'.format(tmpStat, errStr))
        return tmpStat, errStr

    # check jobs
    def check_jobs(self, jobspec_list):
        tmpLog = core_utils.make_logger(_logger, method_name='check_jobs')
        tmpLog.debug('start')
        retList = []
        nLookup = 100
        iLookup = 0
        while iLookup < len(jobspec_list):
            ids = []
            for jobSpec in jobspec_list[iLookup:iLookup+nLookup]:
                ids.append(str(jobSpec.PandaID))
            iLookup += nLookup
            data = dict()
            data['ids'] = ','.join(ids)
            tmpStat, tmpRes = self.post_ssl('checkJobStatus', data)
            errStr = 'OK'
            if tmpStat is False:
                errStr = core_utils.dump_error_message(tmpLog, tmpRes)
                tmpRes = None
            else:
                try:
                    tmpRes = tmpRes.json()
                except:
                    tmpRes = None
                    errStr = core_utils.dump_error_message(tmpLog)
            for idx, pandaID in enumerate(ids):
                if tmpRes is None or 'data' not in tmpRes or idx >= len(tmpRes['data']):
                    retMap = dict()
                    retMap['StatusCode'] = 999
                    retMap['ErrorDiag'] = errStr
                else:
                    retMap = tmpRes['data'][idx]
                    retMap['StatusCode'] = 0
                    retMap['ErrorDiag'] = errStr
                retList.append(retMap)
                tmpLog.debug('got {0} for PandaID={1}'.format(str(retMap), pandaID))
        return retList

    # get key pair
    def get_key_pair(self, public_key_name, private_key_name):
        tmpLog = core_utils.make_logger(_logger, method_name='get_key_pair')
        tmpLog.debug('start for {0}:{1}'.format(public_key_name, private_key_name))
        data = dict()
        data['publicKeyName'] = public_key_name
        data['privateKeyName'] = private_key_name
        tmpStat, tmpRes = self.post_ssl('getKeyPair', data)
        retMap = None
        errStr = None
        if tmpStat is False:
            errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                retMap = tmpRes.json()
                if retMap['StatusCode'] != 0:
                    errStr = 'failed to get key with StatusCode={0} : {1}'.format(retMap['StatusCode'],
                                                                                  retMap['errorDialog'])
                    tmpLog.error(errStr)
                    retMap = None
                else:
                    tmpLog.debug('got {0} with'.format(str(retMap), errStr))
            except:
                errStr = core_utils.dump_error_message(tmpLog)
        return retMap, errStr
