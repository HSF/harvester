"""
Connection to the PanDA server

"""
import ssl
try:
    # disable SNI for TLSV1_UNRECOGNIZED_NAME before importing requests
    ssl.HAS_SNI = False
except Exception:
    pass
import sys
import json
import zlib
import uuid
import inspect
import datetime
import requests
import traceback
from future.utils import iteritems
# TO BE REMOVED for python2.7
import requests.packages.urllib3
try:
    requests.packages.urllib3.disable_warnings()
except Exception:
    pass
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterconfig import harvester_config

from .base_communicator import BaseCommunicator


# connection class
class PandaCommunicator(BaseCommunicator):
    # constructor
    def __init__(self):
        BaseCommunicator.__init__(self)
        self.useInspect = False
        if hasattr(harvester_config.pandacon, 'verbose') and harvester_config.pandacon.verbose:
            self.verbose = True
            if hasattr(harvester_config.pandacon, 'useInspect') and harvester_config.pandacon.useInspect is True:
                self.useInspect = True
        else:
            self.verbose = False

    # POST with http
    def post(self, path, data):
        try:
            tmpLog = None
            if self.verbose:
                tmpLog = self.make_logger(method_name='post')
                if self.useInspect:
                    tmpExec = inspect.stack()[1][3]
                    tmpExec += '/'
                tmpExec = str(uuid.uuid4())
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
        except Exception:
            errType, errValue = sys.exc_info()[:2]
            errMsg = "failed to post with {0}:{1} ".format(errType, errValue)
            errMsg += traceback.format_exc()
        return False, errMsg

    # POST with https
    def post_ssl(self, path, data, cert=None):
        try:
            tmpLog = None
            if self.verbose:
                tmpLog = self.make_logger(method_name='post_ssl')
                if self.useInspect:
                    tmpExec = inspect.stack()[1][3]
                    tmpExec += '/'
                tmpExec = str(uuid.uuid4())
            url = '{0}/{1}'.format(harvester_config.pandacon.pandaURLSSL, path)
            if self.verbose:
                tmpLog.debug('exec={0} URL={1} data={2}'.format(tmpExec, url, str(data)))
            if cert is None:
                cert = (harvester_config.pandacon.cert_file,
                        harvester_config.pandacon.key_file)
            sw = core_utils.get_stopwatch()
            res = requests.post(url,
                                data=data,
                                headers={"Accept": "application/json",
                                         "Connection": "close"},
                                timeout=harvester_config.pandacon.timeout,
                                verify=harvester_config.pandacon.ca_cert,
                                cert=cert)
            if self.verbose:
                tmpLog.debug('exec={0} code={1} {3}. return={2}'.format(tmpExec, res.status_code, res.text,
                                                                        sw.get_elapsed_time()))
            if res.status_code == 200:
                return True, res
            else:
                errMsg = 'StatusCode={0} {1}'.format(res.status_code,
                                                     res.text)
        except Exception:
            errType, errValue = sys.exc_info()[:2]
            errMsg = "failed to post with {0}:{1} ".format(errType, errValue)
            errMsg += traceback.format_exc()
        return False, errMsg

    # PUT with https
    def put_ssl(self, path, files, cert=None):
        try:
            tmpLog = None
            tmpExec = None
            if self.verbose:
                tmpLog = self.make_logger(method_name='put_ssl')
                if self.useInspect:
                    tmpExec = inspect.stack()[1][3]
                    tmpExec += '/'
                tmpExec = str(uuid.uuid4())
            url = '{0}/{1}'.format(harvester_config.pandacon.pandaCacheURL_W, path)
            if self.verbose:
                tmpLog.debug('exec={0} URL={1} files={2}'.format(tmpExec, url, files['file'][0]))
            if cert is None:
                cert = (harvester_config.pandacon.cert_file,
                        harvester_config.pandacon.key_file)
            res = requests.post(url,
                                files=files,
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
        except Exception:
            errType, errValue = sys.exc_info()[:2]
            errMsg = "failed to put with {0}:{1} ".format(errType, errValue)
            errMsg += traceback.format_exc()
        return False, errMsg

    # check server
    def check_panda(self):
        tmpStat, tmpRes = self.post_ssl('isAlive', {})
        if tmpStat:
            return tmpStat, tmpRes.status_code, tmpRes.text
        else:
            return tmpStat, tmpRes

    # get jobs
    def get_jobs(self, site_name, node_name, prod_source_label, computing_element, n_jobs,
                 additional_criteria):
        # get logger
        tmpLog = self.make_logger('siteName={0}'.format(site_name), method_name='get_jobs')
        tmpLog.debug('try to get {0} jobs'.format(n_jobs))
        data = {}
        data['siteName'] = site_name
        data['node'] = node_name
        data['prodSourceLabel'] = prod_source_label
        data['computingElement'] = computing_element
        data['nJobs'] = n_jobs
        data['schedulerID'] = 'harvester-{0}'.format(harvester_config.master.harvester_id)
        if additional_criteria is not None:
            for tmpKey, tmpVal in additional_criteria:
                data[tmpKey] = tmpVal
        sw = core_utils.get_stopwatch()
        tmpStat, tmpRes = self.post_ssl('getJob', data)
        tmpLog.debug('getJob for {0} jobs {1}'.format(n_jobs, sw.get_elapsed_time()))
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
            except Exception:
                errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        return [], errStr

    # update jobs
    def update_jobs(self, jobspec_list, id):
        sw = core_utils.get_stopwatch()
        tmpLogG = self.make_logger('id={0}'.format(id), method_name='update_jobs')
        tmpLogG.debug('update {0} jobs'.format(len(jobspec_list)))
        retList = []
        # update events
        for jobSpec in jobspec_list:
            eventRanges, eventSpecs = jobSpec.to_event_data(max_events=10000)
            if eventRanges != []:
                tmpLogG.debug('update {0} events for PandaID={1}'.format(len(eventSpecs), jobSpec.PandaID))
                tmpRet = self.update_event_ranges(eventRanges, tmpLogG)
                if tmpRet['StatusCode'] == 0:
                    for eventSpec, retVal in zip(eventSpecs, tmpRet['Returns']):
                        if retVal in [True, False] and eventSpec.is_final_status():
                            eventSpec.subStatus = 'done'
        # update jobs in bulk
        nLookup = 100
        iLookup = 0
        while iLookup < len(jobspec_list):
            dataList = []
            jobSpecSubList = jobspec_list[iLookup:iLookup+nLookup]
            for jobSpec in jobSpecSubList:
                data = jobSpec.get_job_attributes_for_panda()
                data['jobId'] = jobSpec.PandaID
                data['siteName'] = jobSpec.computingSite
                data['state'] = jobSpec.get_status()
                data['attemptNr'] = jobSpec.attemptNr
                data['jobSubStatus'] = jobSpec.subStatus
                # change cancelled to failed to be accepted by panda server
                if data['state'] in ['cancelled', 'missed']:
                    if jobSpec.is_pilot_closed():
                        data['jobSubStatus'] = 'pilot_closed'
                    else:
                        data['jobSubStatus'] = data['state']
                    data['state'] = 'failed'
                if jobSpec.startTime is not None and 'startTime' not in data:
                    data['startTime'] = jobSpec.startTime.strftime('%Y-%m-%d %H:%M:%S')
                if jobSpec.endTime is not None and 'endTime' not in data:
                    data['endTime'] = jobSpec.endTime.strftime('%Y-%m-%d %H:%M:%S')
                if 'coreCount' not in data and jobSpec.nCore is not None:
                    data['coreCount'] = jobSpec.nCore
                if jobSpec.is_final_status() and jobSpec.status == jobSpec.get_status():
                    if jobSpec.metaData is not None:
                        data['metaData'] = json.dumps(jobSpec.metaData)
                    if jobSpec.outputFilesToReport is not None:
                        data['xml'] = jobSpec.outputFilesToReport
                dataList.append(data)
            harvester_id = harvester_config.master.harvester_id
            tmpData = {'jobList': json.dumps(dataList), 'harvester_id': harvester_id}
            tmpStat, tmpRes = self.post_ssl('updateJobsInBulk', tmpData)
            retMaps = None
            errStr = ''
            if tmpStat is False:
                errStr = core_utils.dump_error_message(tmpLogG, tmpRes)
            else:
                try:
                    tmpStat, retMaps = tmpRes.json()
                    if tmpStat is False:
                        tmpLogG.error('updateJobsInBulk failed with {0}'.format(retMaps))
                        retMaps = None
                except Exception:
                    errStr = core_utils.dump_error_message(tmpLogG)
            if retMaps is None:
                retMap = {}
                retMap['content'] = {}
                retMap['content']['StatusCode'] = 999
                retMap['content']['ErrorDiag'] = errStr
                retMaps = [json.dumps(retMap)] * len(jobSpecSubList)
            for jobSpec, retMap, data in zip(jobSpecSubList, retMaps, dataList):
                tmpLog = self.make_logger('id={0} PandaID={1}'.format(id, jobSpec.PandaID),
                                          method_name='update_jobs')
                try:
                    retMap = json.loads(retMap['content'])
                except Exception:
                    errStr = 'falied to load json'
                    retMap = {}
                    retMap['StatusCode'] = 999
                    retMap['ErrorDiag'] = errStr
                tmpLog.debug('data={0}'.format(str(data)))
                tmpLog.debug('done with {0}'.format(str(retMap)))
                retList.append(retMap)
            iLookup += nLookup
        tmpLogG.debug('done' + sw.get_elapsed_time())
        return retList

    # get events
    def get_event_ranges(self, data_map, scattered):
        retStat = False
        retVal = dict()
        try:
            getEventsChunkSize = harvester_config.pandacon.getEventsChunkSize
        except Exception:
            getEventsChunkSize = 5120
        for pandaID, data in iteritems(data_map):
            # get logger
            tmpLog = self.make_logger('PandaID={0}'.format(data['pandaID']),
                                      method_name='get_event_ranges')
            if 'nRanges' in data:
                nRanges = data['nRanges']
            else:
                nRanges = 1
            if scattered:
                data['scattered'] = True
            tmpLog.debug('start nRanges={0}'.format(nRanges))
            while nRanges > 0:
                # use a small chunk size to avoid timeout
                chunkSize = min(getEventsChunkSize, nRanges)
                data['nRanges'] = chunkSize
                tmpStat, tmpRes = self.post_ssl('getEventRanges', data)
                if tmpStat is False:
                    core_utils.dump_error_message(tmpLog, tmpRes)
                else:
                    try:
                        tmpDict = tmpRes.json()
                        if tmpDict['StatusCode'] == 0:
                            retStat = True
                            if data['pandaID'] not in retVal:
                                retVal[data['pandaID']] = []
                            retVal[data['pandaID']] += tmpDict['eventRanges']
                            # got empty
                            if len(tmpDict['eventRanges']) == 0:
                                break
                    except Exception:
                        core_utils.dump_error_message(tmpLog, tmpRes)
                        break
                nRanges -= chunkSize
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
            except Exception:
                core_utils.dump_error_message(tmp_log)
        if retMap is None:
            retMap = {}
            retMap['StatusCode'] = 999
        tmp_log.debug('done updateEventRanges with {0}'.format(str(retMap)))
        return retMap

    # get commands
    def get_commands(self, n_commands):
        harvester_id = harvester_config.master.harvester_id
        tmpLog = self.make_logger('harvesterID={0}'.format(harvester_id),
                                  method_name='get_commands')
        tmpLog.debug('Start retrieving {0} commands'.format(n_commands))
        data = {}
        data['harvester_id'] = harvester_id
        data['n_commands'] = n_commands
        tmp_stat, tmp_res = self.post_ssl('getCommands', data)
        if tmp_stat is False:
            core_utils.dump_error_message(tmpLog, tmp_res)
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
        tmpLog = self.make_logger('harvesterID={0}'.format(harvester_id),
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
                core_utils.dump_error_message(tmpLog, tmp_res)
        return False

    # get proxy
    def get_proxy(self, voms_role, cert=None):
        retVal = None
        retMsg = ''
        # get logger
        tmpLog = self.make_logger(method_name='get_proxy')
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
            except Exception:
                retMsg = core_utils.dump_error_message(tmpLog, tmpRes)
                tmpStat = False
        if tmpStat:
            tmpLog.debug('done with {0}'.format(str(retVal)))
        return retVal, retMsg

    # get resource types
    def get_resource_types(self):
        tmp_log = self.make_logger(method_name='get_resource_types')
        tmp_log.debug('Start retrieving resource types')
        data = {}
        ret_msg = ''
        ret_val = None
        tmp_stat, tmp_res = self.post_ssl('getResourceTypes', data)
        if tmp_stat is False:
            core_utils.dump_error_message(tmp_log, tmp_res)
        else:
            try:
                tmp_dict = tmp_res.json()
                if tmp_dict['StatusCode'] == 0:
                    ret_val = tmp_dict['ResourceTypes']
                    tmp_log.debug('Resource types: {0}'.format(ret_val))
                else:
                    ret_msg = tmp_dict['errorDialog']
                    core_utils.dump_error_message(tmp_log, ret_msg)
            except Exception:
                core_utils.dump_error_message(tmp_log, tmp_res)

        return ret_val, ret_msg


    # update workers
    def update_workers(self, workspec_list):
        tmpLog = self.make_logger(method_name='update_workers')
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
            except Exception:
                errStr = core_utils.dump_error_message(tmpLog)
                tmpLog.error('conversion failure from {0}'.format(tmpRes.text))
                tmpStat = False
        if tmpStat:
            tmpLog.debug('done with {0}'.format(errStr))
        return retList, errStr

    # send heartbeat of harvester instance
    def is_alive(self, key_values):
        tmpLog = self.make_logger(method_name='is_alive')
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
            except Exception:
                tmpStr = core_utils.dump_error_message(tmpLog)
                tmpLog.error('conversion failure from {0}'.format(tmpRes.text))
                tmpStat = False
        if tmpStat:
            tmpLog.debug('done with {0} : {1}'.format(retCode, tmpStr))
        return retCode, tmpStr

    # update worker stats
    def update_worker_stats(self, site_name, stats):
        tmpLog = self.make_logger(method_name='update_worker_stats')
        tmpLog.debug('start')
        data = dict()
        data['harvesterID'] = harvester_config.master.harvester_id
        data['siteName'] = site_name
        data['paramsList'] = json.dumps(stats)
        tmpLog.debug('update stats for {0}, stats: {1}'.format(site_name, stats))
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
            except Exception:
                tmpStat = False
                errStr = core_utils.dump_error_message(tmpLog)
                tmpLog.error('conversion failure from {0}'.format(tmpRes.text))
        if tmpStat:
            tmpLog.debug('done with {0}:{1}'.format(tmpStat, errStr))
        return tmpStat, errStr

    # check jobs
    def check_jobs(self, jobspec_list):
        tmpLog = self.make_logger(method_name='check_jobs')
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
                except Exception:
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
        tmpLog = self.make_logger(method_name='get_key_pair')
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
            except Exception:
                errStr = core_utils.dump_error_message(tmpLog)
        return retMap, errStr

    # upload file
    def upload_file(self, file_name, file_object, offset, read_bytes):
        tmpLog = self.make_logger(method_name='upload_file')
        tmpLog.debug('start for {0} {1}:{2}'.format(file_name, offset, read_bytes))
        file_object.seek(offset)
        files = {'file': (file_name, zlib.compress(file_object.read(read_bytes)))}
        tmpStat, tmpRes = self.put_ssl('updateLog', files)
        errStr = None
        if tmpStat is False:
            errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            errStr = tmpRes.text
            tmpLog.debug('got {0}'.format(errStr))
        return tmpStat, errStr

    # check event availability
    def check_event_availability(self, jobspec):
        retStat = False
        retVal = None
        tmpLog = self.make_logger('PandaID={0}'.format(jobspec.PandaID),
                                  method_name='check_event_availability')
        tmpLog.debug('start')
        data = dict()
        data['taskID'] = jobspec.taskID
        data['pandaID'] = jobspec.PandaID
        if jobspec.jobsetID is None:
            data['jobsetID'] = jobspec.jobParams['jobsetID']
        else:
            data['jobsetID'] = jobspec.jobsetID
        tmpStat, tmpRes = self.post_ssl('checkEventsAvailability', data)
        if tmpStat is False:
            core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                tmpDict = tmpRes.json()
                if tmpDict['StatusCode'] == 0:
                    retStat = True
                    retVal = tmpDict['nEventRanges']
            except Exception:
                core_utils.dump_error_message(tmpLog, tmpRes)
        tmpLog.debug('done with {0}'.format(retVal))
        return retStat, retVal

    # send dialog messages
    def send_dialog_messages(self, dialog_list):
        tmpLog = self.make_logger(method_name='send_dialog_messages')
        tmpLog.debug('start')
        dataList = []
        for diagSpec in dialog_list:
            dataList.append(diagSpec.convert_to_propagate())
        data = dict()
        data['harvesterID'] = harvester_config.master.harvester_id
        data['dialogs'] = json.dumps(dataList)
        tmpLog.debug('send {0} messages'.format(len(dataList)))
        tmpStat, tmpRes = self.post_ssl('addHarvesterDialogs', data)
        errStr = 'OK'
        if tmpStat is False:
            errStr = core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                retCode, tmpStr = tmpRes.json()
                if not retCode:
                    errStr = core_utils.dump_error_message(tmpLog, tmpStr)
                    tmpStat = False
            except Exception:
                errStr = core_utils.dump_error_message(tmpLog)
                tmpLog.error('conversion failure from {0}'.format(tmpRes.text))
                tmpStat = False
        if tmpStat:
            tmpLog.debug('done with {0}'.format(errStr))
        return tmpStat, errStr
