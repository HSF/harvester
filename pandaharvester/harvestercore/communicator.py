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
import copy
import json
import requests
# TO BE REMOVED for python2.7
import requests.packages.urllib3
try:
    requests.packages.urllib3.disable_warnings()
except:
    pass
import core_utils
from pandaharvester.harvesterconfig import harvester_config

# logger
_logger = core_utils.setup_logger()


# connection class
class Communicator:
    # constructor
    def __init__(self):
        pass

    # POST with http
    def post(self, path, data):
        try:
            url = '{0}/{1}'.format(harvester_config.pandacon.pandaURL, path)
            res = requests.post(url,
                                data=data,
                                headers={"Accept": "application/json"},
                                timeout=harvester_config.pandacon.timeout)
            if res.status_code == 200:
                return True, res
            else:
                errMsg = 'StatusCode={0} {1}'.format(res.status_code,
                                                     res.text)
        except:
            errType, errValue = sys.exc_info()[:2]
            errMsg = "failed to post with {0}:{1}".format(errType, errValue)
        return False, errMsg

    # POST with https
    def post_ssl(self, path, data):
        try:
            url = '{0}/{1}'.format(harvester_config.pandacon.pandaURLSSL, path)
            res = requests.post(url,
                                data=data,
                                headers={"Accept": "application/json"},
                                timeout=harvester_config.pandacon.timeout,
                                verify=harvester_config.pandacon.ca_cert,
                                cert=(harvester_config.pandacon.cert_file,
                                      harvester_config.pandacon.key_file))
            if res.status_code == 200:
                return True, res
            else:
                errMsg = 'StatusCode={0} {1}'.format(res.status_code,
                                                     res.text)
        except:
            errType, errValue = sys.exc_info()[:2]
            errMsg = "failed to post with {0}:{1}".format(errType, errValue)
        return False, errMsg

    # check server
    def is_alive(self):
        tmpStat, tmpRes = self.post_ssl('isAlive', {})
        return tmpStat, tmpRes.status_code, tmpRes.text

    # get jobs
    def get_jobs(self, site_name, node_name, prod_source_label, computing_element, n_jobs):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'siteName={0}'.format(site_name))
        tmpLog.debug('try to get {0} jobs'.format(n_jobs))
        data = {}
        data['siteName'] = site_name
        data['node'] = node_name
        data['prodSourceLabel'] = prod_source_label
        data['computingElement'] = computing_element
        data['nJobs'] = n_jobs
        tmpStat, tmpRes = self.post_ssl('getJob', data)
        if tmpStat is False:
            core_utils.dump_error_message(tmpLog, tmpRes)
        else:
            try:
                tmpDict = tmpRes.json()
                if tmpDict['StatusCode'] == 0:
                    return tmpDict['jobs']
                return []
            except:
                core_utils.dump_error_message(tmpLog, tmpRes)
        return []

    # update jobs TOBEFIXED to use bulk method
    def update_jobs(self, jobspec_list):
        retList = []
        for jobSpec in jobspec_list:
            tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(jobSpec.PandaID))
            tmpLog.debug('start')
            # update events
            eventRanges, eventSpecs = jobSpec.to_event_data()
            if eventRanges != []:
                tmpRet = self.update_event_ranges(eventRanges, tmpLog)
                if tmpRet['StatusCode'] == 0:
                    for eventSpec, retVal in zip(eventSpecs, tmpRet['Returns']):
                        if retVal in [True, False]:
                            eventSpec.subStatus = 'done'
            # update job
            if jobSpec.jobAttributes is None:
                data = {}
            else:
                data = copy.copy(jobSpec.jobAttributes)
            data['jobId'] = jobSpec.PandaID
            data['state'] = jobSpec.get_status()
            data['attemptNr'] = jobSpec.attemptNr
            data['jobSubStatus'] = jobSpec.subStatus
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
        for pandaID, data in data_map.iteritems():
            # get logger
            tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(data['pandaID']))
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
            errStr = core_utils.dump_error_message(tmp_log, tmpRes)
        else:
            try:
                retMap = tmpRes.json()
            except:
                errStr = core_utils.dump_error_message(tmp_log)
        if retMap is None:
            retMap = {}
            retMap['StatusCode'] = 999
        tmp_log.debug('done updateEventRanges with {0}'.format(str(retMap)))
        return retMap

    # get commands
    def get_commands(self, n_commands):
        harvester_id = harvester_config.pandacon.harvester_id
        _logger.debug('Start retrieving {0} commands'.format(n_commands))
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
                    _logger.debug('Finished retrieving commands')
                    return tmp_dict['commands']
                return []
            except KeyError:
                core_utils.dump_error_message(_logger, tmp_res)
        return []

    # send ACKs
    def ack_commands(self, command_ids):
        harvester_id = harvester_config.pandacon.harvester_id
        _logger.debug('Start acknowledging {0} commands (command_ids={1})'.format(len(command_ids), command_ids))
        data = {}
        data['harvester_id'] = harvester_id
        data['command_ids'] = command_ids
        tmp_stat, tmp_res = self.post_ssl('ackCommands', data)
        if tmp_stat is False:
            core_utils.dump_error_message(_logger, tmp_res)
        else:
            try:
                tmp_dict = tmp_res.json()
                if tmp_dict['StatusCode'] == 0:
                    _logger.debug('Finished acknowledging commands')
                    return True
                return False
            except KeyError:
                core_utils.dump_error_message(_logger, tmp_res)
        return False
