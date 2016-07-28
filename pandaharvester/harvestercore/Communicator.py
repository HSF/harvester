"""
Connection to the PanDA server

"""

import os
import sys
import requests

# TO BE REMOVED for python2.7
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()


import CoreUtils
from pandaharvester.harvesterconfig import harvester_config

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('Communicator')


# connection class
class Communicator:
    
    # constrictor
    def __init__(self):
        pass



    # POST with http
    def post(self,path,data):
        try:
            url = '{0}/{1}'.format(harvester_config.pandacon.pandaURL,path)
            res = requests.post(url,
                                data=data,
                                headers={"Accept":"application/json"},
                                timeout=harvester_config.pandacon.timeout)
            if res.status_code == 200:
                return True,res
            else:
                errMsg = 'StatusCode={0} {1}'.format(res.status_code,
                                                     res.text)
        except:
            errType,errValue = sys.exc_info()[:2]
            errMsg = "failed to post with {0}:{1}".format(errType,errValue)
        return False,errMsg



    # POST with https
    def postSSL(self,path,data):
        try:
            url = '{0}/{1}'.format(harvester_config.pandacon.pandaURLSSL,path)
            res = requests.post(url,
                                data=data,
                                headers={"Accept":"application/json"},
                                timeout=harvester_config.pandacon.timeout,
                                verify=harvester_config.pandacon.ca_cert,
                                cert=(harvester_config.pandacon.cert_file,
                                      harvester_config.pandacon.key_file))
            if res.status_code == 200:
                return True,res
            else:
                errMsg = 'StatusCode={0} {1}'.format(res.status_code,
                                                     res.text)
        except:
            errType,errValue = sys.exc_info()[:2]
            errMsg = "failed to post with {0}:{1}".format(errType,errValue)
        return False,errMsg



    # get jobs
    def getJobs(self,siteName,nodeName,prodSourceLabel,computingElement,nJobs):
        # get logger
        tmpLog = CoreUtils.makeLogger(_logger,'siteName={0}'.format(siteName))
        tmpLog.debug('getting {0} jobs'.format(nJobs))
        data = {}
        data['siteName']         = siteName
        data['node']             = nodeName
        data['prodSourceLabel']  = prodSourceLabel
        data['computingElement'] = computingElement
        data['nJobs']            = nJobs
        tmpStat,tmpRes = self.postSSL('getJob',data)
        if tmpStat == False:
            CoreUtils.dumpErrorMessage(tmpLog,tmpRes)
        else:
            try:
                tmpDict = tmpRes.json()
                if tmpDict['StatusCode'] == 0:
                    return tmpDict['jobs']
                return []
            except:
                CoreUtils.dumpErrorMessage(tmpLog,tmpRes)
        return []



    # update jobs TOBEFIXED
    def updateJobs(self,jobList):
        retList = []
        for jobSpec in jobList:
            retMap = {}
            retMap['StatusCode'] = 0
            retList.append(retMap)
        return retList
