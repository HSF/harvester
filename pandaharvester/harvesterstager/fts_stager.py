import os
import sys
import os.path
import zipfile
import hashlib
import requests

# TO BE REMOVED for python2.7
import requests.packages.urllib3
try:
    requests.packages.urllib3.disable_warnings()
except:
    pass
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config

# logger
baseLogger = core_utils.setup_logger('fts_stager')


# plugin for stager with FTS
class FtsStager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check status
    def check_status(self, jobspec):
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                        method_name='check_status')
        tmpLog.debug('start')
        # loop over all files
        allChecked = True
        oneErrMsg = None
        trasnferStatus = {}
        for fileSpec in jobspec.outFiles:
            # get transfer ID
            transferID = fileSpec.fileAttributes['transferID']
            if transferID not in trasnferStatus:
                # get status
                errMsg = None
                try:
                    url = "{0}/jobs/{1}".format(self.ftsServer,
                                                transferID)
                    res = requests.get(url,
                                       timeout=self.ftsLookupTimeout,
                                       verify=self.ca_cert,
                                       cert=(harvester_config.pandacon.cert_file,
                                             harvester_config.pandacon.key_file)
                                       )
                    if res.status_code == 200:
                        transferData = res.json()
                        trasnferStatus[transferID] = transferData["job_state"]
                        tmpLog.debug('got {0} for {1}'.format(trasnferStatus[transferID],
                                                              transferID))
                    else:
                        errMsg = 'StatusCode={0} {1}'.format(res.status_code,
                                                             res.text)
                except:
                    if errMsg is None:
                        errtype, errvalue = sys.exc_info()[:2]
                        errMsg = "{0} {1}".format(errtype.__name__, errvalue)
                # failed
                if errMsg is not None:
                    allChecked = False
                    tmpLog.error('failed to get status for {0} with {1}'.format(transferID,
                                                                                errMsg))
                    # set dummy not to lookup again
                    trasnferStatus[transferID] = None
                    # keep one message
                    if oneErrMsg is None:
                        oneErrMsg = errMsg
            # final status
            if trasnferStatus[transferID] == 'DONE':
                fileSpec.status = 'finished'
            elif trasnferStatus[transferID] in ['FAILED', 'CANCELED']:
                fileSpec.status = 'failed'
        if allChecked:
            return True, ''
        else:
            return False, oneErrMsg

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                        method_name='trigger_stage_out')
        tmpLog.debug('start')
        # default return
        tmpRetVal = (True, '')
        # loop over all files
        files = []
        lfns = set()
        fileAttrs = jobspec.get_output_file_attributes()
        for fileSpec in jobspec.outFiles:
            # skip zipped files
            if fileSpec.zipFileID is not None:
                continue
            # source and destination URLs
            if fileSpec.fileType == 'es_output':
                srcURL = self.srcEndpointES + fileSpec.path
                dstURL = self.dstEndpointES + fileSpec.path
                # set OS ID
                fileSpec.objstoreID = self.esObjStoreID
            else:
                scope = fileAttrs[fileSpec.lfn]['scope']
                hash = hashlib.md5()
                hash.update('%s:%s' % (scope, fileSpec.lfn))
                hash_hex = hash.hexdigest()
                correctedscope = "/".join(scope.split('.'))
                if fileSpec.fileType == 'output':
                    srcURL = self.srcEndpointOut + fileSpec.path
                    dstURL = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(endPoint=self.dstEndpointOut,
                                                                               scope=correctedscope,
                                                                               hash1=hash_hex[0:2],
                                                                               hash2=hash_hex[2:4],
                                                                               lfn=fileSpec.lfn)
                elif fileSpec.fileType == 'log':
                    # skip if no endpoint
                    if self.srcEndpointLog == None:
                        continue
                    srcURL = self.srcEndpointLog + fileSpec.path
                    dstURL = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(endPoint=self.dstEndpointLog,
                                                                               scope=correctedscope,
                                                                               hash1=hash_hex[0:2],
                                                                               hash2=hash_hex[2:4],
                                                                               lfn=fileSpec.lfn)
                else:
                    continue
            tmpLog.debug('src={srcURL} dst={dstURL}'.format(srcURL=srcURL, dstURL=dstURL))
            files.append({
                "sources": [srcURL],
                "destinations": [dstURL],
            })
            lfns.add(fileSpec.lfn)
        # submit
        if files != []:
            # get status
            errMsg = None
            try:
                url = "{0}/jobs".format(self.ftsServer)
                res = requests.post(url,
                                    json={"Files": files},
                                    timeout=self.ftsLookupTimeout,
                                    verify=self.ca_cert,
                                    cert=(harvester_config.pandacon.cert_file,
                                          harvester_config.pandacon.key_file)
                                    )
                if res.status_code == 200:
                    transferData = res.json()
                    transferID = transferData["job_id"]
                    tmpLog.debug('successfully submitted id={0}'.format(transferID))
                    # set
                    for fileSpec in jobspec.outFiles:
                        if fileSpec.fileAttributes == None:
                            fileSpec.fileAttributes = {}
                        fileSpec.fileAttributes['transferID'] = transferID
                else:
                    # HTTP error
                    errMsg = 'StatusCode={0} {1}'.format(res.status_code,
                                                         res.text)
            except:
                if errMsg is None:
                    errtype, errvalue = sys.exc_info()[:2]
                    errMsg = "{0} {1}".format(errtype.__name__, errvalue)
            # failed
            if errMsg is not None:
                tmpLog.error('failed to submit transfer to {0} with {1}'.format(url, errMsg))
                tmpRetVal = (False, errMsg)
        # return
        tmpLog.debug('done')
        return tmpRetVal

    # zip output files
    def zip_output(self, jobspec):
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                        method_name='zip_output')
        tmpLog.debug('start')
        try:
            for fileSpec in jobspec.outFiles:
                if self.zipDir == "${SRCDIR}":
                    # the same directory as src
                    zipDir = os.path.dirname(next(iter(fileSpec.associatedFiles)).path)
                else:
                    zipDir = self.zipDir
                zipPath = os.path.join(zipDir, fileSpec.lfn)
                # remove zip file just in case
                try:
                    os.remove(zipPath)
                except:
                    pass
                # make zip file
                with zipfile.ZipFile(zipPath, "w", zipfile.ZIP_STORED) as zf:
                    for assFileSpec in fileSpec.associatedFiles:
                        zf.write(assFileSpec.path)
                # set path
                fileSpec.path = zipPath
                # get size
                statInfo = os.stat(zipPath)
                fileSpec.fsize = statInfo.st_size
        except:
            errMsg = core_utils.dump_error_message(tmpLog)
            return False, 'failed to zip with {0}'.format(errMsg)
        tmpLog.debug('done')
        return True, ''
