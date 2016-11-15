import os
import sys
import shutil
import os.path
import zipfile
import uuid

# TO BE REMOVED for python2.7
import requests.packages.urllib3
try:
    requests.packages.urllib3.disable_warnings()
except:
    pass
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermover import mover_utils

from rucio.client import Client as RucioClient
from rucio.common.exception import RuleNotFound

# logger
baseLogger = core_utils.setup_logger()


# plugin for stage-out with Rucio
class RucioStager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check status
    def check_status(self, jobspec):
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID))
        tmpLog.debug('start')
        # loop over all files
        allChecked = True
        oneErrMsg = None
        transferStatus = dict()
        for fileSpec in jobspec.outFiles:
            # skip already don
            if fileSpec.status in ['finished', 'failed']:
                continue
            # get transfer ID
            transferID = fileSpec.fileAttributes['transferID']
            if transferID not in transferStatus:
                # get status
                errMsg = None
                try:
                    rucioAPI = RucioClient()
                    ruleInfo = rucioAPI.get_replication_rule(ruleID)
                    tmpTransferStatus = ruleInfo['state']
                    tmpLog.debug('got state={0} for rule={1}'.format(tmpTransferStatus, ruleID))
                except RuleNotFound:
                    tmpLog.error('rule {0} not found'.format(ruleID))
                    tmpTransferStatus = 'FAILED'
                except:
                    err_type, err_value = sys.exc_info()[:2]
                    errMsg = "{0} {1}".format(err_type.__name__, err_value)
                    tmpLog.error('failed to get status for rule={0} with {1}'.format(transferID, errMsg))
                    # set dummy not to lookup again
                    tmpTransferStatus = None
                    allChecked = False
                    # keep one message
                    if oneErrMsg is None:
                        oneErrMsg = errMsg
                transferStatus[transferID] = tmpTransferStatus
            # final status
            if transferStatus[transferID] == 'OK':
                fileSpec.status = 'finished'
            elif transferStatus[transferID] in ['FAILED', 'CANCELED']:
                fileSpec.status = 'failed'
        if allChecked:
            return True, ''
        else:
            return False, oneErrMsg

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID))
        tmpLog.debug('start')
        # default return
        tmpRetVal = (True, '')
        # loop over all files
        files = dict()
        transferIDs = dict()
        transferDatasets = dict()
        fileAttrs = jobspec.get_output_file_attributes()
        for fileSpec in jobspec.outFiles:
            # skip zipped files
            if fileSpec.zipFileID is not None:
                continue
            # skip if already processed
            if 'transferDataset' in fileSpec.fileAttributes:
                if fileSpec.fileType not in transferDatasets:
                    transferDatasets[fileSpec.fileType] = fileSpec.fileAttributes['transferDataset']
                if fileSpec.fileType not in transferIDs:
                    transferIDs[fileSpec.fileType] = fileSpec.fileAttributes['transferID']
                continue
            # set OS ID
            if fileSpec.fileType == 'es_output':
                fileSpec.objstoreID = self.objStoreID_ES
            # make path where file is copied for transfer
            scope = fileAttrs[fileSpec.lfn]['scope']
            datasetName = fileAttrs[fileSpec.lfn]['dataset']
            srcPath = fileAttrs[fileSpec.lfn]['path']
            dstPath = mover_utils.construct_file_path(self.srcBasePath, datasetName, scope, fileSpec.lfn)
            # remove
            if os.path.exists(dstPath):
                os.remove(dstPath)
            # copy
            tmpLog.debug('src={srcPath} dst={dstPath}'.format(srcPath=srcPath, dstPath=dstPath))
            shutil.copyfile(srcPath, dstPath)
            # collect files
            tmpFile = dict()
            tmpFile['scope'] = scope
            tmpFile['name'] = fileSpec.lfn
            if fileSpec.fileType not in files:
                files[fileSpec.fileType] = []
            files[fileSpec.fileType].append(tmpFile)
        # loop over all file types to be registered to rucio
        rucioAPI = RucioClient()
        for fileType, fileList in files.iteritems():
            # set destination RSE
            if fileType == 'es_output':
                dstRSE = self.dstRSE_ES
            elif fileType == 'output':
                dstRSE = self.dstRSE_Out
            elif fileType == 'log':
                dstRSE = self.dstRSE_Log
            else:
                errMsg = 'unsupported file type {0}'.format(fileType)
                tmpLog.error(errMsg)
                return (False, errMsg)
            # skip if destination is None
            if dstRSE is None:
                continue
            # make datasets if missing
            if fileType not in transferDatasets:
                try:
                    tmpScope = 'panda'
                    tmpDS = 'panda.harvester_stage_out.{0}'.format(str(uuid.uuid4()))
                    rucioAPI.add_dataset(tmpScope, tmpDS,
                                         meta={'hidden': True},
                                         lifetime=7*24*60*60,
                                         files=fileList,
                                         rse=self.srcRSE
                                         )
                    transferDatasets[fileType] = tmpDS
                    # add rule
                    tmpDID = '{0}:{1}'.format(tmpScope, tmpDS)
                    tmpRet = rucioAPI.add_replication_rule([tmpDID], 1, dstRSE,
                                                           lifetime=7*24*60*60
                                                           )
                    transferIDs[fileType] = tmpRet['rule_id']
                except:
                    errMsg = core_utils.dump_error_message(tmpLog)
                    return (False, errMsg)
            else:
                # add files to existing dataset
                try:
                    tmpScope = 'panda'
                    tmpDS = transferDatasets[fileType]
                    rucioAPI.add_files_to_dataset(tmpScope, tmpDS, fileList, self.srcRSE)
                except:
                    errMsg = core_utils.dump_error_message(tmpLog)
                    return (False, errMsg)
        # set transfer datasets and rules
        for fileSpec in jobspec.outFiles:
            # skip zipped files
            if fileSpec.zipFileID is not None:
                continue
            # skip already done
            if fileSpec.status in ['finished', 'failed']:
                continue
            # skip if already processed
            if 'transferDataset' in fileSpec.fileAttributes:
                continue
            # no destination
            if fileSpec.fileType not in transferDatasets:
                fileSpec.status = 'finished'
                continue
            # set dataset
            fileSpec.fileAttributes['transferDataset'] = transferDatasets[fileSpec.fileType]
            # set rule
            fileSpec.fileAttributes['transferID'] = transferIDs[fileSpec.fileType]
            # force update
            fileSpec.force_update('fileAttributes')
        # return
        tmpLog.debug('done')
        return (True, '')

    # zip output files
    def zip_output(self, jobspec):
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID))
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
