import os
import sys
import shutil
import os.path
import zipfile
import uuid
from future.utils import iteritems

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermover import mover_utils

from rucio.client import Client as RucioClient
from rucio.common.exception import RuleNotFound

# logger
baseLogger = core_utils.setup_logger('rucio_stager')


# plugin for stage-out with Rucio
class RucioStager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        if not hasattr(self, 'scopeForTmp'):
            self.scopeForTmp = 'panda'

    # check status
    def check_status(self, jobspec):
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                        method_name='check_status')
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
                try:
                    rucioAPI = RucioClient()
                    ruleInfo = rucioAPI.get_replication_rule(transferID)
                    tmpTransferStatus = ruleInfo['state']
                    tmpLog.debug('got state={0} for rule={1}'.format(tmpTransferStatus, transferID))
                except RuleNotFound:
                    tmpLog.error('rule {0} not found'.format(transferID))
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
                tmpTransferStatus = 'OK'
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
        tmpLog = core_utils.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                        method_name='trigger_stage_out')
        tmpLog.debug('start')
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
            if fileSpec.fileType == ['es_output', 'zip_output']:
                fileSpec.objstoreID = self.objStoreID_ES
            # make path where file is copied for transfer
            if fileSpec.fileType != 'zip_output':
                scope = fileAttrs[fileSpec.lfn]['scope']
                datasetName = fileAttrs[fileSpec.lfn]['dataset']
            else:
                # use panda scope for zipped files
                scope = self.scopeForTmp
                datasetName = 'dummy'
            srcPath = fileSpec.path
            dstPath = mover_utils.construct_file_path(self.srcBasePath, scope, fileSpec.lfn)
            # remove
            if os.path.exists(dstPath):
                os.remove(dstPath)
            # copy
            tmpLog.debug('copy src={srcPath} dst={dstPath}'.format(srcPath=srcPath, dstPath=dstPath))
            dstDir = os.path.dirname(dstPath)
            if not os.path.exists(dstDir):
                os.makedirs(dstDir)
            shutil.copyfile(srcPath, dstPath)
            # collect files
            tmpFile = dict()
            tmpFile['scope'] = scope
            tmpFile['name'] = fileSpec.lfn
            tmpFile['bytes'] = fileSpec.fsize
            if fileSpec.fileType not in files:
                files[fileSpec.fileType] = []
            files[fileSpec.fileType].append(tmpFile)
        # loop over all file types to be registered to rucio
        rucioAPI = RucioClient()
        for fileType, fileList in iteritems(files):
            # set destination RSE
            if fileType in ['es_output', 'zip_output']:
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
                    tmpScope = self.scopeForTmp
                    tmpDS = 'panda.harvester_stage_out.{0}'.format(str(uuid.uuid4()))
                    rucioAPI.add_dataset(tmpScope, tmpDS,
                                         meta={'hidden': True},
                                         lifetime=7*24*60*60,
                                         files=fileList,
                                         rse=self.srcRSE
                                         )
                    transferDatasets[fileType] = tmpDS
                    # add rule
                    tmpDID = dict()
                    tmpDID['scope'] = tmpScope
                    tmpDID['name'] = tmpDS
                    tmpRet = rucioAPI.add_replication_rule([tmpDID], 1, dstRSE,
                                                           lifetime=7*24*60*60
                                                           )
                    tmpTransferIDs = tmpRet[0]
                    transferIDs[fileType] = tmpTransferIDs
                    tmpLog.debug('register dataset {0} with rule {1}'.format(tmpDS, str(tmpTransferIDs)))
                except:
                    errMsg = core_utils.dump_error_message(tmpLog)
                    return (False, errMsg)
            else:
                # add files to existing dataset
                try:
                    tmpScope = self.scopeForTmp
                    tmpDS = transferDatasets[fileType]
                    rucioAPI.add_files_to_dataset(tmpScope, tmpDS, fileList, self.srcRSE)
                    tmpLog.debug('added files to {0}'.format(tmpDS))
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
                # added empty attributes
                if fileSpec.fileAttributes is None:
                    fileSpec.fileAttributes = dict()
                tmpLog.debug('zipped {0}'.format(zipPath))
        except:
            errMsg = core_utils.dump_error_message(tmpLog)
            return False, 'failed to zip with {0}'.format(errMsg)
        tmpLog.debug('done')
        return True, ''
