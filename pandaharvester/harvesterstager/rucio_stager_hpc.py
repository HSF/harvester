import os
import sys
import shutil
import subprocess
import time
import os.path
import zipfile
import uuid

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermover import mover_utils

from rucio.client import Client as RucioClient
from rucio.common.exception import RuleNotFound

# logger
baseLogger = core_utils.setup_logger()


# plugin for stage-out with Rucio on an HPC site that must copy output elsewhere
class RucioStagerHPC(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        if not hasattr(self, 'scopeForTmp'):
            self.scopeForTmp = 'panda'

    # check status
    def check_status(self, jobspec):
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID))
        tmpLog.debug('start')
        return (True,'')

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID))
        tmpLog.debug('start')
        # loop over all files
        lifetime = 7*24*60*60
        allChecked = True
        ErrMsg = 'These files failed to upload : '
        zip_datasetName = 'harvester_stage_out.{0}'.format(str(uuid.uuid4()))
        fileAttrs = jobspec.get_output_file_attributes()
        for fileSpec in jobspec.outFiles:
           # skip already done                                                                                                                                               
            if fileSpec.status in ['finished', 'failed']:
                continue
            # set destination RSE
            if fileSpec.fileType in ['es_output', 'zip_output', 'output']:
                dstRSE = self.dstRSE_Out
            elif fileSpec.fileType == 'log':
                dstRSE = self.dstRSE_Log
            else:
                errMsg = 'unsupported file type {0}'.format(fileSpec.fileType)
                tmpLog.error(errMsg)
                return (False, errMsg)
            # skip if destination is None
            if dstRSE is None:
                continue
            
            # get/set scope and dataset name
            if fileSpec.fileType != 'zip_output':
                scope = fileAttrs[fileSpec.lfn]['scope']
                datasetName = fileAttrs[fileSpec.lfn]['dataset']
            else:
                # use panda scope for zipped files
                scope = self.scopeForTmp
                datasetName = zip_datasetName


            # for now mimic behaviour and code of pilot v2 rucio copy tool (rucio download) change when needed

            executable = ['/usr/bin/env',
                          'rucio', '-v', 'upload']
            executable += [ '--lifetime',('%d' %lifetime)]
            executable += [ '--rse',dstRSE]
            executable += [ '--scope',scope]
            executable += [('%s:%s' %(scope,datasetName))]
            executable += [('%s' %fileSpec.path)]

            #print executable 

            tmpLog.debug('rucio upload command: {0} '.format(executable))

            process = subprocess.Popen(executable,
                                       bufsize=-1,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)

            while True:
                time.sleep(0.1)
                exit_code = process.poll()
                if exit_code is not None:
                    if exit_code == 0:
                        fileSpec.status = 'finished'
                    else:
                        fileSpec.status = 'failed'
                        ErrMsg = ErrMsg + (" %s " %fileSpec.lfn)
                        allChecked = False                        
                    break
                else:
                    continue

            response = process.communicate()
            #print response
            if fileSpec.status == 'failed' : 
                ErrMsg = ErrMsg + response[1].strip("\n")
                tmpLog.debug(ErrMsg)

            # force update
            fileSpec.force_update('status')

            tmpLog.debug('Result of rucio upload command: {0}'.format(fileSpec.status))

            #        errMsg = core_utils.dump_error_message(tmpLog)

        # return
        tmpLog.debug('done')
        if allChecked:
            return True, ''
        else:
            return False, ErrMsg


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
                # set the file type
                fileSpec.fileType = 'zip_output'
                # set path
                fileSpec.path = zipPath
                # get size
                statInfo = os.stat(zipPath)
                fileSpec.fsize = statInfo.st_size
                # added empty attributes
                fileSpec.fileAttributes = dict()
                tmpLog.debug('zipped {0}'.format(zipPath))
        except:
            errMsg = core_utils.dump_error_message(tmpLog)
            return False, 'failed to zip with {0}'.format(errMsg)
        tmpLog.debug('done')
        return True, ''
