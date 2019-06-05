import os
import re
import tempfile
try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

from pandaharvester.harvestercore import core_utils
from .base_stager import BaseStager
from pandaharvester.harvestermover import mover_utils

import uuid

# logger
baseLogger = core_utils.setup_logger('gridftp_stager')


# stager plugin with GridFTP
class GridFtpStager(BaseStager):
    # constructor
    def __init__(self, **kwarg):
        self.scopeForTmp = 'panda'
        self.pathConvention = None
        self.objstoreID = None
        self.parallel = None
        self.stripe = None
        self.maxAttempts = 3
        self.cred = None
        BaseStager.__init__(self, **kwarg)

    # check status
    def check_stage_out_status(self, jobspec):
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            fileSpec.status = 'finished'
            fileSpec.pathConvention = self.pathConvention
            fileSpec.objstoreID = self.objstoreID
        return True, ''

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                  method_name='trigger_stage_out')
        tmpLog.debug('start')
        # loop over all files
        gucInput = None
        for fileSpec in jobspec.outFiles:
            # skip if already done
            if fileSpec.status in ['finished', 'failed']:
                continue
            # scope
            if fileSpec.fileType in ['es_output', 'zip_output']:
                scope = self.scopeForTmp
            else:
                scope = fileSpec.fileAttributes['scope']
            # construct source and destination paths
            srcPath = re.sub(fileSpec.path, self.srcOldPath, self.srcNewPath)
            dstPath = mover_utils.construct_file_path(self.dstBasePath, scope, fileSpec.lfn)
            # make input for globus-url-copy
            if gucInput is None:
                gucInput = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_guc_out.tmp')
            gucInput.write("{0} {1}\n".format(srcPath, dstPath))
            fileSpec.attemptNr += 1
        # nothing to transfer
        if gucInput is None:
            tmpLog.debug('done with no transfers')
            return True, ''
        # transfer
        tmpLog.debug('execute globus-url-copy')
        gucInput.close()
        args = ['globus-url-copy', '-f', gucInput.name]
        if self.parallel is not None:
            args += ['-p', self.parallel]
        if self.stripe is not None:
            args += ['-stripe', self.stripe]
        if self.cred is not None:
            args += ['-cred', self.cred]
        p = subprocess.Popen(args)
        stdout, stderr = p.communicate()
        os.remove(gucInput.name)
        tmpLog.debug("stdout: %s" % stdout)
        tmpLog.debug("stderr: %s" % stderr)
        if p.returncode == 0:
            tmpLog.debug('succeeded')
            return True, ''
        else:
            errMsg = 'failed with {0}'.format(p.returncode)
            tmpLog.error(errMsg)
            # check attemptNr
            for fileSpec in jobspec.inFiles:
                if fileSpec.attemptNr >= self.maxAttempts:
                    errMsg = 'gave up due to max attempts'
                    tmpLog.error(errMsg)
                    return (False, errMsg)
            return None, errMsg
