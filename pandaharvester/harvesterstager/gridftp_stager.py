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

# logger
baseLogger = core_utils.setup_logger('gridftp_stager')


# stager plugin with GridFTP
"""
  -- example of plugin config
  "stager":{
        "name":"GridFtpStager",
        "module":"pandaharvester.harvesterstager.gridftp_stager",
        "objstoreID_ES":117,
        # base path for local access to the files to be copied
        "srcOldBasePath":"/tmp/workdirs",
        # base path for access through source GridFTP server to the files to be copied
        "srcNewBasePath":"gsiftp://dcdum02.aglt2.org/pnfs/aglt2.org",
        # base path for destination GridFTP server
        "dstBasePath":"gsiftp://dcgftp.usatlas.bnl.gov:2811/pnfs/usatlas.bnl.gov/atlasscratchdisk/rucio",
        # max number of attempts
        maxAttempts: 3,
        # options for globus-url-copy
        "gulOpts":"-verify-checksum -v"
    }
"""
class GridFtpStager(BaseStager):
    # constructor
    def __init__(self, **kwarg):
        self.scopeForTmp = 'transient'
        self.pathConvention = 1000
        self.objstoreID = None
        self.gulOpts = None
        self.maxAttempts = 3
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
                scope = fileSpec.fileAttributes.get('scope')
                if scope is None:
                    scope = fileSpec.scope
            # construct source and destination paths
            srcPath = re.sub(self.srcOldBasePath, self.srcNewBasePath, fileSpec.path)
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
        gucInput.close()
        args = ['globus-url-copy', '-f', gucInput.name, '-cd']
        if self.gulOpts is not None:
            args += self.gulOpts.split()
        try:
            tmpLog.debug('execute globus-url-copy' + ' '.join(args))
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            return_code = p.returncode
            if stdout is not None:
                stdout = stdout.replace('\n', ' ')
            if stderr is not None:
                stderr = stderr.replace('\n', ' ')
            tmpLog.debug("stdout: %s" % stdout)
            tmpLog.debug("stderr: %s" % stderr)
        except Exception:
            core_utils.dump_error_message(tmpLog)
            return_code = 1
        os.remove(gucInput.name)
        if return_code == 0:
            tmpLog.debug('succeeded')
            return True, ''
        else:
            errMsg = 'failed with {0}'.format(return_code)
            tmpLog.error(errMsg)
            # check attemptNr
            for fileSpec in jobspec.inFiles:
                if fileSpec.attemptNr >= self.maxAttempts:
                    errMsg = 'gave up due to max attempts'
                    tmpLog.error(errMsg)
                    return (False, errMsg)
            return None, errMsg
