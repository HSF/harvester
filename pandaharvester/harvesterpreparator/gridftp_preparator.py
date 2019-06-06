import os
import tempfile
try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermover import mover_utils

# logger
baseLogger = core_utils.setup_logger('gridftp_preparator')


# preparator plugin with GridFTP
"""
  -- Example of plugin config
    "preparator": {
        "name": "GridFtpPreparator",
        "module": "pandaharvester.harvesterpreparator.gridftp_preparator",
        # base path for source GridFTP server 
        "srcBasePath": "gsiftp://dcdum02.aglt2.org/pnfs/aglt2.org/atlasdatadisk/rucio/",
        # base path for destination GridFTP server
        "dstBasePath": "gsiftp://dcgftp.usatlas.bnl.gov:2811/pnfs/usatlas.bnl.gov/atlasscratchdisk/rucio/",
        # base path for local access to the copied files
        "localBasePath": "/data/rucio",
        # max number of attempts
        maxAttempts: 3,
        # options for globus-url-copy
        "gulOpts": "-cred /tmp/x509_u1234 -sync -sync-level 3 -verify-checksum -v"
    }
"""
class GridFtpPreparator(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.gulOpts = None
        self.maxAttempts = 3
        PluginBase.__init__(self, **kwarg)

    # trigger preparation
    def trigger_preparation(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                  method_name='trigger_preparation')
        tmpLog.debug('start')
        # loop over all inputs
        inFileInfo = jobspec.get_input_file_attributes()
        gucInput = None
        for tmpFileSpec in jobspec.inFiles:
            # construct source and destination paths
            srcPath = mover_utils.construct_file_path(self.srcBasePath, inFileInfo[tmpFileSpec.lfn]['scope'],
                                                      tmpFileSpec.lfn)
            dstPath = mover_utils.construct_file_path(self.dstBasePath, inFileInfo[tmpFileSpec.lfn]['scope'],
                                                      tmpFileSpec.lfn)
            # local access path
            accPath = mover_utils.construct_file_path(self.localBasePath, inFileInfo[tmpFileSpec.lfn]['scope'],
                                                      tmpFileSpec.lfn)
            # check if already exits
            if os.path.exists(accPath):
                # calculate checksum
                checksum = core_utils.calc_adler32(accPath)
                checksum = 'ad:{0}'.format(checksum)
                if checksum == inFileInfo[tmpFileSpec.lfn]['checksum']:
                    continue
            # make directories if needed
            if not os.path.isdir(os.path.dirname(accPath)):
                os.makedirs(os.path.dirname(accPath))
            # make input for globus-url-copy
            if gucInput is None:
                gucInput = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_guc_in.tmp')
            gucInput.write("{0} {1}\n".format(srcPath, dstPath))
            tmpFileSpec.attemptNr += 1
        # nothing to transfer
        if gucInput is None:
            tmpLog.debug('done with no transfers')
            return True, ''
        # transfer
        tmpLog.debug('execute globus-url-copy')
        gucInput.close()
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
            for tmpFileSpec in jobspec.inFiles:
                if tmpFileSpec.attemptNr >= self.maxAttempts:
                    errMsg = 'gave up due to max attempts'
                    tmpLog.error(errMsg)
                    return (False, errMsg)
            return None, errMsg

    # check status
    def check_stage_in_status(self, jobspec):
        return True, ''

    # resolve input file paths
    def resolve_input_paths(self, jobspec):
        #  input files
        inFileInfo = jobspec.get_input_file_attributes()
        pathInfo = dict()
        for tmpFileSpec in jobspec.inFiles:
            accPath = mover_utils.construct_file_path(self.localBasePath, inFileInfo[tmpFileSpec.lfn]['scope'],
                                                      tmpFileSpec.lfn)
            pathInfo[tmpFileSpec.lfn] = {'path': accPath}
        jobspec.set_input_file_paths(pathInfo)
        return True, ''
