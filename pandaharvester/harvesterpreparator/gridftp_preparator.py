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
class GridFtpPreparator(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.parallel = None
        self.stripe = None
        self.maxAttempts = 3
        self.cred = None
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
            srcPath = mover_utils.construct_file_path(self.srcBasePath, inFileInfo['scope'], tmpFileSpec.lfn)
            dstPath = mover_utils.construct_file_path(self.dstBasePath, inFileInfo['scope'], tmpFileSpec.lfn)
            # local access path
            accPath = mover_utils.construct_file_path(self.localBasePath, inFileInfo['scope'], tmpFileSpec.lfn)
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
            accPath = mover_utils.construct_file_path(self.localBasePath, inFileInfo['scope'], tmpFileSpec.lfn)
            pathInfo[tmpFileSpec.lfn] = {'path': accPath}
        jobspec.set_input_file_paths(pathInfo)
        return True, ''
