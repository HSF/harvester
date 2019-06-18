import os
import tempfile
try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

import requests
import requests.exceptions

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermover import mover_utils
from pandaharvester.harvesterconfig import harvester_config

# logger
baseLogger = core_utils.setup_logger('analysis_aux_preparator')


# preparator plugin for analysis auxiliary inputs
class AnalysisAuxPreparator(PluginBase):
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
        allDone = True
        for tmpFileSpec in jobspec.inFiles:
            # local access path
            url = tmpFileSpec.url
            accPath = self.make_local_access_path(tmpFileSpec.scope, tmpFileSpec.lfn)
            # check if already exits
            if os.path.exists(accPath):
                    continue
            # make directories if needed
            if not os.path.isdir(os.path.dirname(accPath)):
                os.makedirs(os.path.dirname(accPath))
            # get
            return_code = 1
            if url.startswith('http'):
                try:
                    tmpLog.debug('getting via http from {0} to {1}'.format(url, accPath))
                    res = requests.get(url, timeout=180, verify=False)
                    if res.status_code == 200:
                        with open(accPath, 'w') as f:
                            f.write(res.content)
                        return_code = 0
                    else:
                        errMsg = 'failed to get {0} with StatusCode={1} {2}'.format(url, res.status_code, res.text)
                        tmpLog.error(errMsg)
                except requests.exceptions.ReadTimeout:
                    tmpLog.error('read timeout when getting data from {0}'.format(url))
                except Exception:
                    core_utils.dump_error_message(tmpLog)
            elif url.startswith('docker'):
                args = ['docker', 'save', '-o', accPath, url.split('://')[-1]]
                try:
                    tmpLog.debug('executing ' + ' '.join(args))
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
            else:
                tmpLog.error('unsupported protocol in {0}'.format(url))
            if return_code != 0:
                allDone = False
        if allDone:
            tmpLog.debug('succeeded')
            return True, ''
        else:
            errMsg = 'failed'
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
        pathInfo = dict()
        for tmpFileSpec in jobspec.inFiles:
            url = tmpFileSpec.lfn
            accPath = self.make_local_access_path(tmpFileSpec.scope, tmpFileSpec.lfn)
            pathInfo[tmpFileSpec.lfn] = {'path': accPath}
        jobspec.set_input_file_paths(pathInfo)
        return True, ''

    # make local access path
    def make_local_access_path(self, scope, lfn):
        return mover_utils.construct_file_path(self.localBasePath, scope, lfn)
