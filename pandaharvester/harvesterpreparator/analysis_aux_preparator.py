import os
import stat
import shutil
try:
    import subprocess32 as subprocess
except Exception:
    import subprocess

import requests
import requests.exceptions

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermover import mover_utils

# logger
baseLogger = core_utils.setup_logger('analysis_aux_preparator')


# preparator plugin for analysis auxiliary inputs
class AnalysisAuxPreparator(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.containerRuntime = None
        self.maxAttempts = 3
        PluginBase.__init__(self, **kwarg)

    # trigger preparation
    def trigger_preparation(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                  method_name='trigger_preparation')
        tmpLog.debug('start')
        tmpLog.debug("from queueconfig file - containerRuntime : {0}".format(self.containerRuntime))
        tmpLog.debug("from queueconfig file - localContainerPath: {0}".format(self.localContainerPath))
        # loop over all inputs
        allDone = True
        for tmpFileSpec in jobspec.inFiles:
            # local access path
            url = tmpFileSpec.url
            accPath = self.make_local_access_path(tmpFileSpec.scope, tmpFileSpec.lfn)
            accPathTmp = accPath + '.tmp'
            tmpLog.debug('url : {0} accPath : {1}'.format(url,accPath))
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
                    tmpLog.debug('getting via http from {0} to {1}'.format(url, accPathTmp))
                    res = requests.get(url, timeout=180, verify=False)
                    if res.status_code == 200:
                        with open(accPathTmp, 'wb') as f:
                            f.write(res.content)
                        tmpLog.debug('Successfully fetched file - {0}'.format(accPathTmp))
                        return_code = 0
                    else:
                        errMsg = 'failed to get {0} with StatusCode={1} {2}'.format(url, res.status_code, res.text)
                        tmpLog.error(errMsg)
                except requests.exceptions.ReadTimeout:
                    tmpLog.error('read timeout when getting data from {0}'.format(url))
                except Exception:
                    core_utils.dump_error_message(tmpLog)
            elif url.startswith('docker'):
                if self.containerRuntime is None:
                    tmpLog.debug('container downloading is disabled')
                    continue
                if self.containerRuntime == 'docker':
                    args = ['docker', 'save', '-o', accPathTmp, url.split('://')[-1]]
                    return_code = self.make_container(tmpLog,args)
                elif self.containerRuntime == 'singularity':
                    args = ['singularity', 'build', '--sandbox', accPathTmp, url ]
                    return_code = self.make_container(tmpLog,args)
                elif self.containerRuntime == 'Summit_singularity':
                    return_code = self.make_container_script(tmpLog, accPathTmp, url)
                else:
                    tmpLog.error('unsupported container runtime : {0}'.format(self.containerRuntime))
                #
            elif url.startswith('/'):
                try:
                    shutil.copyfile(url, accPathTmp)
                    return_code = 0
                except Exception:
                    core_utils.dump_error_message(tmpLog)
            else:
                tmpLog.error('unsupported protocol in {0}'.format(url))
            # remove empty files
            if os.path.exists(accPathTmp) and os.path.getsize(accPathTmp) == 0:
                return_code = 1
                tmpLog.debug('remove empty file - {0}'.format(accPathTmp))
                try:
                    os.remove(accPathTmp)
                except Exception:
                    core_utils.dump_error_message(tmpLog)
            # rename
            if return_code == 0:
                try:
                    os.rename(accPathTmp, accPath)
                except Exception:
                    return_code = 1
                    core_utils.dump_error_message(tmpLog)
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

    # execute commands to make container in subprocess
    def make_container(self, tmpLog, args):
        return_code = 1
        try:
            tmpLog.debug('executing ' + ' '.join(args))
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            return_code = p.returncode
            if stdout is not None:
                stdout = stdout.replace('\n', ' ')
            if stderr is not None:
                stderr = stderr.replace('\n', ' ')
            tmpLog.debug("stdout: {0}".format(stdout))
            tmpLog.debug("stderr: [0}".format(stderr))
        except Exception:
            core_utils.dump_error_message(tmpLog)
        return return_code

    # create file to be used to create container
    def make_container_script(self, tmpLog, accPathTmp, url):
        return_code = 1
        # extract container name from url
        container_name = url.rsplit('/',1)[1]
        # construct path to container
        containerPath = "{basePath}/{name}".format(basePath=self.localContainerPath, name=container_name)
        tmpLog.debug("accPathTmp : {0} url : {1} containerPath : {2}".format(accPathTmp,url,containerPath))
        # check if container already exits
        if os.path.exists(containerPath):
            return_code = 0
        else:
            try:
                # make directories if needed
                if not os.path.isdir(containerPath):
                    os.makedirs(containerPath)
                if not os.path.isdir(os.path.dirname(accPathTmp)):
                    os.makedirs(os.path.dirname(accPathTmp))
                # now create the command file for creating Singularity sandbox container
                with open(accPathTmp, 'w') as f:
                    f.write("#!/bin/sh\n")
                    f.write("# this file creates the Singularity sandbox container {0}\n".format(containerPath))
                    f.write("set -x \n")
                    f.write("singularity build --force --sandbox {path} {url}\n".format(path=containerPath,url=url))
                    f.write("set +x \n")
                # change permissions on script to executable
                st = os.stat(accPathTmp)
                os.chmod(accPathTmp, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH )
                tmpLog.debug('Successfully fetched file - {0}'.format(accPathTmp))
                return_code = 0
            except Exception:
                core_utils.dump_error_message(tmpLog)
        return return_code
