import os
import uuid
import time
import multiprocessing
from concurrent.futures import ThreadPoolExecutor as Pool

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase


# base class for stager plugin
class BaseStager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.zipDir = "${SRCDIR}"
        self.zip_tmp_log = None
        self.zip_jobSpec = None
        PluginBase.__init__(self, **kwarg)

    # zip output files
    def simple_zip_output(self, jobspec, tmp_log):
        tmp_log.debug('start')
        self.zip_tmp_log = tmp_log
        self.zip_jobSpec = jobspec
        argDictList = []
        try:
            for fileSpec in jobspec.outFiles:
                if self.zipDir == "${SRCDIR}":
                    # the same directory as src
                    zipDir = os.path.dirname(next(iter(fileSpec.associatedFiles)).path)
                elif self.zipDir == "${WORKDIR}":
                    # work dir
                    workSpec = jobspec.get_workspec_list()[0]
                    zipDir = workSpec.get_access_point()
                else:
                    zipDir = self.zipDir
                zipPath = os.path.join(zipDir, fileSpec.lfn)
                argDict = dict()
                argDict['zipPath'] = zipPath
                argDict['associatedFiles'] = []
                for assFileSpec in fileSpec.associatedFiles:
                    if os.path.exists(assFileSpec.path):
                        argDict['associatedFiles'].append(assFileSpec.path)
                    else:
                        assFileSpec.status = 'failed'
                argDictList.append(argDict)
            # parallel execution
            try:
                nThreadsForZip = harvester_config.stager.nThreadsForZip
            except Exception:
                nThreadsForZip = multiprocessing.cpu_count()
            with Pool(max_workers=nThreadsForZip) as pool:
                retValList = pool.map(self.make_one_zip, argDictList)
                # check returns
                for fileSpec, retVal in zip(jobspec.outFiles, retValList):
                    tmpRet, errMsg, fileInfo = retVal
                    if tmpRet is True:
                        # set path
                        fileSpec.path = fileInfo['path']
                        fileSpec.fsize = fileInfo['fsize']
                        fileSpec.chksum = fileInfo['chksum']
                        msgStr = 'fileSpec.path - {0}, fileSpec.fsize - {1}, fileSpec.chksum(adler32) - {2}' \
                            .format(fileSpec.path, fileSpec.fsize, fileSpec.chksum)
                        tmp_log.debug(msgStr)
                    else:
                        tmp_log.error('got {0} with {1} when zipping {2}'.format(tmpRet, errMsg, fileSpec.lfn))
                        return tmpRet, 'failed to zip with {0}'.format(errMsg)
        except Exception:
            errMsg = core_utils.dump_error_message(tmp_log)
            return False, 'failed to zip with {0}'.format(errMsg)
        tmp_log.debug('done')
        return True, ''

    # make one zip file
    def make_one_zip(self, arg_dict):
        try:
            zipPath = arg_dict['zipPath']
            lfn = os.path.basename(zipPath)
            self.zip_tmp_log.debug('{0} start zipPath={1} with {2} files'.format(lfn, zipPath,
                                                                                 len(arg_dict['associatedFiles'])))
            # make zip if doesn't exist
            if not os.path.exists(zipPath):
                # tmp file names
                tmpZipPath = zipPath + '.' + str(uuid.uuid4())
                tmpZipPathIn = tmpZipPath + '.in'
                with open(tmpZipPathIn, "w") as f:
                    for associatedFile in arg_dict['associatedFiles']:
                        f.write("{0}\n".format(associatedFile))
                # make command
                com = 'tar -c -f {0} -T {1} '.format(tmpZipPath, tmpZipPathIn)
                com += "--transform 's/.*\///' "
                # execute
                p = subprocess.Popen(com,
                                     shell=True,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
                stdOut, stdErr = p.communicate()
                retCode = p.returncode
                if retCode != 0:
                    msgStr = 'failed to make zip for {0} with {1}:{2}'.format(lfn, stdOut, stdErr)
                    self.zip_tmp_log.error(msgStr)
                    return None, msgStr, {}
                # avoid overwriting
                lockName = 'zip.lock.{0}'.format(lfn)
                lockInterval = 60
                tmpStat = False
                # get lock
                for i in range(lockInterval):
                    tmpStat = self.dbInterface.get_object_lock(lockName, lock_interval=lockInterval)
                    if tmpStat:
                        break
                    time.sleep(1)
                # failed to lock
                if not tmpStat:
                    msgStr = 'failed to lock for {0}'.format(lfn)
                    self.zip_tmp_log.error(msgStr)
                    return None, msgStr
                if not os.path.exists(zipPath):
                    os.rename(tmpZipPath, zipPath)
                # release lock
                self.dbInterface.release_object_lock(lockName)
            # make return
            fileInfo = dict()
            fileInfo['path'] = zipPath
            # get size
            statInfo = os.stat(zipPath)
            fileInfo['fsize'] = statInfo.st_size
            fileInfo['chksum'] = core_utils.calc_adler32(zipPath)
        except Exception:
            errMsg = core_utils.dump_error_message(self.zip_tmp_log)
            return False, 'failed to zip with {0}'.format(errMsg)
        self.zip_tmp_log.debug('{0} done'.format(lfn))
        return True, '', fileInfo
