import os
import uuid
import time
import tarfile

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase


# base class for stager plugin
class BaseStager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.zipDir = "${SRCDIR}"
        PluginBase.__init__(self, **kwarg)

    # zip output files
    def simple_zip_output(self, jobspec, tmp_log):
        tmp_log.debug('start')
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
                msgStr = 'self.zipDir - {0} zipDir - {1} fileSpec.lfn - {2} zipPath - {3}' \
                    .format(self.zipDir, zipDir, fileSpec.lfn, zipPath)
                tmp_log.debug(msgStr)
                # make zip if doesn't exist
                if not os.path.exists(zipPath):
                    tmpZipPath = zipPath + '.' + str(uuid.uuid4())
                    with tarfile.open(tmpZipPath, "w") as zf:
                        for assFileSpec in fileSpec.associatedFiles:
                            zf.add(assFileSpec.path, os.path.basename(assFileSpec.path))
                    # avoid overwriting
                    lockName = 'zip.lock.{0}'.format(fileSpec.lfn)
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
                        msgStr = 'failed to get zip lock for {0}'.format(fileSpec.lfn)
                        tmp_log.error(msgStr)
                        return None, msgStr
                    if not os.path.exists(zipPath):
                        os.rename(tmpZipPath, zipPath)
                    # release lock
                    self.dbInterface.release_object_lock(lockName)
                # set path
                fileSpec.path = zipPath
                # get size
                statInfo = os.stat(zipPath)
                fileSpec.fsize = statInfo.st_size
                fileSpec.chksum = core_utils.calc_adler32(zipPath)
                msgStr = 'fileSpec.path - {0}, fileSpec.fsize - {1}, fileSpec.chksum(adler32) - {2}'\
                    .format(fileSpec.path, fileSpec.fsize, fileSpec.chksum)
                tmp_log.debug(msgStr)
        except Exception:
            errMsg = core_utils.dump_error_message(tmp_log)
            return False, 'failed to zip with {0}'.format(errMsg)
        tmp_log.debug('done')
        return True, ''
