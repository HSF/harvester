import os
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
                else:
                    zipDir = self.zipDir
                zipPath = os.path.join(zipDir, fileSpec.lfn)
                msgStr = 'self.zipDir - {0} zipDir - {1} fileSpec.lfn - {2} zipPath - {3}' \
                    .format(self.zipDir, zipDir, fileSpec.lfn, zipPath)
                tmp_log.debug(msgStr)
                # remove zip file just in case
                try:
                    os.remove(zipPath)
                    msgStr = 'removed file {}'.format(zipPath)
                    tmp_log.debug(msgStr)
                except Exception:
                    pass
                # make tar file
                with tarfile.open(zipPath, "w") as zf:
                    for assFileSpec in fileSpec.associatedFiles:
                        zf.add(assFileSpec.path, os.path.basename(assFileSpec.path))
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
