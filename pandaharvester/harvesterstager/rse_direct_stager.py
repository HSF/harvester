import os
import zipfile

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger('rse_direct_stager')

# stager plugin with RSE + no data motion
class RseDirectStager(PluginBase):
    """In the workflow for RseDirectStager, workers directly upload output files to RSE
    and thus there is no data motion in Harvester."""
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check status
    def check_status(self, jobspec):
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            fileSpec.status = 'finished'
        return True, ''

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        return True, ''

    # zip output files
    def zip_output(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                  method_name='zip_output')
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
                        zf.write(assFileSpec.path,os.path.basename(assFileSpec.path))
                # set path
                fileSpec.path = zipPath
                # get size
                statInfo = os.stat(zipPath)
                fileSpec.fsize = statInfo.st_size
                # added empty attributes
                if fileSpec.fileAttributes is None:
                    fileSpec.fileAttributes = dict()
                tmpLog.debug('zipped {0}'.format(zipPath))
        except:
            errMsg = core_utils.dump_error_message(tmpLog)
            return False, 'failed to zip with {0}'.format(errMsg)
        tmpLog.debug('done')
        return True, ''
