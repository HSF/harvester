from pandaharvester.harvestercore import core_utils
from .base_stager import BaseStager

import uuid

# logger
_logger = core_utils.setup_logger("dummy_stager")


# dummy plugin for stager
class DummyStager(BaseStager):
    # constructor
    def __init__(self, **kwarg):
        BaseStager.__init__(self, **kwarg)

    # check status
    def check_stage_out_status(self, jobspec):
        """Check the status of stage-out procedure. If staging-out is done synchronously in trigger_stage_out
        this method should always return True.
        Output files are available through jobspec.get_outfile_specs(skip_done=False) which gives
        a list of FileSpecs not yet done.
        FileSpec.attemptNr shows how many times the transfer was checked for the file.
        If the file was successfully transferred, status should be set to 'finished'.
        Or 'failed', if the file failed to be transferred. Once files are set to 'finished' or 'failed',
        jobspec.get_outfile_specs(skip_done=False) ignores them.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True: transfer success, False: fatal transfer failure,
                 None: on-going or temporary failure) and error dialog
        :rtype: (bool, string)
        """
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            fileSpec.status = "finished"
        return True, ""

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        """Trigger the stage-out procedure for the job.
        Output files are available through jobspec.get_outfile_specs(skip_done=False) which gives
        a list of FileSpecs not yet done.
        FileSpec.attemptNr shows how many times transfer was tried for the file so far.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True: success, False: fatal failure, None: temporary failure)
                 and error dialog
        :rtype: (bool, string)
        """
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            # fileSpec.objstoreID = 123
            # fileSpec.fileAttributes['guid']
            pass
        return True, ""

    # zip output files
    def zip_output(self, jobspec):
        """OBSOLETE : zip functions should be implemented in zipper plugins.
        Zip output files. This method loops over jobspec.outFiles, which is a list of zip file's FileSpecs,
        to make a zip file for each zip file's FileSpec. FileSpec.associatedFiles is a list of FileSpecs of
        associated files to be zipped. The path of each associated file is available in associated
        file's FileSpec.path. Once zip files are made, their FileSpec.path, FileSpec.fsize and
        FileSpec.chksum need to be set.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        # make logger
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        return self.simple_zip_output(jobspec, tmpLog)

    # asynchronous zip output
    def async_zip_output(self, jobspec):
        """OBSOLETE : zip functions should be implemented in zipper plugins.
        Zip output files asynchronously. This method is followed by post_zip_output(),
        which is typically useful to trigger an asynchronous zipping mechanism such as batch job.
        This method loops over jobspec.outFiles, which is a list of zip file's FileSpecs, to make
        a zip file for each zip file's FileSpec. FileSpec.associatedFiles is a list of FileSpecs
        of associated files to be zipped. The path of each associated file is available in associated
        file's FileSpec.path.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        # make logger
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        # set some ID which can be used for lookup in post_zip_output()
        groupID = str(uuid.uuid4())
        lfns = []
        for fileSpec in jobspec.outFiles:
            lfns.append(fileSpec.lfn)
        jobspec.set_groups_to_files({groupID: {"lfns": lfns, "groupStatus": "zipping"}})
        return True, ""

    # post zipping
    def post_zip_output(self, jobspec):
        """OBSOLETE : zip functions should be implemented in zipper plugins.
        This method is executed after async_zip_output(), to do post-processing for zipping.
        Once zip files are made, this method needs to look over jobspec.outFiles to set their
        FileSpec.path, FileSpec.fsize, and FileSpec.chksum.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        # make logger
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        # get groups for lookup
        groups = jobspec.get_groups_of_output_files()
        # do something with groupIDs
        pass
        # update file attributes
        for fileSpec in jobspec.outFiles:
            fileSpec.path = "/path/to/zip"
            fileSpec.fsize = 12345
            fileSpec.chksum = "66bb0985"
        return True, ""
