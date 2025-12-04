from pandaharvester.harvestercore import core_utils

from .base_zipper import BaseZipper

# logger
_logger = core_utils.setup_logger("ssh_zipper")


# ssh plugin for zipper
class SshZipper(BaseZipper):
    # constructor
    def __init__(self, **kwarg):
        BaseZipper.__init__(self, **kwarg)

    # zip output files
    def zip_output(self, jobspec):
        """Zip output files. This method loops over jobspec.outFiles, which is a list of zip file's FileSpecs,
        to make a zip file for each zip file's FileSpec. FileSpec.associatedFiles is a list of FileSpecs of
        associated files to be zipped. The path of each associated file is available in associated
        file's FileSpec.path. Once zip files are made, their FileSpec.path, FileSpec.fsize and
        FileSpec.chksum need to be set.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        tmpLog = self.make_logger(_logger, f"PandaID={jobspec.PandaID}", method_name="zip_output")
        return self.ssh_zip_output(jobspec, tmpLog)

    # asynchronous zip output
    def async_zip_output(self, jobspec):
        """Zip output files asynchronously. This method is followed by post_zip_output(),
        which is typically useful to trigger an asynchronous zipping mechanism such as batch job.
        This method loops over jobspec.outFiles, which is a list of zip file's FileSpecs, to make
        a zip file for each zip file's FileSpec. FileSpec.associatedFiles is a list of FileSpecs
        of associated files to be zipped. The path of each associated file is available in associated
        file's FileSpec.path.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False for fatal, None for pending) and error dialog
        :rtype: (bool, string)
        """
        tmpLog = self.make_logger(_logger, f"PandaID={jobspec.PandaID}", method_name="zip_output")
        # not really asynchronous as two staged zipping is not implemented in this plugin
        return self.ssh_zip_output(jobspec, tmpLog)

    # post zipping
    def post_zip_output(self, jobspec):
        """This method is executed after async_zip_output(), to do post-processing for zipping.
        Once zip files are made, this method needs to look over jobspec.outFiles to set their
        FileSpec.path, FileSpec.fsize, and FileSpec.chksum.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False for fatal, None for pending) and error dialog
        :rtype: (bool, string)
        """
        return True, ""
