import uuid

from pandaharvester.harvestercore import core_utils
from .base_zipper import BaseZipper

# logger
_logger = core_utils.setup_logger("dummy_zipper")


# dummy plugin for zipper
class DummyZipper(BaseZipper):
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
        # make logger
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        return self.simple_zip_output(jobspec, tmpLog)

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
        # make logger
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="async_zip_output")
        tmpLog.debug("start")
        # set some ID which can be used for lookup in post_zip_output()
        groupID = str(uuid.uuid4())
        lfns = []
        for fileSpec in jobspec.outFiles:
            lfns.append(fileSpec.lfn)
        jobspec.set_groups_to_files({groupID: {"lfns": lfns, "groupStatus": "zipping"}})
        return True, ""

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
        # make logger
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="post_zip_output")
        tmpLog.debug("start")
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
