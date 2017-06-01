from pandaharvester.harvestercore.plugin_base import PluginBase


# dummy plugin for stager
class DummyStager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check status
    def check_status(self, jobspec):
        """Check status of stage-out procedure. If that is done synchronously in trigger_stage_out
        this method should always return True.
        Output files are available through jobspec.outFiles which gives a list of FileSpecs.
        FileSpec.status needs to be set to 'finished' if stage-out was successful for a file,
        or to 'failed' if it failed.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise, None if on-going) and error dialog
        :rtype: (bool, string)
        """
        for fileSpec in jobspec.outFiles:
            fileSpec.status = 'finished'
        return True, ''

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        """Trigger stage-out procedure for the job.
        Output files are available through jobspec.outFiles which gives a list of FileSpecs.
        FileSpec.status should be checked to skip finished or failed files.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        for fileSpec in jobspec.outFiles:
            # fileSpec.objstoreID = 123
            # fileSpec.fileAttributes['guid']
            pass
        return True, ''

    # zip output files
    def zip_output(self, jobspec):
        """Zip output files. This method loops over jobspec.outFiles to make a zip file
        for each outFileSpec from FileSpec.associatedFiles which is a list of toZipFileSpec to be zipped.
        The file path is available in toZipFileSpec. One zip files are made, their toZipFileSpec.path and
        toZipFileSpec.fsize need to be set.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        for fileSpec in jobspec.outFiles:
            fileSpec.path = '/path/to/zip'
        return True, ''
