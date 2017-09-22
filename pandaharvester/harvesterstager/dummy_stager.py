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
        FileSpec.attemptNr shows how many times the transfer was checked for the file.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False for failure, or None if on-going) and error dialog
        :rtype: (bool, string)
        """
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            fileSpec.status = 'finished'
        return True, ''

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        """Trigger stage-out procedure for the job.
        Output files are available through jobspec.get_outfile_specs(skip_done=False) which gives
        a list of FileSpecs not yet done.
        FileSpec.attemptNr shows how many times transfer was tried for the file.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True: success, False: fatal error, None: temporary error) and error dialog
        :rtype: (bool, string)
        """
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
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
        for fileSpec in jobspec.get_output_file_specs(skip_done=False):
            fileSpec.path = '/path/to/zip'
        return True, ''
