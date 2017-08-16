from pandaharvester.harvestercore.plugin_base import PluginBase


# dummy plugin for preparator
class DummyPreparator(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check status
    def check_status(self, jobspec):
        """Check status of stage-in procedure. If that is done synchronously in trigger_preparation
        this method should always return True.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        return True, ''

    # trigger preparation
    def trigger_preparation(self, jobspec):
        """Trigger stage-in procedure synchronously or asynchronously for the job.
        Input file attributes are available through jobspec.get_input_file_attributes(skip_ready=True)
        which gives a dictionary. The key of the dictionary is LFN of the input file
        and the value is a dictionary of file attributes. The attribute names are
        fsize, guid, checksum, scope, dataset, and endpoint.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        return True, ''

    # resolve input file paths
    def resolve_input_paths(self, jobspec):
        """Set input file paths to jobspec.get_input_file_attributes[LFN]['path'] for the job.
        New input file attributes need to be set to jobspec using jobspec.set_input_file_paths()
        after setting the file paths.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        # get input files
        inFiles = jobspec.get_input_file_attributes()
        # set path to each file
        for inLFN, inFile in inFiles.iteritems():
            inFile['path'] = 'dummypath/{0}'.format(inLFN)
        # set
        jobspec.set_input_file_paths(inFiles)
        return True, ''
