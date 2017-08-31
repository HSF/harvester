import uuid

from pandaharvester.harvestercore.plugin_base import PluginBase


# dummy plugin for preparator
class DummyPreparator(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # trigger preparation
    def trigger_preparation(self, jobspec):
        """Trigger stage-in procedure synchronously or asynchronously for the job.
        Input file attributes are available through jobspec.get_input_file_attributes(skip_ready=True)
        which gives a dictionary. The key of the dictionary is LFN of the input file
        and the value is a dictionary of file attributes. The attribute names are
        fsize, guid, checksum, scope, dataset, and endpoint. Grouping information such as transferID can be set
        to input files using jobspec.set_group_to_files(id_map) where id_map is
        {groupID:'lfns':[lfn1, ...], 'status':status}, and groupID and status are arbitrary strings.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        #
        # Here is an example with file grouping :
        # get input files while skipping files already in ready state
        inFiles = jobspec.get_input_file_attributes(skip_ready=True)
        lfns = []
        for inLFN in inFiles.keys():
            lfns.append(inLFN)
        # one transfer ID for all input files
        transferID = str(uuid.uuid4())
        # set transfer ID which are used for later lookup
        jobspec.set_groups_to_files({transferID: {'lfns': lfns, 'groupStatus': 'active'}})
        return True, ''

    # check status
    def check_status(self, jobspec):
        """Check status of stage-in procedure. If that is done synchronously in trigger_preparation
        this method should always return True. Status of file group can be updated using
        jobspec.update_group_status_in_files(group_id, group_status)

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        #
        # Here is an example with file grouping :
        # get group IDs of input files except ones already in ready state
        transferIDs = jobspec.get_groups_of_input_files(skip_ready=True)
        # update transfer status
        for transferID in transferIDs:
            jobspec.update_group_status_in_files(transferID, 'done')
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
