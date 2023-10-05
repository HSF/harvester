import uuid
from future.utils import iteritems

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils

# logger
_logger = core_utils.setup_logger("dummy_preparator")


# dummy plugin for preparator
class DummyPreparator(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # trigger preparation
    def trigger_preparation(self, jobspec):
        """Trigger the stage-in procedure synchronously or asynchronously for the job.
        If the return code of this method is True, the job goes to the next step. If it is False,
        preparator immediately gives up the job. If it is None, the job is retried later.
        Input file attributes are available through jobspec.get_input_file_attributes(skip_ready=True)
        which gives a dictionary. The key of the dictionary is LFN of the input file
        and the value is a dictionary of file attributes. The attribute names are
        fsize, guid, checksum, scope, dataset, attemptNr, and endpoint. attemptNr shows how many times
        the file was tried so far. Grouping information such as transferID can be set to input files using
        jobspec.set_group_to_files(id_map) where id_map is
        {groupID:'lfns':[lfn1, ...], 'status':status}, and groupID and status are arbitrary strings.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True: success, False: fatal error, None: temporary error)
                 and error dialog
        :rtype: (bool, string)
        """
        # -- make log
        # tmpLog = self.make_logger(_logger, 'PandaID={0}'.format(jobspec.PandaID),
        #                           method_name='trigger_preparation')
        # tmpLog.debug('start')
        #
        # Here is an example to access cached data
        # c_data = self.dbInterface.get_cache('panda_queues.json')
        # tmpLog.debug(len(c_data.data))
        #
        # Here is an example with file grouping :
        # -- get input files while skipping files already in ready state
        # inFiles = jobspec.get_input_file_attributes(skip_ready=True)
        # tmpLog.debug('inputs={0}'.format(str(inFiles)))
        # lfns = []
        # for inLFN in inFiles.keys():
        #     lfns.append(inLFN)
        # -- one transfer ID for all input files
        # transferID = str(uuid.uuid4())
        # -- set transfer ID which are used for later lookup
        # jobspec.set_groups_to_files({transferID: {'lfns': lfns, 'groupStatus': 'active'}})
        # tmpLog.debug('done')
        return True, ""

    # check status
    def check_stage_in_status(self, jobspec):
        """Check status of the stage-in procedure.
        If the return code of this method is True, the job goes to the next step. If it is False,
        preparator immediately gives up the job. If it is None, the job is retried later.
        If preparation is done synchronously in trigger_preparation
        this method should always return True. Status of file group can be updated using
        jobspec.update_group_status_in_files(group_id, group_status) if necessary.

        :param jobspec: job specifications
        :type jobspec: JobSpec
        :return: A tuple of return code (True: transfer success, False: fatal transfer failure,
                 None: on-going or temporary failure) and error dialog
        :rtype: (bool, string)
        """
        #
        # Here is an example with file grouping :
        # get groups of input files except ones already in ready state
        # transferGroups = jobspec.get_groups_of_input_files(skip_ready=True)
        # -- update transfer status
        # for transferID, transferInfo in iteritems(transferGroups):
        #    jobspec.update_group_status_in_files(transferID, 'done')
        return True, ""

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
        # Here is an example to set file paths
        # -- get input files
        inFiles = jobspec.get_input_file_attributes()
        # -- set path to each file
        for inLFN, inFile in iteritems(inFiles):
            inFile["path"] = "dummypath/{0}".format(inLFN)
        # -- set
        jobspec.set_input_file_paths(inFiles)
        return True, ""
