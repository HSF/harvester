import os.path
import os

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermover import mover_utils
from pilot.api import data

# logger
baseLogger = core_utils.setup_logger()


# plugin for preparator with RSE + rucio copytool from pilot
class RsePreparator(PluginBase):
    """The workflow for RsePreparator is as follows. First panda makes a rule to
    transfer files to an RSE which is associated to the resource. Once files are transferred
    to the RSE, job status is changed to activated from assigned. Then Harvester fetches
    the job and constructs input file paths that point to pfns in the storage. This means
    that the job directly read input files from the storage.
    """
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check status
    def check_status(self, jobspec):
        return True, ''

    # trigger preparation
    def trigger_preparation(self, jobspec):
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID))
        tmpLog.debug('start')        
       
        # check that jobspec.computingSite is defined
        if jobspec.computingSite is None:
            # not found
            tmpLog.error('jobspec.computingSite is not defined')
            return False, 'jobspec.computingSite is not defined'
        else:
            tmpLog.debug('jobspec.computingSite : {0}'.format(jobspec.computingSite))
        # get input files
        files = []
        inFiles = jobspec.get_input_file_attributes()
        # set path to each file
        for inLFN, inFile in inFiles.iteritems():
            inFile['path'] = mover_utils.construct_file_path(self.basePath, inFile['dataset'], inFile['scope'], inLFN)
            dstpath = os.path.dirname(inFile['path'])
            # check if path exists if not create it.
            if not os.access(dstpath, os.F_OK):
                os.makedirs(dstpath)
            files.append({'scope': inFile['scope'],
                          'name': inLFN,
                          'destination': dstpath})
        tmpLog.debug('files[] {0}'.format(files))
        data_client = data.StageInClient(site=jobspec.computingSite)
        result = data_client.transfer(files)
        tmpLog.debug('pilot.api data.StageInClient.transfer(files) result: {0}'.format(result))
        # loop over each file check result all must be true for entire result to be true
        allChecked = True
        ErrMsg = 'These files failed to download : '
        for answer in result:
            if answer['errno'] != 0:
                allChecked = False
                ErrMsg = ErrMsg + (" %s " % answer['name'])
        # return
        tmpLog.debug('stop')
        if allChecked:
            return True, ''
        else:
            return False, ErrMsg

    # resolve input file paths
    def resolve_input_paths(self, jobspec):
        # get input files
        inFiles = jobspec.get_input_file_attributes()
        # set path to each file
        for inLFN, inFile in inFiles.iteritems():
            inFile['path'] = mover_utils.construct_file_path(self.basePath, inFile['dataset'], inFile['scope'], inLFN)
        # set
        jobspec.set_input_file_paths(inFiles)
        return True, ''
