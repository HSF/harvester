import os.path
import os
import threading
import time
import zlib
from future.utils import iteritems

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermover import mover_utils
from pilot.api import data

# logger
baseLogger = core_utils.setup_logger('pilotmover_mt_preparator')


# plugin for preparator based on Pilot2.0 Data API, MultipleThreads
# Pilot 2.0 should be deployed as library
# default self.basePath came from preparator section of configuration file

class PilotmoverMTPreparator(PluginBase):
    """
    Praparator bring files from remote ATLAS/Rucio storage to local facility. 
    """


    # constructor
    def __init__(self, **kwarg):
        self.n_threads = 3
        PluginBase.__init__(self, **kwarg)
        if self.n_threads < 1:
            self.n_threads = 1

    # check status
    def check_status(self, jobspec):
        return True, ''


    def calculate_adler32_checksum(self, filename):
        asum = 1  # default adler32 starting value
        blocksize = 64 * 1024 * 1024  # read buffer size, 64 Mb

        with open(filename, 'rb') as f:
            while True:
                data = f.read(blocksize)
                if not data:
                    break
                asum = zlib.adler32(data, asum)
                if asum < 0:
                    asum += 2**32

        # convert to hex
        return "{0:08x}".format(asum)

    def stage_in(self, tmpLog, jobspec, files):
        tmpLog.debug('To stagein files[] {0}'.format(files))
        data_client = data.StageInClient(site=jobspec.computingSite)
        allChecked = True
        ErrMsg = 'These files failed to download : '
        if len(files) > 0:
            result = data_client.transfer(files)
            tmpLog.debug('pilot.api data.StageInClient.transfer(files) result: {0}'.format(result))
        
            # loop over each file check result all must be true for entire result to be true
            if result:
                for answer in result:
                    if answer['errno'] != 0:
                        allChecked = False
                        ErrMsg = ErrMsg + (" %s " % answer['name'])
            else:
                tmpLog.info('Looks like all files already inplace: {0}'.format(files))
        # return
        tmpLog.debug('stop thread')
        if allChecked:
            return True, ''
        else:
            return False, ErrMsg

    # trigger preparation
    def trigger_preparation(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                  method_name='trigger_preparation')
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
        inFiles = jobspec.get_input_file_attributes(skip_ready=True)
        # set path to each file
        for inLFN, inFile in iteritems(inFiles):
            inFile['path'] = mover_utils.construct_file_path(self.basePath, inFile['scope'], inLFN)
            tmpLog.debug('To check file: %s' % inFile)
            if os.path.exists(inFile['path']):
                checksum = self.calculate_adler32_checksum(inFile['path'])
                checksum = 'ad:%s' % checksum
                tmpLog.debug('checksum for file %s is %s' % (inFile['path'], checksum))
                if 'checksum' in inFile and inFile['checksum'] and inFile['checksum'] == checksum:
                    tmpLog.debug('File %s already exists at %s' % (inLFN, inFile['path']))
                    continue
            dstpath = os.path.dirname(inFile['path'])
            # check if path exists if not create it.
            if not os.access(dstpath, os.F_OK):
                os.makedirs(dstpath)
            files.append({'scope': inFile['scope'],
                          'name': inLFN,
                          'destination': dstpath})
        tmpLog.debug('files[] {0}'.format(files))

        allChecked = True
        ErrMsg = 'These files failed to download : '
        if files:
            threads = []
            n_files_per_thread = (len(files)+ self.n_threads - 1)/self.n_threads
            tmpLog.debug('num files per thread: %s' % n_files_per_thread)
            for i in range(0, len(files), n_files_per_thread):
                sub_files = files[i:i + n_files_per_thread]
                thread = threading.Thread(target=self.stage_in, kwargs={'tmpLog': tmpLog, 'jobspec': jobspec, 'files': sub_files})
                threads.append(thread)
            [t.start() for t in threads]
            while len(threads) > 0:
                time.sleep(1)
                threads = [t for t in threads if t and t.isAlive()]

            tmpLog.info('Checking all files: {0}'.format(files))
            for file in files:
                if file['errno'] != 0:
                    allChecked = False
                    ErrMsg = ErrMsg + (" %s " % file['name'])
        # return
        tmpLog.debug('stop')
        if allChecked:
            tmpLog.info('Looks like all files are successfully downloaded.')
            return True, ''
        else:
            return False, ErrMsg

    # resolve input file paths
    def resolve_input_paths(self, jobspec):
        # get input files
        inFiles = jobspec.get_input_file_attributes()
        # set path to each file
        for inLFN, inFile in iteritems(inFiles):
            inFile['path'] = mover_utils.construct_file_path(self.basePath, inFile['scope'], inLFN)
        # set
        jobspec.set_input_file_paths(inFiles)
        return True, ''
