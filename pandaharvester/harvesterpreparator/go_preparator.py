import sys
from globus_sdk import TransferClient
from globus_sdk import TransferData

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils

# logger
_logger = core_utils.setup_logger()


# preparator with Globus Online
class GoPreparator(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check status
    def check_status(self, jobspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(jobspec.PandaID))
        # get label
        label = self.make_label(jobspec)
        tmpLog.debug('label={0}'.format(label))
        # get transfer task
        tmpStat, transferTasks = self.get_transfer_tasks(label)
        # return a temporary error when failed to get task
        if not tmpStat:
            errStr = 'failed to get transfer task'
            tmpLog.error(errStr)
            return None, errStr
        # return a fatal error when task is missing # FIXME retry instead?
        if label not in transferTasks:
            errStr = 'transfer task is missing'
            tmpLog.error(errStr)
            return False, errStr
        # succeeded
        if transferTasks[label]['status'] == 'SUCCEEDED':
            tmpLog.debug('transfer task succeeded')
            return True, ''
        # failed
        if transferTasks[label]['status'] == 'FAILED':
            errStr = 'transfer task failed'
            tmpLog.error(errStr)
            return False, errStr
        # another status
        tmpStr = 'transfer task is in {0}'.format(transferTasks[label]['status'])
        tmpLog.debug(tmpStr)
        return None, ''

    # trigger preparation
    def trigger_preparation(self, jobspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(jobspec.PandaID))
        # get label
        label = self.make_label(jobspec)
        tmpLog.debug('label={0}'.format(label))
        # get transfer tasks
        tmpStat, transferTasks = self.get_transfer_tasks(label)
        if not tmpStat:
            errStr = 'failed to get transfer tasks'
            tmpLog.error(errStr)
            return False, errStr
        # check if already queued
        if label in transferTasks:
            tmpLog.debug('skip since already queued with {0}'.format(str(transferTasks[label])))
            return True, ''
        # get input file attributes
        inFiles = jobspec.get_input_file_attributes(skip_ready=True)
        tmpLog.debug('transfer {0} input files'.format(len(inFiles)))
        # make transfer data
        try:
            tc = TransferClient()
            tdata = TransferData(tc,
                                 self.srcEndpoint,
                                 self.dstEndpoint,
                                 label=label,
                                 sync_level="checksum")
            # loop over all input files
            for lfn, fileAttrs in inFiles.iteritems():
                # source path # FIXME
                srcPath = "/" + lfn
                # destination path
                dstPath = "/panda_input/{PandaID}/{dataset}/{lfn}".format(PandaID=jobspec.PandaID,
                                                                          dataset=fileAttrs['dataset'],
                                                                          lfn=lfn)
                # add
                tdata.add_item(srcPath, dstPath)
            # submit
            transfer_result = tc.submit_transfer(tdata)
            # check status code and message
            tmpLog.debug(str(transfer_result))
            if False:  # FIXME
                return False, '...'
            # succeeded
            return True, ''
        except:
            core_utils.dump_error_message(tmpLog)
            return False, {}

    # get transfer tasks
    def get_transfer_tasks(self, label=None):
        # get logger
        tmpLog = core_utils.make_logger(_logger)
        try:
            # execute
            tc = TransferClient()
            if label == None:
                gRes = tc.task_list(filter='status:ACTIVE,INACTIVE,FAILED,SUCCEEDED',
                                    num_results=1000)
            else:
                gRes = tc.task_list(filter='status:ACTIVE,INACTIVE,FAILED,SUCCEEDED/label:{0}'.format(label))
            # parse output
            tasks = {}
            for res in gRes:
                label = res.data['label']
                tasks[label] = res.data
            # return
            tmpLog.debug('got {0} tasks'.format(len(tasks)))
            return True, tasks
        except:
            core_utils.dump_error_message(tmpLog)
            return False, {}

    # make label for transfer task
    def make_label(self, jobspec):
        return "IN-{computingSite}-{PandaID}".format(computingSite=jobspec.computingSite,
                                                     PandaID=jobspec.PandaID)

    # resolve input file paths
    def resolve_input_paths(self, jobspec):
        # get input files
        inFiles = jobspec.get_input_file_attributes()
        # set path to each file
        for inLFN, inFile in inFiles.iteritems():
            # FIXME
            inFile['path'] = 'dummypath/{0}'.format(inLFN)
        # set
        jobspec.set_input_file_paths(inFiles)
        return True, ''
