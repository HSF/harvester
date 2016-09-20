import sys
from globus_sdk import TransferClient
from globus_sdk import TransferData

from pandaharvester.harvestercore.PluginBase import PluginBase
from pandaharvester.harvestercore import CoreUtils

# logger
_logger = CoreUtils.setupLogger()



# preparator with Globus Online
class GoPreparator (PluginBase):
    
    # constructor
    def __init__(self,**kwarg):
        PluginBase.__init__(self,**kwarg)



    # check status
    def checkStatus(self,jobSpec):
        # get logger
        tmpLog = CoreUtils.makeLogger(_logger,'PandaID={0}'.format(jobSpec.PandaID))
        # get label
        label = self.makeLabel(jobSpec)
        tmpLog.debug('label={0}'.format(label))
        # get transfer task
        tmpStat,transferTasks = self.getTransferTasks(label)
        # return a temporary error when failed to get task
        if not tmpStat:
            errStr = 'failed to get transfer task'
            tmpLog.error(errStr)
            return None,errStr
        # return a fatal error when task is missing # FIXME retry instead?
        if not label in transferTasks:
            errStr = 'transfer task is missing'
            tmpLog.error(errStr)
            return False,errStr
        # succeeded
        if transferTasks[label]['status'] == 'SUCCEEDED':
            tmpLog.debug('transfer task succeeded')
            return True,''
        # failed
        if transferTasks[label]['status'] == 'FAILED':
            errStr = 'transfer task failed'
            tmpLog.error(errStr)
            return False,errStr
        # another status
        tmpStr = 'transfer task is in {0}'.format(transferTasks[label]['status'])
        tmpLog.debug(tmpStr)
        return None,''




    # trigger preparation
    def triggerPreparation(self,jobSpec):
        # get logger
        tmpLog = CoreUtils.makeLogger(_logger,'PandaID={0}'.format(jobSpec.PandaID))
        # get label
        label = self.makeLabel(jobSpec)
        tmpLog.debug('label={0}'.format(label))
        # get transfer tasks
        tmpStat,transferTasks = self.getTransferTasks(label)
        if not tmpStat:
            errStr = 'failed to get transfer tasks'
            tmpLog.error(errStr)
            return False,errStr
        # check if already queued
        if label in transferTasks:
            tmpLog.debug('skip since already queued with {0}'.format(str(transferTasks[label])))
            return True,''
        # get input file attributes
        inFiles = jobSpec.getInputFileAttributes()
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
            for lfn,fileAttrs in inFiles.iteritems():
                # source path # FIXME
                srcPath = "/" + lfn
                # destination path
                dstPath = "/panda_input/{PandaID}/{dataset}/{lfn}".format(PandaID=jobSpec.PandaID,
                                                                          dataset=fileAttrs['dataset'],
                                                                          lfn=lfn)
                # add
                tdata.add_item(srcPath,dstPath)
            # submit
            transfer_result = tc.submit_transfer(tdata)
            # check status code and message
            tmpLog.debug(str(transfer_result))
            if False: # FIXME
                return False,'...'
            # succeeded
            return True,''
        except:
            CoreUtils.dumpErrorMessage(tmpLog)
            return False,{}



    # get transfer tasks
    def getTransferTasks(self,label=None):
        # get logger
        tmpLog = CoreUtils.makeLogger(_logger)
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
                label  = res.data['label']
                tasks[label] = res.data
            # return
            tmpLog.debug('got {0} tasks'.format(len(tasks)))
            return True,tasks
        except:
            CoreUtils.dumpErrorMessage(tmpLog)
            return False,{}



    # make label for transfer task
    def makeLabel(self,jobSpec):
        return "IN-{computingSite}-{PandaID}".format(computingSite=jobSpec.computingSite,
                                                      PandaID=jobSpec.PandaID)
