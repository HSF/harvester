import os
import sys
import os.path
import zipfile
import hashlib
import requests
from globus_sdk import TransferClient
from globus_sdk import TransferData
from globus_sdk import NativeAppAuthClient
from globus_sdk import RefreshTokenAuthorizer

# TO BE REMOVED for python2.7
import requests.packages.urllib3
try:
    requests.packages.urllib3.disable_warnings()
except:
    pass
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc import globus_utils
from pandaharvester.harvestermover import mover_utils

# logger
baseLogger = core_utils.setup_logger('go_stager')

def dump(obj):
   for attr in dir(obj):
       if hasattr( obj, attr ):
           print( "obj.%s = %s" % (attr, getattr(obj, attr)))



# plugin for stager with FTS
class GlobusStager(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        # create Globus Transfer Client
        tmpLog = core_utils.make_logger(_logger, method_name='GlobusStager __init__ ')
        try:
            self.tc = None
            tmpStat, statusStr = self.create_globus_transfer_client()
            if not tmpStat:
                errStr = 'failed to create Globus Transfer Client'
                tmpLog.error(errStr)
        except:
            core_utils.dump_error_message(tmpLog)

    # check status
    def check_status(self, jobspec):
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                        method_name='check_status')
        tmpLog.debug('start')
        # loop over all files
        allChecked = True
        oneErrMsg = None
        transferStatus = {}
        for fileSpec in jobspec.outFiles:
            # get transfer ID
            transferID = fileSpec.fileAttributes['transferID']
            if transferID not in transferStatus:
                # get status
                errMsg = None
                try:
                    # get transfer task by transfer ID
                    tmpStat, transferTask = self.get_transfer_task_id(transferID)
                    if tmpStat:
                        # succeeded have task associated with transferID
                        transferStatus[transferID] = transferTask[transferID]['status']
                        tmpLog.debug('got {0} for {1}'.format(transferStatus[transferID],
                                                              transferID))
                    else:
                        errMsg = 'failed to get transfer task transferID: {}'.format(transferID)
                except:
                    if errMsg is None:
                        errtype, errvalue = sys.exc_info()[:2]
                        errMsg = "{0} {1}".format(errtype.__name__, errvalue)
                # failed
                if errMsg is not None:
                    allChecked = False
                    tmpLog.error('failed to get status for {0} with {1}'.format(transferID,
                                                                                errMsg))
                    # set dummy not to lookup again
                    transferStatus[transferID] = None
                    # keep one message
                    if oneErrMsg is None:
                        oneErrMsg = errMsg
            # final status
            if transferStatus[transferID] == 'SUCCEEDED':
                fileSpec.status = 'finished'
            elif transferStatus[transferID] in ['FAILED', 'INACTIVE']:
                fileSpec.status = 'failed'
        if allChecked:
            return True, ''
        else:
            return False, oneErrMsg

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                        method_name='trigger_stage_out')
        tmpLog.debug('start')
        # default return
        tmpRetVal = (True, '')
        # check that jobspec.computingSite is defined
        if jobspec.computingSite is None:
            # not found
            tmpLog.error('jobspec.computingSite is not defined')
            return False, 'jobspec.computingSite is not defined'
        else:
            tmpLog.debug('jobspec.computingSite : {0}'.format(jobspec.computingSite))
        # test we have a Globus Transfer Client
        if not self.tc :
            try:
                self.tc = None
                tmpStat, statusStr = self.create_globus_transfer_client()
                if not tmpStat:
                    errStr = 'failed to create Globus Transfer Client'
                    tmpLog.error(errStr)
            except:
                core_utils.dump_error_message(tmpLog)
        if not self.tc :
            errStr = 'failed to get Globus Transfer Client'
            tmpLog.error(errStr)
            return False, errStr
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
        # set the Globus destination Endpoint id and path will get them from Agis eventually  
        from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
        queueConfigMapper = QueueConfigMapper()
        queueConfig = queueConfigMapper.get_queue(jobspec.computingSite)
        #self.Globus_srcPath = queueConfig.stager['Globus_srcPath']
        self.srcEndpoint = queueConfig.stager['srcEndpoint']
        self.Globus_srcPath = self.basePath
        self.Globus_dstPath = queueConfig.stager['Globus_dstPath']
        self.dstEndpoint = queueConfig.stager['dstEndpoint']
        # loop over all files
        files = []
        lfns = set()
        fileAttrs = jobspec.get_output_file_attributes()
        for fileSpec in jobspec.outFiles:
            scope = fileAttrs[fileSpec.lfn]['scope']
            hash = hashlib.md5()
            hash.update('%s:%s' % (scope, fileSpec.lfn))
            hash_hex = hash.hexdigest()
            correctedscope = "/".join(scope.split('.'))
            srcURL = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(endPoint=self.Globus_srcpath,
                                                                       scope=correctedscope,
                                                                       hash1=hash_hex[0:2],
                                                                       hash2=hash_hex[2:4],
                                                                       lfn=fileSpec.lfn)                                                                        
            dstURL = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(endPoint=self.Globus_dstpath,
                                                                       scope=correctedscope,
                                                                       hash1=hash_hex[0:2],
                                                                       hash2=hash_hex[2:4],
                                                                       lfn=fileSpec.lfn)
            tmpLog.debug('src={srcURL} dst={dstURL}'.format(srcURL=srcURL, dstURL=dstURL))
            files.append({
                "sources": [srcURL],
                "destinations": [dstURL],
            })
            lfns.add(fileSpec.lfn)
        # submit
        if files != []:
            # get status
            errMsg = None
            try:
                # Test endpoints for activation
                tmpStatsrc, srcStr = self.check_endpoint_activation(self.srcEndpoint)
                tmpStatdst, dstStr = self.check_endpoint_activation(self.dstEndpoint)
                if tmpStatsrc and tmpStatdst:
                    errStr = 'source Endpoint and destination Endpoint activated'
                    tmpLog.debug(errStr)
                else:
                    errMsg = ''
                    if not tmpStatsrc :
                        errMsg += ' source Endpoint not activated '
                    if not tmpStatdst :
                        errMsg += ' destination Endpoint not activated '
                    tmpLog.error(errMsg)
                    tmpRetVal = (False,errMsg)
                    return tmpRetVal
                # both endpoints activated now prepare to transfer data
                tdata = TransferData(self.tc,
                                     self.srcEndpoint,
                                     self.dstEndpoint,
                                     label=label,
                                     sync_level="checksum")
                # loop over all input files and add 
                for myfile in files:
                    tdata.add_item(myfile['sources'],myfile['destinations'])
                # submit
                transfer_result = self.tc.submit_transfer(tdata)
                # check status code and message
                tmpLog.debug(str(transfer_result))
                if transfer_result['code'] == "Accepted":
                    # succeeded
                    # set transfer ID which are used for later lookup
                    transferID = transfer_result['task_id']
                    tmpLog.debug('successfully submitted id={0}'.format(transferID))
                    # set
                    for fileSpec in jobspec.outFiles:
                        if fileSpec.fileAttributes == None:
                            fileSpec.fileAttributes = {}
                        fileSpec.fileAttributes['transferID'] = transferID
                 else:
                    tmpRetVal = (False, transfer_result['message'])
             except:
                globus_utils.handle_globus_exception(tmpLog)
                if errMsg is None:
                    errtype, errvalue = sys.exc_info()[:2]
                    errMsg = "{0} {1}".format(errtype.__name__, errvalue)
            # failed
            if errMsg is not None:
                tmpLog.error('failed to submit transfer to Globus with {1}'.format(errMsg))
                tmpRetVal = (False, errMsg)
        # return
        tmpLog.debug('done')
        return tmpRetVal

    # zip output files
    def zip_output(self, jobspec):
        # make logger
        tmpLog = core_utils.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                        method_name='zip_output')
        tmpLog.debug('start')
        try:
            for fileSpec in jobspec.outFiles:
                if self.zipDir == "${SRCDIR}":
                    # the same directory as src
                    zipDir = os.path.dirname(next(iter(fileSpec.associatedFiles)).path)
                else:
                    zipDir = self.zipDir
                zipPath = os.path.join(zipDir, fileSpec.lfn)
                # remove zip file just in case
                try:
                    os.remove(zipPath)
                except:
                    pass
                # make zip file
                with zipfile.ZipFile(zipPath, "w", zipfile.ZIP_STORED) as zf:
                    for assFileSpec in fileSpec.associatedFiles:
                        zf.write(assFileSpec.path)
                # set path
                fileSpec.path = zipPath
                # get size
                statInfo = os.stat(zipPath)
                fileSpec.fsize = statInfo.st_size
        except:
            errMsg = core_utils.dump_error_message(tmpLog)
            return False, 'failed to zip with {0}'.format(errMsg)
        tmpLog.debug('done')
        return True, ''

    # make label for transfer task
    def make_label(self, jobspec):
        return "OUT-{computingSite}-{PandaID}".format(computingSite=jobspec.computingSite,
                                                     PandaID=jobspec.PandaID)

    # resolve input file paths
    def resolve_input_paths(self, jobspec):
        # get input files
        inFiles = jobspec.get_input_file_attributes()
        # set path to each file
        for inLFN, inFile in inFiles.iteritems():
            inFile['path'] = mover_utils.construct_file_path(self.basePath, inFile['scope'], inLFN)
        # set
        jobspec.set_input_file_paths(inFiles)
        return True, ''

    # Globus specific commands

    # get transfer tasks
    def get_transfer_task_by_id(self,transferID=None):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name='get_transfer_task_by_id')
        # test we have a Globus Transfer Client
        if not self.tc :
            try:
                self.tc = None
                tmpStat, statusStr = self.create_globus_transfer_client()
                if not tmpStat:
                    errStr = 'failed to create Globus Transfer Client'
                    tmpLog.error(errStr)
            except:
                core_utils.dump_error_message(tmpLog)
        if not self.tc :
            errStr = 'failed to get Globus Transfer Client'
            tmpLog.error(errStr)
            return False, errStr
        try:
            # execute
            if transferID == None:                
                # error need to have task ID
                errStr = 'failed to provide transfer task ID '
                tmpLog.error(errStr)
                return False, errStr
            else:
                 gRes = self.tc.get_task(transferID)
            # parse output
            tasks = {}
            tasks[transferID] = gRes
            # return
            tmpLog.debug('got {0} tasks'.format(len(tasks)))
            return True, tasks
        except:
            globus_utils.handle_globus_exception(tmpLog)
            return False, {}

    # get transfer tasks
    def get_transfer_tasks(self,label=None):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name='get_transfer_tasks')
        # test we have a Globus Transfer Client
        if not self.tc :
            try:
                self.tc = None
                tmpStat, statusStr = self.create_globus_transfer_client()
                if not tmpStat:
                    errStr = 'failed to create Globus Transfer Client'
                    tmpLog.error(errStr)
            except:
                core_utils.dump_error_message(tmpLog)
        if not self.tc :
            errStr = 'failed to get Globus Transfer Client'
            tmpLog.error(errStr)
            return False, errStr
        try:
            # execute
            if label == None:                
                params = {"filter": "type:TRANSFER/status:SUCCEEDED,INACTIVE,FAILED,SUCCEEDED"}
                gRes = self.tc.task_list(num_results=1000,**params)
            else:
                params = {"filter": "type:TRANSFER/status:SUCCEEDED,INACTIVE,FAILED,SUCCEEDED/label:{0}".format(label)}
                gRes = self.tc.task_list(**params)
            # parse output
            tasks = {}
            for res in gRes:
                reslabel = res.data['label']
                tasks[reslabel] = res.data
            # return
            tmpLog.debug('got {0} tasks'.format(len(tasks)))
            return True, tasks
        except:
            globus_utils.handle_globus_exception(tmpLog)
            return False, {}



    def create_globus_transfer_client(self):
        """
        create Globus Transfer Client and put results in self.tc
        """
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name='create_globus_transfer_client')
        tmpLog.info('Creating instance of GlobusTransferClient')
        # need to get client_id and refresh_token from PanDA server via harvester cache mechanism
        c_data = self.dbInterface.get_cache('globus_secret')
        if (not c_data == None) and  c_data.data['StatusCode'] == 0 :
            self.client_id = c_data.data['publicKey']  # client_id
            self.refresh_token = c_data.data['privateKey'] # refresh_token
        else :
            self.client_id = None
            self.refresh_token = None
            self.tc = None
            errStr = 'failed to get Globus Client ID and Refresh Token'
            tmpLog.error(errStr)
            return False, errStr
        # start the Native App authentication process 
        # use the refresh token to get authorizer
        # create the Globus Transfer Client
        self.tc = None
        try:
            client = NativeAppAuthClient(client_id=self.client_id)
            authorizer = RefreshTokenAuthorizer(refresh_token=self.refresh_token,auth_client=client)
            tc = TransferClient(authorizer=authorizer)
            self.tc = tc
            return True,''
        except:
            globus_utils.handle_globus_exception(tmpLog)
            return False, {}

    def check_endpoint_activation (self,endpoint_id):
        """
        check if endpoint is activated 
        """
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name='check_endpoint_activation')
        # test we have a Globus Transfer Client
        if not self.tc :
            errStr = 'failed to get Globus Transfer Client'
            tmpLog.error(errStr)
            return False, errStr
        try: 
            endpoint = self.tc.get_endpoint(endpoint_id)
            r = self.tc.endpoint_autoactivate(endpoint_id, if_expires_in=3600)

            tmpLog.info("Endpoint - %s - activation status code %s"%(endpoint["display_name"],str(r["code"])))
            if r['code'] == 'AutoActivationFailed':
                errStr = 'Endpoint({0}) Not Active! Error! Source message: {1}'.format(endpoint_id, r['message'])
                tmpLog.debug(errStr)
                return False, errStr
            elif r['code'] == 'AutoActivated.CachedCredential': 
                errStr = 'Endpoint({0}) autoactivated using a cached credential.'.format(endpoint_id)
                tmpLog.debug(errStr)
                return True,errStr
            elif r['code'] == 'AutoActivated.GlobusOnlineCredential':
                errStr = 'Endpoint({0}) autoactivated using a built-in Globus '.format(endpoint_id)
                tmpLog.debug(errStr)
                return True,errStr
            elif r['code'] == 'AlreadyActivated':
                errStr = 'Endpoint({0}) already active until at least {1}'.format(endpoint_id,3600)
                tmpLog.debug(errStr)
                return True,errStr
        except:
            globus_utils.handle_globus_exception(tmpLog)
            return False, {}
