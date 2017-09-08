import sys
from globus_sdk import TransferClient
from globus_sdk import TransferData
from globus_sdk import NativeAppAuthClient
from globus_sdk import RefreshTokenAuthorizer


from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc import globus_utils

# logger
_logger = core_utils.setup_logger()


# preparator with Globus Online
class GoPreparator(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        # create Globus Transfer Client
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
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(jobspec.PandaID),
                                        method_name='check_status')
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
        tmpLog = core_utils.make_logger(_logger, 'PandaID={0}'.format(jobspec.PandaID),
                                        method_name='trigger_preparation')
        tmpLog.debug('start')        
       
        # check that jobspec.computingSite is defined
        if jobspec.computingSite is None:
            # not found
            tmpLog.error('jobspec.computingSite is not defined')
            return False, 'jobspec.computingSite is not defined'
        else:
            tmpLog.debug('jobspec.computingSite : {0}'.format(jobspec.computingSite))
        # test we have a Globus Transfer Client
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
        # get input files
        files = []
        inFiles = jobspec.get_input_file_attributes(skip_ready=True)
        # set path to each file
        for inLFN, inFile in inFiles.iteritems():
            inFile['path'] = mover_utils.construct_file_path(self.basePath, inFile['scope'], inLFN)
            dstpath = inFile['path']
            # check if path exists if not create it.
            if not os.access(dstpath, os.F_OK):
                os.makedirs(dstpath)
            # create the file paths for the Globus source and destination endpoints 
            Globus_srcpath = mover_utils.construct_file_path(self.Globus_srcPath, inFile['scope'], inLFN)
            Globus_dstpath = mover_utils.construct_file_path(self.Globus_dstPath, inFile['scope'], inLFN)
            files.append({'scope': inFile['scope'],
                          'name': inLFN,
                          'Globus_dstPath': Globus_dstpath,
                          'Globus_srcPath': Globus_srcpath})
        tmpLog.debug('files[] {0}'.format(files))
        try:
            # Test endpoints for activation
            tmpStatsrc, srcStr = self.check_endpoint_activation(self.srcEndpoint)
            tmpStatdst, dstStr = self.check_endpoint_activation(self.dstEndpoint)
            if tmpStatsrc and tmpStatdst:
                errStr = 'source Endpoint and destination Endpoint activated'
                tmpLog.debug(errStr)
            else:
                errStr = ''
                if not tmpStatsrc :
                    errStr += ' source Endpoint not activated '
                if not tmpStatdst :
                    errStr += ' destination Endpoint not activated '
                tmpLog.error(errStr)
                return False,errStr
            # both endpoints activated now prepare to transfer data
            if len(files) > 0:
                tdata = TransferData(self.tc,
                                     self.srcEndpoint,
                                     self.dstEndpoint,
                                     label=label,
                                     sync_level="checksum")
                # loop over all input files and add 
                for myfile in files:
                    tdata.add_item(myfile['Globus_srcPath'],myfile['Globus_dstPath'])
                # submit
                transfer_result = tc.submit_transfer(tdata)
                # check status code and message
                tmpLog.debug(str(transfer_result))
                if transfer_result['code'] == "Accepted":
                    # succeeded
                    return True,''
                else:
                    return False,transfer_result['message']
            # if no files to transfer return True
            return True, 'No files to transfer'
        except:
            globus_utils.handle_globus_exception(tmpLog)
            return False, {}

    # get transfer tasks
    def get_transfer_tasks(self,label=None):
        # get logger
        tmpLog = core_utils.make_logger(_logger, method_name='get_transfer_tasks')
        # test we have a Globus Transfer Client
        if not self.tc :
            errStr = 'failed to get Globus Transfer Client'
            tmpLog.error(errStr)
            return False, errStr
        try:
            # execute
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
            globus_utils.handle_globus_exception(tmpLog)
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
            inFile['path'] = mover_utils.construct_file_path(self.basePath, inFile['scope'], inLFN)
        # set
        jobspec.set_input_file_paths(inFiles)
        return True, ''


    # Globus specific commands
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
        tmpLog = core_utils.make_logger(_logger, method_name='check_endpoint_activation'))
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
