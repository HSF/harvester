from rucio.client import Client as RucioClient
from rucio.common.exception import DataIdentifierNotFound, DuplicateRule, DataIdentifierAlreadyExists

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterstager.go_bulk_stager import GlobusBulkStager


# logger
_logger = core_utils.setup_logger('go_rucio_stager')
GlobusBulkStager._logger = _logger


# plugin with Globus + Rucio + bulk transfers
class GlobusRucioStager(GlobusBulkStager):

    # constructor
    def __init__(self, **kwarg):
        GlobusBulkStager.__init__(self, **kwarg)
        self.changeFileStatusOnSuccess = False

    # check status
    def check_status(self, jobspec):
        # make logger
        tmpLog = self.make_logger(_logger, 'PandaID={0}'.format(jobspec.PandaID),
                                  method_name='check_status')
        tmpLog.debug('executing base check_status')
        tmpStat, tmpMsg = GlobusBulkStager.check_status(self, jobspec)
        tmpLog.debug('got {0} {1}'.format(tmpLog, tmpMsg))
        if tmpStat is not True:
            return tmpStat, tmpMsg
        # get transfer groups
        groups = jobspec.get_groups_of_output_files()
        if len(groups) == 0:
            return tmpStat, tmpMsg
        # endpoint
        nucleus = jobspec.jobParams['nucleus']
        agis = self.dbInterface.get_cache('panda_queues.json').data
        dstRSE = [agis[x]["astorages"]['pr'][0] for x in agis if agis[x]["atlas_site"] == nucleus][0]
        # loop over all transfers
        tmpStat = True
        tmpMsg = ''
        for transferID in groups:
            if transferID is None:
                continue
            datasetName = 'panda.harvester.{0}.{1}'.format(jobspec.PandaID, transferID)
            datasetScope = 'transient'
            srcRSE = None
            # lock
            have_db_lock = self.dbInterface.get_object_lock(transferID)
            if not have_db_lock:
                msgStr = 'escape since {0} is locked by another thread'.format(transferID)
                tmpLog.debug(msgStr)
                # release lock
                self.dbInterface.release_object_lock(transferID)
                return None, msgStr
            # get transfer status
            groupStatus = self.dbInterface.get_file_group_status(transferID)
            if 'hopped' in groupStatus:
                pass
            elif 'failed' in groupStatus:
                # transfer failure
                tmpStat = False
                tmpMsg = 'rucio rule for {0}:{1} already failed'.format(datasetScope, datasetName)
            elif 'hopping' in groupStatus:
                # check rucio rule
                ruleStatus = 'FAILED'
                try:
                    tmpLog.debug('check state for {0}:{1}'.format(datasetScope, datasetName))
                    rucioAPI = RucioClient()
                    for ruleInfo in rucioAPI.list_did_rules(datasetScope, datasetName):
                        if ruleInfo['rse_expression'] != dstRSE:
                            continue
                        ruleStatus = ruleInfo['state']
                        tmpLog.debug('got state={0}'.format(ruleStatus))
                        if ruleStatus == 'OK':
                            break
                except DataIdentifierNotFound:
                    tmpLog.error('rule {0} not found'.format(transferID))
                except Exception:
                    core_utils.dump_error_message(tmpLog)
                    ruleStatus = None
                if ruleStatus in ['FAILED', 'CANCELED']:
                    # transfer failure
                    tmpStat = False
                    tmpMsg = 'rucio rule for {0}:{1} failed with {2}'.format(datasetScope, datasetName, ruleStatus)
                    # update file group status
                    self.dbInterface.update_file_group_status(transferID, 'failed')
                elif ruleStatus == 'OK':
                    # update file group status
                    self.dbInterface.update_file_group_status(transferID, 'hopped')
                else:
                    # replicating or temporary error
                    tmpStat = None
                    tmpMsg = 'replicating or temporary error for {0}:{1}'.format(datasetScope, datasetName)
            else:
                # make rucio rule
                fileSpecs = self.dbInterface.get_files_with_group_id(transferID)
                fileList = []
                for fileSpec in fileSpecs:
                    tmpFile = dict()
                    tmpFile['scope'] = datasetScope
                    tmpFile['name'] = fileSpec.lfn
                    tmpFile['bytes'] = fileSpec.fsize
                    tmpFile['adler32'] = fileSpec.checksum
                    tmpFile['meta'] = {'guid': fileSpec.fileAttributes['guid']}
                    fileList.append(tmpFile)
                    # get source RSE
                    if srcRSE is None and fileSpec.objstoreID is not None:
                        ddm = self.dbInterface.get_cache('agis_ddmendpoints.json')
                        srcRSE = [x for x in ddm if ddm[x]["id"] == fileSpec.objstoreID][0]
                try:
                    # register dataset
                    tmpLog.debug('register {0}:{1}'.format(datasetScope, datasetName))
                    rucioAPI = RucioClient()
                    try:
                        rucioAPI.add_dataset(datasetScope, datasetName,
                                             meta={'hidden': True},
                                             lifetime=7 * 24 * 60 * 60,
                                             files=fileList,
                                             rse=srcRSE
                                             )
                    except DataIdentifierAlreadyExists:
                        pass
                    except Exception:
                        raise
                    # add rule
                    try:
                        tmpDID = dict()
                        tmpDID['scope'] = datasetScope
                        tmpDID['name'] = datasetName
                        tmpRet = rucioAPI.add_replication_rule([tmpDID], 1, dstRSE,
                                                               lifetime=7 * 24 * 60 * 60)
                        ruleIDs = tmpRet[0]
                    except DuplicateRule:
                        pass
                    except Exception:
                        raise
                    # update file group status
                    self.dbInterface.update_file_group_status(transferID, 'hopping')
                    tmpLog.debug('registered dataset {0}:{1} with rule {2}'.format(datasetScope, datasetName,
                                                                                   str(ruleIDs)))
                except Exception:
                    core_utils.dump_error_message(tmpLog)
                    tmpStat = None
                    tmpMsg = 'failed to add a rule for {0}:{1}'.format(datasetScope, datasetName)
            # release lock
            self.dbInterface.release_object_lock(transferID)
            # escape if already failed
            if tmpStat is False:
                break
        # all done
        if tmpStat is True:
            self.set_FileSpec_status(jobspec, 'finished')
        tmpLog.debug('done with {0} : {1}'.format(tmpStat, tmpMsg))
        return tmpStat, tmpMsg
