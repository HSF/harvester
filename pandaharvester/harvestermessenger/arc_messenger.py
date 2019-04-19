import errno
import os
import json
import re
import shutil
import tarfile
from urlparse import urlparse
import arc

from pandaharvester.harvestercore import core_utils
from .base_messenger import BaseMessenger
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc import arc_utils
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestercore.work_spec import WorkSpec


# json for outputs
jsonOutputsFileName = harvester_config.payload_interaction.eventStatusDumpJsonFile

# xml for outputs
xmlOutputsBaseFileName = harvester_config.payload_interaction.eventStatusDumpXmlFile

# json for event request
jsonEventsRequestFileName = harvester_config.payload_interaction.eventRequestFile

# json to feed events
jsonEventsFeedFileName = harvester_config.payload_interaction.eventRangesFile

# json to update events
jsonEventsUpdateFileName = harvester_config.payload_interaction.updateEventsFile

# suffix to read json
suffixReadJson = '.read'

# logger
baselogger = core_utils.setup_logger()

class ARCMessenger(BaseMessenger):
    '''Mechanism for passing information about completed jobs back to harvester.'''

    def __init__(self, **kwarg):
        self.jobSpecFileFormat = 'json'
        BaseMessenger.__init__(self, **kwarg)
        self.schedulerid = harvester_config.master.harvester_id
        self.tmpdir = '/tmp' # TODO configurable or common function

        # Credential dictionary role: proxy file
        self.certs = dict(zip([r.split('=')[1] for r in list(harvester_config.credmanager.voms)],
                              list(harvester_config.credmanager.outCertFile)))
        self.cred_type = arc.initializeCredentialsType(arc.initializeCredentialsType.SkipCredentials)


    def _setup_proxy(self, usercfg, workspec, jobid, log):
        '''Set up UserConfig object with correct proxy'''

        proxyrole = workspec.workAttributes['proxyrole']
        try:
            usercfg.ProxyPath(str(self.certs[proxyrole]))
        except:
            log.error("Job {0}: no proxy found with role {1}".format(jobid, proxyrole))
            return False
        return True


    def _copy_file(self, source, destination, usercfg, log):
        '''Copy a file from source to destination'''

        log.info('Copying {0} to {1}'.format(source, destination))
        source_datapoint = arc_utils.DataPoint(str(source), usercfg)
        destination_datapoint = arc_utils.DataPoint(str(destination), usercfg)
        dm = arc.DataMover()
        dm.retry(False)
        dm.passive(True)
        dm.secure(False)

        status = dm.Transfer(source_datapoint.h, destination_datapoint.h, arc.FileCache(), arc.URLMap())
        return status


    def _delete_file(self, filename, usercfg, log):
        '''Delete a remote file on ARC CE'''
        log.info('Deleting {0}'.format(filename))
        datapoint = arc_utils.DataPoint(str(filename), usercfg)
        datapoint.h.SetSecure(False)
        status = datapoint.h.Remove()
        return status


    def _list_url_recursive(self, url, log, fname='', filelist=[]):
        '''List ARC job directory recursively to find all files'''

        dp = arc_utils.DataPoint(url+'/'+fname, self.userconfig)
        files = dp.h.List(arc.DataPoint.INFO_TYPE_NAME | arc.DataPoint.INFO_TYPE_TYPE)
        if not files[1]:
            log.warning("Failed listing %s/%s" % (url, fname))
            return filelist
        for f in files[0]:
            if f.GetType() == f.file_type_file:
                filelist.append((fname+'/'+f.GetName()).strip('/'))
            elif f.GetType() == f.file_type_dir:
                filelist = self.listUrlRecursive(url, log, (fname+'/'+str(f.GetName())).strip('/'), filelist)
        return filelist


    def _download_outputs(self, files, logdir, jobid, pandaid, userconfig, log):
        '''Download the output files specified in downloadfiles'''

        # construct datapoint object, initialising connection. Use the same
        # object until base URL changes. TODO group by base URL.

        datapoint = arc_utils.DataPoint(str(jobid), userconfig)
        dp = datapoint.h
        dm = arc.DataMover()
        dm.retry(False)
        dm.passive(True)
        dm.secure(False)
        fetched = []
        notfetched = []
        notfetchedretry = []

        # create required local log dirs
        try:
            os.makedirs(logdir, 0755)
        except OSError as e:
            if e.errno != errno.EEXIST or not os.path.isdir(logdir):
                log.warning('Failed to create directory {0}: {1}'.format(logdir, os.strerror(e.errno)))
                notfetched.append(jobid)
                return (fetched, notfetched, notfetchedretry)

        tmpdldir = os.path.join(self.tmpdir, pandaid)
        try:
            os.makedirs(tmpdldir, 0755)
        except OSError as e:
            if e.errno != errno.EEXIST or not os.path.isdir(tmpdldir):
                log.warning('Failed to create directory {0}: {1}'.format(tmpdldir, os.strerror(e.errno)))
                notfetched.append(jobid)
                return (fetched, notfetched, notfetchedretry)

        filelist = files.split(';')
        if re.search(r'[\*\[\]\?]', files):
            # found wildcard, need to get sessiondir list
            remotefiles = self.listUrlRecursive(jobid, log)
            expandedfiles = []
            for wcf in filelist:
                if re.search(r'[\*\[\]\?]', wcf):
                    # only match wildcards in matching dirs
                    expandedfiles += [rf for rf in remotefiles if fnmatch.fnmatch(rf, wcf) and os.path.dirname(rf) == os.path.dirname(wcf)]
                else:
                    expandedfiles.append(wcf)
            # remove duplicates from wildcard matching through set
            filelist = list(set(expandedfiles))

        for f in filelist:
            if f == 'gmlog/errors':
                localfile = os.path.join(logdir, '%s.log' % pandaid)
            elif f.find('.log') != -1:
                localfile = os.path.join(logdir, '%s.out' % pandaid)
            else:
                localfile = os.path.join(tmpdldir, f)

            remotefile = arc.URL(str(jobid + '/' + f))
            dp.SetURL(remotefile)
            localdp = arc_utils.DataPoint(str(localfile), userconfig)
            # do the copy
            status = dm.Transfer(dp, localdp.h, arc.FileCache(), arc.URLMap())
            if not status and str(status).find('File unavailable') == -1: # tmp fix for globus error which is always retried
                if status.Retryable():
                    log.warning('Failed to download but will retry {0}: {1}'.format(dp.GetURL().str(), str(status)))
                    notfetchedretry.append(jobid)
                else:
                    log.error('Failed to download with permanent failure {0}: {1}'.format(dp.GetURL().str(), str(status)))
                    notfetched.append(jobid)
            else:
                os.chmod(localfile, 0644)
                log.info('Downloaded {0}'.format(dp.GetURL().str()))

        if jobid not in notfetched and jobid not in notfetchedretry:
            fetched.append(jobid)

        return (fetched, notfetched, notfetchedretry)


    def _extractAndFixPilotPickle(self, arcjob, pandaid, haveoutput, logurl, log):
        '''
        Extract the pilot pickle from jobSmallFiles.tgz, and fix attributes
        '''

        arcid = arcjob['JobID']
        pandapickle = None
        tmpjobdir = os.path.join(self.tmpdir, pandaid)
        if haveoutput:
            log.debug('os.cwd(): {0}'.format(os.getcwd()))
            try:
                smallfiles = tarfile.open(os.path.join(tmpjobdir, 'jobSmallFiles.tgz'))
                pandapickle = smallfiles.extractfile("panda_node_struct.pickle")
            except Exception as e:
                log.error("{0}: failed to extract pickle for arcjob {1}: {2}".format(pandaid, arcid, str(e)))

        if pandapickle:
            jobinfo = arc_utils.ARCPandaJob(filehandle=pandapickle)
            # de-serialise the metadata to json
            try:
                jobinfo.metaData = json.loads(jobinfo.metaData)
            except:
                log.warning("{0}: no metaData in pilot pickle".format(pandaid))
            shutil.rmtree(tmpjobdir, ignore_errors=True)
        else:
            jobinfo = arc_utils.ARCPandaJob(jobinfo={'jobId': long(pandaid), 'state': 'finished'})
            jobinfo.schedulerID = self.schedulerid
            if logurl:
                jobinfo.pilotID = "%s.out|Unknown|Unknown|Unknown|Unknown" % '/'.join([logurl, pandaid])

            # TODO: set error code based on batch error message (memory kill etc)
            jobinfo.pilotErrorCode = 1008
            if arcjob['Error']:
                jobinfo.pilotErrorDiag = arcjob['Error']
            else:
                # Probably failure getting outputs
                jobinfo.pilotErrorDiag = "Failed to get outputs from CE"

        jobinfo.computingElement = urlparse(arcid).netloc

        return jobinfo.dictionary()


    def get_access_point(self, workspec, panda_id):
        '''Get access point'''
        if workspec.mapType == WorkSpec.MT_MultiJobs:
            accessPoint = os.path.join(workspec.get_access_point(), str(panda_id))
        else:
            accessPoint = workspec.get_access_point()
        return accessPoint


    def post_processing(self, workspec, jobspec_list, map_type):
        '''
        Fetch job output and process pilot info for sending in final heartbeat.
        The pilot pickle is loaded and some attributes corrected (schedulerid,
        pilotlog etc), then converted to dictionary and stored in
        workspec.workAttributes[pandaid]. If pilot pickle cannot be used,
        report ARC error in pilotErrorDiag and fill all possible attributes
        using ARC information.
        '''

        arclog = arc_utils.ARCLogger(baselogger, workspec.workerID)
        tmplog = arclog.log
        tmplog.info('Post processing ARC job {0}'.format(workspec.batchID))
        job = workspec.workAttributes['arcjob']
        arcid = job['JobID']
        tmplog.info('Job id {0}'.format(arcid))

        if 'arcdownloadfiles' not in workspec.workAttributes:
            tmplog.error('No files to download')
            return True

        # Assume one-to-one mapping of workers to jobs. If jobspec_list is empty
        # it means the job was cancelled by panda or otherwise forgotten
        if not jobspec_list:
            return True

        # Set certificate to use for interacting with ARC CE
        userconfig = arc.UserConfig(self.cred_type)
        if not self._setup_proxy(usercfg, workspec, arcid, tmplog):
            return True

        queueconfigmapper = QueueConfigMapper()
        queueconfig = queueconfigmapper.get_queue(jobspec_list[0].computingSite)
        logbaseurl = queueconfig.submitter.get('logBaseURL')
        logbasedir = queueconfig.submitter.get('logDir', self.tmpdir)
        logsubdir = workspec.workAttributes['logsubdir']
        pandaid = str(jobspec_list[0].PandaID)

        # Construct log path and url
        logurl = '/'.join([logbaseurl, logsubdir, str(pandaid)]) if logbaseurl else None
        logdir = os.path.join(logbasedir, logsubdir)

        # post_processing is only called once, so no retries are done. But keep
        # the possibility here in case it changes
        (fetched, notfetched, notfetchedretry) = self._download_outputs(workspec.workAttributes['arcdownloadfiles'],
                                                                        logdir, arcid, pandaid, userconfig, tmplog)
        if arcid not in fetched:
            tmplog.warning("Could not get outputs of {0}".format(arcid))

        workspec.workAttributes[long(pandaid)] = {}

        workspec.workAttributes[long(pandaid)] = self._extractAndFixPilotPickle(job, pandaid, (arcid in fetched), logurl, tmplog)

        tmplog.debug("pilot info for {0}: {1}".format(pandaid, workspec.workAttributes[long(pandaid)]))
        return True


    def get_work_attributes(self, workspec):
        '''Get info from the job to pass back to harvester'''
        # Just return existing attributes. Attributes are added to workspec for
        # finished jobs in post_processing
        return workspec.workAttributes


    def events_requested(self, workspec):
        '''Used to tell harvester that the worker requests events'''

        # get logger
        arclog = arc_utils.ARCLogger(baselogger, workspec.workerID)
        tmpLog = arclog.log

        # Check for jobid/jsonEventsRequestFileName
        job = workspec.workAttributes['arcjob']
        arcid = job['JobID']
        # Set certificate to use for interacting with ARC CE
        usercfg = arc.UserConfig(self.cred_type)
        if not self._setup_proxy(usercfg, workspec, arcid, tmpLog):
            return {}

        remoteJsonFilePath = '%s/%s' % (arcid, jsonEventsRequestFileName)
        localJsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsRequestFileName)
        tmpLog.debug('looking for event request file {0}'.format(remoteJsonFilePath))
        # Try to copy the file
        status = self._copy_file(remoteJsonFilePath, localJsonFilePath, usercfg, tmpLog)
        if not status:
            if status.GetErrno() == errno.ENOENT:
                # Not found
                tmpLog.debug('not found')
                return {}
            # Some other error
            tmpLog.warning('Failed to copy {0}: {1}'.format(remoteJsonFilePath, str(status)))
            return {}

        try:
            with open(localJsonFilePath) as jsonFile:
                retDict = json.load(jsonFile)
            os.remove(localJsonFilePath)
        except Exception:
            tmpLog.debug('failed to load json')
            return {}
        tmpLog.debug('found')
        return retDict


    def feed_events(self, workspec, events_dict):
        '''Havester has an event range to pass to job'''

        # get logger
        arclog = arc_utils.ARCLogger(baselogger, workspec.workerID)
        tmpLog = arclog.log

        # Upload to jobid/jsonEventsFeedFileName, delete jobid/jsonEventsRequestFileName
        job = workspec.workAttributes['arcjob']
        arcid = job['JobID']
        # Set certificate to use for interacting with ARC CE
        usercfg = arc.UserConfig(self.cred_type)
        if not self._setup_proxy(usercfg, workspec, arcid, tmpLog):
            return False

        retVal = True
        if workspec.mapType in [WorkSpec.MT_OneToOne, WorkSpec.MT_MultiWorkers]:
            # put the json just under the access point then upload to ARC CE
            localJsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsFeedFileName)
            tmpLog.debug('feeding events to {0}'.format(localJsonFilePath))
            try:
                with open(localJsonFilePath, 'w') as jsonFile:
                    json.dump(events_dict, jsonFile)
            except Exception:
                core_utils.dump_error_message(tmpLog)
                retVal = False

            remoteJsonFilePath = '%s/%s' % (arcid, jsonEventsFeedFileName)
            # Try to copy the file
            status = self._copy_file(localJsonFilePath, remoteJsonFilePath, usercfg, tmpLog)
            if not status:
                tmpLog.error('Failed to feed events to {0}: {1}'.format(remoteJsonFilePath, str(status)))
                retVal = False
            else:
                remoteJsonEventsRequestFile = '%s/%s' % (arcid, jsonEventsRequestFileName)
                status = self._delete_file(remoteJsonEventsRequestFile, usercfg, tmpLog)
                if not status and status.GetErrno() != errno.ENOENT:
                    tmpLog.error('Failed to delete event request file at {0}'.format(remoteJsonEventsRequestFile))

        elif workspec.mapType == WorkSpec.MT_MultiJobs:
            # TOBEFIXED
            pass
        # remove request file
        try:
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsFeedFileName)
            os.remove(jsonFilePath)
        except Exception:
            pass
        tmpLog.debug('done')
        return retVal


    def events_to_update(self, workspec):
        '''Report events processed for harvester to update'''

        # get logger
        arclog = arc_utils.ARCLogger(baselogger, workspec.workerID)
        tmpLog = arclog.log

        job = workspec.workAttributes['arcjob']
        arcid = job['JobID']
        # Set certificate to use for interacting with ARC CE
        usercfg = arc.UserConfig(self.cred_type)
        if not self._setup_proxy(usercfg, workspec, arcid, tmpLog):
            return False

        # Check for jobid/jsonEventsUpdateFileName on CE, rename to .read
        retDict = dict()
        for pandaID in workspec.pandaid_list:

            # first look for json.read which is not yet acknowledged
            accessPoint = self.get_access_point(workspec, pandaID)
            localJsonFilePath = os.path.join(accessPoint, jsonEventsUpdateFileName)
            remoteJsonFilePathRead = '%s/%s%s' % (arcid, jsonEventsUpdateFileName, suffixReadJson)
            tmpLog.debug('looking for event update file {0}'.format(remoteJsonFilePathRead))

            status = self._copy_file(remoteJsonFilePathRead, localJsonFilePath, usercfg, tmpLog)
            if not status:
                if status.GetErrno() != errno.ENOENT:
                    tmpLog.warning('Failed checking {0}: {1}'.format(remoteJsonFilePathRead, str(status)))
                    continue

                # Look for new json
                remoteJsonFilePath = '%s/%s' % (arcid, jsonEventsUpdateFileName)
                status = self._copy_file(remoteJsonFilePath, localJsonFilePath, usercfg, tmpLog)
                if not status:
                    if status.GetErrno() != errno.ENOENT:
                        tmpLog.warning('Failed checking {0}: {1}'.format(remoteJsonFilePath, str(status)))
                    else:
                        # not found
                        tmpLog.debug('not found')
                    continue

                # Rename to prevent from being overwritten
                # Gridftp does not support renaming so upload .read file and delete old one
                status = self._copy_file(localJsonFilePath, remoteJsonFilePathRead, usercfg, tmpLog)
                if not status:
                    tmpLog.warning('Failed copying {0} to {1}: {2}'.format(localJsonFilePath, remoteJsonFilePathRead, str(status)))
                # If rename fails, delete old file anyway
                status = self._delete_file(remoteJsonFilePath, usercfg, tmpLog)
                if not status:
                    tmpLog.warning('Failed deleting {0}: {1}'.format(remoteJsonFilePath, str(status)))

            # load json
            nData = 0
            try:
                with open(localJsonFilePath) as jsonFile:
                    tmpOrigDict = json.load(jsonFile)
                    newDict = dict()
                    # change the key from str to int
                    for tmpPandaID, tmpDict in tmpOrigDict.iteritems():
                        tmpPandaID = long(tmpPandaID)
                        retDict[tmpPandaID] = tmpDict
                        nData += 1
            except Exception:
                raise
                tmpLog.error('failed to load json')
            # delete local file
            try:
                os.remove(localJsonFilePath)
            except Exception:
                pass
            tmpLog.debug('got {0} events for PandaID={1}'.format(nData, pandaID))
        return retDict


    def acknowledge_events_files(self, workspec):
        '''Tell workers that harvester received events/files'''

        # get logger
        arclog = arc_utils.ARCLogger(baselogger, workspec.workerID)
        tmpLog = arclog.log

        job = workspec.workAttributes['arcjob']
        arcid = job['JobID']
        # Set certificate to use for interacting with ARC CE
        usercfg = arc.UserConfig(self.cred_type)
        if not self._setup_proxy(usercfg, workspec, arcid, tmpLog):
            return False

        # Delete jobid/jsonEventsUpdateFileName.read
        for pandaID in workspec.pandaid_list:
            accessPoint = self.get_access_point(workspec, pandaID)
            remoteJsonFilePath = '%s/%s%s' % (arcid, jsonEventsUpdateFileName, suffixReadJson)
            status = self._delete_file(remoteJsonFilePath, usercfg, tmpLog)
            if not status and status.GetErrno() != errno.ENOENT:
                 tmpLog.error('Failed deleting {0}: {1}'.format(remoteJsonFilePath, str(status)))

        tmpLog.debug('done')
        return


    # The remaining methods do not apply to ARC
    def feed_jobs(self, workspec, jobspec_list):
        '''Pass job to worker. No-op for Grid'''
        return True

    def get_files_to_stage_out(self, workspec):
        '''Not required in Grid case'''
        return {}

    def job_requested(self, workspec):
        '''Used in pull model to say that worker is ready for a job'''
        return False

    def setup_access_points(self, workspec_list):
        '''Access is through CE so nothing to set up here'''
        pass

    def get_panda_ids(self, workspec):
        '''For pull model, get panda IDs assigned to jobs'''
        return []

    def kill_requested(self, workspec):
        '''Worker wants to kill itself (?)'''
        return False

    def is_alive(self, workspec, time_limit):
        '''Check if worker is alive, not for Grid'''
        return True


def test():
    from pandaharvester.harvestercore.work_spec import WorkSpec
    wspec = WorkSpec()
    jobid = "gsiftp://pcoslo5.cern.ch:2811/jobs/XkNNDmultdtn1ZPzno6AuCjpABFKDmABFKDmwqyLDmABFKDm8dOcOn"
    wspec.batchID = jobid
    workAttributes = {"arcjob": {}}
    workAttributes["arcjob"]["JobID"] = wspec.batchID
    workAttributes["arcjob"]["JobStatusURL"] = "ldap://{0}:2135/mds-vo-name=local,o=grid??sub?(nordugrid-job-globalid={1})".format(urlparse(jobid).netloc, jobid)
    workAttributes["arcjob"]["JobStatusInterfaceName"] = "org.nordugrid.ldapng"
    jobmanagementurl = arc.URL(wspec.batchID)
    jobmanagementurl.ChangePath("/jobs")
    workAttributes["arcjob"]["JobManagementURL"] = jobmanagementurl.str()
    workAttributes["arcjob"]["JobManagementInterfaceName"] = "org.nordugrid.gridftpjob"
    workAttributes["proxyrole"] = 'production'

    wspec.workAttributes = workAttributes
    wspec.accessPoint = '/tmp'
    wspec.mapType = WorkSpec.MT_OneToOne
    wspec.pandaid_list = [1234]
    print wspec.workAttributes

    messenger = ARCMessenger()
    print messenger.events_requested(wspec)
    print messenger.feed_events(wspec, {'event': 1234})
    print messenger.events_to_update(wspec)
    messenger.acknowledge_events_files(wspec)

if __name__ == '__main__':
    test()
